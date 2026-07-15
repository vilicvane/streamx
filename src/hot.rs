use std::{
  collections::VecDeque,
  pin::Pin,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
  },
  task::{Context, Poll},
};

use futures::{Stream, task::AtomicWaker};

pub(crate) const WORK_BUDGET: usize = 64;

struct State<T> {
  queue: VecDeque<T>,
  capacity: usize,
  done: bool,
}

struct Shared<T> {
  state: Mutex<State<T>>,
  waker: AtomicWaker,
  driver_started: AtomicBool,
}

/// The producer side of a bounded, drop-oldest channel used by hot operators.
pub(crate) struct HotSender<T> {
  shared: Arc<Shared<T>>,
}

impl<T> HotSender<T> {
  fn mark_driver_started(&self) {
    self.shared.driver_started.store(true, Ordering::Release);
    self.shared.waker.wake();
  }

  pub(crate) fn send(&self, item: T) {
    let mut state = self.shared.state.lock().unwrap();

    if state.done {
      return;
    }

    if state.queue.len() == state.capacity {
      state.queue.pop_front();
    }
    state.queue.push_back(item);

    drop(state);
    self.shared.waker.wake();
  }
}

impl<T> Drop for HotSender<T> {
  fn drop(&mut self) {
    let mut state = self.shared.state.lock().unwrap();
    state.done = true;
    drop(state);
    self.shared.waker.wake();
  }
}

/// The common receiver and task lifetime used by single-subscriber hot operators.
pub(crate) struct HotStream<T> {
  shared: Arc<Shared<T>>,
  task: tokio::task::JoinHandle<()>,
}

impl<T> HotStream<T> {
  pub(crate) fn spawn<TFuture, TBuild>(capacity: usize, build: TBuild) -> Self
  where
    T: Send + 'static,
    TFuture: Future<Output = ()> + Send + 'static,
    TBuild: FnOnce(HotSender<T>) -> TFuture + Send + 'static,
  {
    assert!(capacity > 0, "capacity must be greater than zero");

    let shared = Arc::new(Shared {
      state: Mutex::new(State {
        queue: VecDeque::with_capacity(capacity),
        capacity,
        done: false,
      }),
      waker: AtomicWaker::new(),
      driver_started: AtomicBool::new(false),
    });

    let sender = HotSender {
      shared: Arc::clone(&shared),
    };
    let task = tokio::spawn(async move {
      sender.mark_driver_started();
      build(sender).await;
    });

    Self { shared, task }
  }

  pub(crate) fn driver_started(&self) -> bool {
    self.shared.driver_started.load(Ordering::Acquire)
  }
}

impl<T> Stream for HotStream<T> {
  type Item = T;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let mut state = self.shared.state.lock().unwrap();

    if let Some(item) = state.queue.pop_front() {
      return Poll::Ready(Some(item));
    }

    if state.done {
      return Poll::Ready(None);
    }

    // Registration happens while holding the state lock, so a producer cannot
    // enqueue between the empty check and waker registration without waking us.
    self.shared.waker.register(cx.waker());
    Poll::Pending
  }
}

impl<T> Drop for HotStream<T> {
  fn drop(&mut self) {
    self.task.abort();
  }
}
