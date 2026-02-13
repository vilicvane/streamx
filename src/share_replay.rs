use futures::Stream;
use futures::StreamExt;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::{
  Arc, Weak,
  atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::task::{Context, Poll};

struct Inner<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  sender: async_broadcast::Sender<(u64, T::Item)>,
  seed_receiver: Mutex<Option<async_broadcast::Receiver<(u64, T::Item)>>>,
  source: tokio::sync::Mutex<Option<T>>,
  task: Mutex<Option<tokio::task::JoinHandle<()>>>,
  subscription_count: AtomicUsize,
  replay_buffer: Mutex<VecDeque<(u64, T::Item)>>,
  replay_buffer_size: usize,
  sequence: AtomicU64,
}

impl<T> Inner<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn stop(&self) {
    if let Ok(mut task) = self.task.lock()
      && let Some(handle) = task.take()
    {
      handle.abort();
    }
    self.sender.close();
  }

  fn start(self: &Arc<Self>) {
    let inner = Arc::clone(self);
    let mut task = match self.task.lock() {
      Ok(task) => task,
      Err(poisoned) => poisoned.into_inner(),
    };

    if task.is_some() {
      return;
    }

    let handle = tokio::spawn(async move {
      let Some(source) = inner.source.lock().await.take() else {
        return;
      };

      let mut source = Box::pin(source);

      while let Some(item) = source.next().await {
        let sequence = inner.sequence.fetch_add(1, Ordering::Relaxed);

        if inner.replay_buffer_size > 0 {
          let mut replay_buffer = match inner.replay_buffer.lock() {
            Ok(replay_buffer) => replay_buffer,
            Err(poisoned) => poisoned.into_inner(),
          };

          replay_buffer.push_back((sequence, item.clone()));
          while replay_buffer.len() > inner.replay_buffer_size {
            replay_buffer.pop_front();
          }
        }

        if inner.sender.broadcast((sequence, item)).await.is_err() {
          break;
        }
      }

      inner.sender.close();
    });

    *task = Some(handle);
  }
}

pub struct ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  inner: Arc<Inner<T>>,
  receiver: Option<async_broadcast::Receiver<(u64, T::Item)>>,
  replay_queue: VecDeque<T::Item>,
  replay_max_sequence: Option<u64>,
}

impl<T> ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  pub fn downgrade(&self) -> ShareReplayStreamWeak<T> {
    ShareReplayStreamWeak {
      inner: Arc::downgrade(&self.inner),
    }
  }
}

impl<T> Clone for ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn clone(&self) -> Self {
    self
      .inner
      .subscription_count
      .fetch_add(1, Ordering::Relaxed);

    Self {
      inner: Arc::clone(&self.inner),
      receiver: None,
      replay_queue: VecDeque::new(),
      replay_max_sequence: None,
    }
  }
}

impl<T> Drop for ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn drop(&mut self) {
    if self.inner.subscription_count.fetch_sub(1, Ordering::AcqRel) == 1 {
      self.inner.stop();
    }
  }
}

pub struct ShareReplayStreamWeak<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  inner: Weak<Inner<T>>,
}

impl<T> Clone for ShareReplayStreamWeak<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn clone(&self) -> Self {
    Self {
      inner: Weak::clone(&self.inner),
    }
  }
}

impl<T> ShareReplayStreamWeak<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  pub fn upgrade(&self) -> Option<ShareReplayStream<T>> {
    self.inner.upgrade().map(|inner| {
      inner.subscription_count.fetch_add(1, Ordering::Relaxed);

      ShareReplayStream {
        inner: Arc::clone(&inner),
        receiver: None,
        replay_queue: VecDeque::new(),
        replay_max_sequence: None,
      }
    })
  }
}

impl<T> ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn initialize_receiver_and_replay_queue(&mut self) {
    if self.receiver.is_some() {
      return;
    }

    let receiver = self
      .inner
      .seed_receiver
      .lock()
      .ok()
      .and_then(|mut guard| guard.take())
      .unwrap_or_else(|| self.inner.sender.new_receiver());

    let replay_buffer = match self.inner.replay_buffer.lock() {
      Ok(replay_buffer) => replay_buffer,
      Err(poisoned) => poisoned.into_inner(),
    };

    let replay_max_sequence = replay_buffer.back().map(|(sequence, _)| *sequence);
    let replay_queue = replay_buffer
      .iter()
      .map(|(_, item)| item.clone())
      .collect::<VecDeque<_>>();

    self.receiver = Some(receiver);
    self.replay_queue = replay_queue;
    self.replay_max_sequence = replay_max_sequence;
  }
}

impl<T> Stream for ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  type Item = T::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = unsafe { self.get_unchecked_mut() };

    this.initialize_receiver_and_replay_queue();
    this.inner.start();

    if let Some(item) = this.replay_queue.pop_front() {
      return Poll::Ready(Some(item));
    }

    loop {
      let poll = Pin::new(
        this
          .receiver
          .as_mut()
          .expect("receiver must be initialized."),
      )
      .poll_next(cx);

      match poll {
        Poll::Ready(Some((sequence, item))) => {
          if this
            .replay_max_sequence
            .is_some_and(|replay_max_sequence| sequence <= replay_max_sequence)
          {
            continue;
          }

          return Poll::Ready(Some(item));
        }
        Poll::Ready(None) => {
          return Poll::Ready(None);
        }
        Poll::Pending => {
          return Poll::Pending;
        }
      }
    }
  }
}

pub trait StreamShareReplayExt: Stream + Sized
where
  Self: Send + 'static,
  Self::Item: Clone + Send + 'static,
{
  fn share_replay(self, buffer_size: usize) -> ShareReplayStream<Self> {
    self.share_replay_with_capacity(buffer_size, 16)
  }

  fn share_replay_with_capacity(
    self,
    buffer_size: usize,
    capacity: usize,
  ) -> ShareReplayStream<Self> {
    let (mut sender, receiver) = async_broadcast::broadcast(capacity);

    sender.set_await_active(true);

    ShareReplayStream {
      inner: Arc::new(Inner {
        sender,
        seed_receiver: Mutex::new(Some(receiver)),
        source: tokio::sync::Mutex::new(Some(self)),
        task: Mutex::new(None),
        subscription_count: AtomicUsize::new(1),
        replay_buffer: Mutex::new(VecDeque::new()),
        replay_buffer_size: buffer_size,
        sequence: AtomicU64::new(0),
      }),
      receiver: None,
      replay_queue: VecDeque::new(),
      replay_max_sequence: None,
    }
  }
}

impl<T> StreamShareReplayExt for T
where
  T: Stream + Sized + Send + 'static,
  T::Item: Clone + Send + 'static,
{
}

#[cfg(test)]
mod tests {
  use super::StreamShareReplayExt;
  use futures::StreamExt;
  use std::pin::Pin;
  use std::task::{Context, Poll};

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn share_replay_replays_for_late_subscribers() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let shared = MpscStream(rx).share_replay(2);

    let mut first = shared.clone();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();

    assert_eq!(first.next().await, Some(1));
    assert_eq!(first.next().await, Some(2));
    assert_eq!(first.next().await, Some(3));

    let mut late = shared.clone();
    assert_eq!(late.next().await, Some(2));
    assert_eq!(late.next().await, Some(3));

    tx.send(4).unwrap();
    assert_eq!(first.next().await, Some(4));
    assert_eq!(late.next().await, Some(4));

    drop(tx);
    assert_eq!(first.next().await, None);
    assert_eq!(late.next().await, None);
  }

  #[tokio::test]
  async fn share_replay_with_zero_buffer_replays_nothing() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let shared = MpscStream(rx).share_replay(0);

    let mut first = shared.clone();

    tx.send(10).unwrap();
    tx.send(20).unwrap();

    assert_eq!(first.next().await, Some(10));
    assert_eq!(first.next().await, Some(20));

    let mut late = shared.clone();
    tx.send(30).unwrap();

    assert_eq!(late.next().await, Some(30));
    assert_eq!(first.next().await, Some(30));

    drop(tx);
    assert_eq!(late.next().await, None);
    assert_eq!(first.next().await, None);
  }

  #[tokio::test]
  async fn share_replay_with_capacity_replays_for_late_subscribers() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let shared = MpscStream(rx).share_replay_with_capacity(2, 1);

    let mut first = shared.clone();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();

    assert_eq!(first.next().await, Some(1));
    assert_eq!(first.next().await, Some(2));
    assert_eq!(first.next().await, Some(3));

    let mut late = shared.clone();
    assert_eq!(late.next().await, Some(2));
    assert_eq!(late.next().await, Some(3));

    drop(tx);
    assert_eq!(late.next().await, None);
    assert_eq!(first.next().await, None);
  }

  #[tokio::test]
  async fn weak_can_upgrade_to_share_replay_stream() {
    let shared = futures::stream::iter([1_u32, 2, 3]).share_replay(2);
    let weak = shared.downgrade();

    let upgraded = weak.upgrade().expect("share replay stream should be alive");
    drop(shared);

    let collected = upgraded.collect::<Vec<_>>().await;
    assert_eq!(collected, vec![1, 2, 3]);
  }

  #[test]
  fn weak_does_not_keep_share_replay_stream_alive() {
    let weak = {
      let shared = futures::stream::empty::<u32>().share_replay(2);
      shared.downgrade()
    };

    assert!(weak.upgrade().is_none());
  }
}
