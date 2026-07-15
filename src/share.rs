use std::{
  pin::Pin,
  sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
  },
  task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use crate::hot::WORK_BUDGET;

struct CloseOnDrop<T>(async_broadcast::Sender<T>);

impl<T> Drop for CloseOnDrop<T> {
  fn drop(&mut self) {
    self.0.close();
  }
}

struct Inner<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  sender: async_broadcast::Sender<T::Item>,
  task: Mutex<Option<tokio::task::JoinHandle<()>>>,
  subscription_count: AtomicUsize,
}

impl<T> Inner<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn start(self: &Arc<Self>, source: T) {
    let inner = Arc::clone(self);
    let task = tokio::spawn(async move {
      let _close_on_drop = CloseOnDrop(inner.sender.clone());
      let mut source = Box::pin(source);
      let mut work = 0;

      while let Some(item) = source.next().await {
        if inner.sender.broadcast(item).await.is_err() {
          break;
        }

        work += 1;
        if work == WORK_BUDGET {
          work = 0;
          tokio::task::yield_now().await;
        }
      }
    });

    *self.task.lock().unwrap() = Some(task);
  }

  fn stop(&self) {
    self.sender.close();
    if let Some(task) = self.task.lock().unwrap().take() {
      task.abort();
    }
  }

  fn add_subscription(&self) {
    let previous = self.subscription_count.fetch_add(1, Ordering::Relaxed);
    debug_assert!(previous > 0);
  }

  fn try_add_subscription(&self) -> bool {
    self
      .subscription_count
      .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
        (count > 0).then_some(count + 1)
      })
      .is_ok()
  }

  fn remove_subscription(&self) {
    if self.subscription_count.fetch_sub(1, Ordering::AcqRel) == 1 {
      self.stop();
    }
  }
}

/// A clonable, immediately-active subscription to one shared upstream stream.
///
/// Every strong handle is registered at construction, clone, or weak upgrade;
/// first polling has no lifecycle meaning. `share` backpressures at capacity,
/// while overflow variants drop the oldest live item.
pub struct ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  inner: Arc<Inner<T>>,
  receiver: async_broadcast::Receiver<T::Item>,
}

impl<T> ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  pub fn downgrade(&self) -> ShareStreamWeak<T> {
    ShareStreamWeak {
      inner: Arc::downgrade(&self.inner),
    }
  }
}

impl<T> Clone for ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn clone(&self) -> Self {
    self.inner.add_subscription();
    Self {
      inner: Arc::clone(&self.inner),
      receiver: self.inner.sender.new_receiver(),
    }
  }
}

impl<T> Drop for ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn drop(&mut self) {
    self.inner.remove_subscription();
  }
}

impl<T> Stream for ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  type Item = T::Item;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.receiver).poll_next(cx)
  }
}

/// A non-owning handle to a [`ShareStream`].
pub struct ShareStreamWeak<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  inner: Weak<Inner<T>>,
}

impl<T> Clone for ShareStreamWeak<T>
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

impl<T> ShareStreamWeak<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  /// Upgrade while the source is live and at least one strong subscriber exists.
  pub fn upgrade(&self) -> Option<ShareStream<T>> {
    let inner = self.inner.upgrade()?;

    if inner.sender.is_closed() || !inner.try_add_subscription() {
      return None;
    }

    // Completion may race this point. The upgrade linearizes at the successful
    // live-state check above; a subsequent completion simply closes this receiver.
    let receiver = inner.sender.new_receiver();

    Some(ShareStream { inner, receiver })
  }
}

fn create_share_stream<T>(source: T, capacity: usize, overflow: bool) -> ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  assert!(capacity > 0, "capacity must be greater than zero");

  let (mut sender, receiver) = async_broadcast::broadcast(capacity);
  sender.set_overflow(overflow);

  let inner = Arc::new(Inner {
    sender,
    task: Mutex::new(None),
    subscription_count: AtomicUsize::new(1),
  });
  inner.start(source);

  ShareStream { inner, receiver }
}

pub trait StreamShareExt: Stream + Sized
where
  Self: Send + 'static,
  Self::Item: Clone + Send + 'static,
{
  /// Losslessly multicast with bounded live capacity and slow-subscriber backpressure.
  ///
  /// This must be called from within a Tokio runtime.
  fn share(self, capacity: usize) -> ShareStream<Self> {
    create_share_stream(self, capacity, false)
  }

  /// Multicast while dropping the oldest live item when capacity is full.
  ///
  /// This must be called from within a Tokio runtime.
  fn share_overflow(self, capacity: usize) -> ShareStream<Self> {
    create_share_stream(self, capacity, true)
  }

  /// Equivalent to `share_overflow(1)`; it does not replay to later subscribers.
  fn share_latest(self) -> ShareStream<Self> {
    self.share_overflow(1)
  }
}

impl<T> StreamShareExt for T
where
  T: Stream + Sized + Send + 'static,
  T::Item: Clone + Send + 'static,
{
}

#[cfg(test)]
mod tests {
  use std::{
    pin::Pin,
    sync::{
      Arc,
      atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
  };

  use futures::{Stream, StreamExt};
  use lits::duration;

  use super::StreamShareExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn share_overflow_runs_before_first_poll() {
    let mut shared = futures::stream::iter([1, 2, 3, 4, 5]).share_latest();
    tokio::time::sleep(duration!("10ms")).await;

    assert_eq!(shared.next().await, Some(5));
    assert_eq!(shared.next().await, None);
  }

  #[tokio::test]
  async fn share_backpressures_an_unpolled_strong_subscriber() {
    struct CountingStream {
      next: u32,
      produced: Arc<AtomicUsize>,
    }

    impl Stream for CountingStream {
      type Item = u32;

      fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.next == 6 {
          return Poll::Ready(None);
        }
        let item = self.next;
        self.next += 1;
        self.produced.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Some(item))
      }
    }

    let produced = Arc::new(AtomicUsize::new(0));
    let shared = CountingStream {
      next: 1,
      produced: Arc::clone(&produced),
    }
    .share(2);

    tokio::time::sleep(duration!("10ms")).await;
    assert!(produced.load(Ordering::Relaxed) <= 3);
    assert_eq!(shared.collect::<Vec<_>>().await, vec![1, 2, 3, 4, 5]);
  }

  #[tokio::test]
  async fn clone_is_active_immediately_and_receives_no_history() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut first = MpscStream(rx).share(4);

    tx.send(1).unwrap();
    assert_eq!(first.next().await, Some(1));

    let mut second = first.clone();
    tx.send(2).unwrap();
    assert_eq!(first.next().await, Some(2));
    assert_eq!(second.next().await, Some(2));
  }

  #[tokio::test]
  async fn weak_upgrade_is_an_active_subscription_before_poll() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut current = MpscStream(rx).share(1);
    let weak = current.downgrade();

    tx.send(1).unwrap();
    assert_eq!(current.next().await, Some(1));

    let mut replacement = weak.upgrade().unwrap();
    drop(current);
    tx.send(2).unwrap();
    assert_eq!(replacement.next().await, Some(2));
  }

  #[tokio::test]
  async fn weak_cannot_upgrade_after_source_completion() {
    let mut shared = futures::stream::iter([1]).share_latest();
    let weak = shared.downgrade();

    assert_eq!(shared.next().await, Some(1));
    assert_eq!(shared.next().await, None);
    assert!(weak.upgrade().is_none());
  }

  #[tokio::test]
  async fn weak_does_not_keep_source_alive() {
    let weak = futures::stream::pending::<u32>().share(1).downgrade();
    assert!(weak.upgrade().is_none());
  }

  #[tokio::test]
  async fn source_is_released_after_last_strong_drop() {
    struct DropTracker {
      receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
      _retained: Arc<()>,
    }

    impl Stream for DropTracker {
      type Item = ();

      fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
      }
    }

    let retained = Arc::new(());
    let weak = Arc::downgrade(&retained);
    let (_tx, receiver) = tokio::sync::mpsc::unbounded_channel();
    let shared = DropTracker {
      receiver,
      _retained: Arc::clone(&retained),
    }
    .share(1);
    drop(retained);

    drop(shared);
    tokio::task::yield_now().await;
    assert!(weak.upgrade().is_none());
  }

  #[test]
  #[should_panic(expected = "capacity must be greater than zero")]
  fn share_rejects_zero_capacity() {
    let _ = futures::stream::empty::<u32>().share(0);
  }
}
