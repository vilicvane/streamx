use std::{
  collections::VecDeque,
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

struct ReplayState<T> {
  next_sequence: u64,
  buffer: VecDeque<(u64, T)>,
}

struct Subscription<T> {
  receiver: async_broadcast::Receiver<(u64, T)>,
  replay_queue: VecDeque<T>,
  live_start_sequence: u64,
}

struct Inner<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  sender: async_broadcast::Sender<(u64, T::Item)>,
  task: Mutex<Option<tokio::task::JoinHandle<()>>>,
  subscription_count: AtomicUsize,
  replay: Mutex<ReplayState<T::Item>>,
  replay_buffer_size: usize,
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
        let (sequence, item) = inner.record(item);

        if inner.sender.broadcast((sequence, item)).await.is_err() {
          break;
        }

        work += 1;
        if work == WORK_BUDGET {
          work = 0;
          tokio::task::yield_now().await;
        }
      }

      inner.close();
    });

    *self.task.lock().unwrap() = Some(task);
  }

  /// Record an item before live delivery. Subscription creation takes the same
  /// lock, making its replay snapshot and live sequence boundary atomic with
  /// respect to production.
  fn record(&self, item: T::Item) -> (u64, T::Item) {
    let mut replay = self.replay.lock().unwrap();
    let sequence = replay.next_sequence;
    replay.next_sequence = replay
      .next_sequence
      .checked_add(1)
      .expect("share replay sequence exhausted");

    if self.replay_buffer_size > 0 {
      replay.buffer.push_back((sequence, item.clone()));
      while replay.buffer.len() > self.replay_buffer_size {
        replay.buffer.pop_front();
      }
    }

    (sequence, item)
  }

  fn subscribe(&self, require_open: bool) -> Option<Subscription<T::Item>> {
    let replay = self.replay.lock().unwrap();

    if require_open && self.sender.is_closed() {
      return None;
    }

    let receiver = self.sender.new_receiver();
    let replay_queue = replay.buffer.iter().map(|(_, item)| item.clone()).collect();

    Some(Subscription {
      receiver,
      replay_queue,
      live_start_sequence: replay.next_sequence,
    })
  }

  fn close(&self) {
    // Serialize completion with weak upgrade and replay snapshot creation.
    let _replay = self.replay.lock().unwrap();
    self.sender.close();
  }

  fn stop(&self) {
    self.close();
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

/// A clonable, immediately-active shared stream with replay history.
pub struct ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  inner: Arc<Inner<T>>,
  receiver: async_broadcast::Receiver<(u64, T::Item)>,
  replay_queue: VecDeque<T::Item>,
  live_start_sequence: u64,
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

  fn from_subscription(inner: Arc<Inner<T>>, subscription: Subscription<T::Item>) -> Self {
    Self {
      inner,
      receiver: subscription.receiver,
      replay_queue: subscription.replay_queue,
      live_start_sequence: subscription.live_start_sequence,
    }
  }
}

impl<T> Clone for ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn clone(&self) -> Self {
    self.inner.add_subscription();
    let subscription = self
      .inner
      .subscribe(false)
      .expect("unconditional subscription creation cannot fail");
    Self::from_subscription(Arc::clone(&self.inner), subscription)
  }
}

impl<T> Drop for ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  fn drop(&mut self) {
    self.inner.remove_subscription();
  }
}

impl<T> Stream for ShareReplayStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  type Item = T::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    // No pinned field is moved.
    let this = unsafe { self.get_unchecked_mut() };

    if let Some(item) = this.replay_queue.pop_front() {
      return Poll::Ready(Some(item));
    }

    loop {
      match Pin::new(&mut this.receiver).poll_next(cx) {
        Poll::Ready(Some((sequence, _))) if sequence < this.live_start_sequence => {}
        Poll::Ready(Some((_, item))) => return Poll::Ready(Some(item)),
        Poll::Ready(None) => return Poll::Ready(None),
        Poll::Pending => return Poll::Pending,
      }
    }
  }
}

/// A non-owning handle to a [`ShareReplayStream`].
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
  /// Upgrade while the source is live and at least one strong subscriber exists.
  pub fn upgrade(&self) -> Option<ShareReplayStream<T>> {
    let inner = self.inner.upgrade()?;

    if !inner.try_add_subscription() {
      return None;
    }

    let Some(subscription) = inner.subscribe(true) else {
      inner.remove_subscription();
      return None;
    };

    Some(ShareReplayStream::from_subscription(inner, subscription))
  }
}

fn create_share_replay_stream<T>(
  source: T,
  buffer_size: usize,
  capacity: usize,
  overflow: bool,
) -> ShareReplayStream<T>
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
    replay: Mutex::new(ReplayState {
      next_sequence: 0,
      buffer: VecDeque::with_capacity(buffer_size),
    }),
    replay_buffer_size: buffer_size,
  });

  let stream = ShareReplayStream {
    inner: Arc::clone(&inner),
    receiver,
    replay_queue: VecDeque::new(),
    live_start_sequence: 0,
  };
  inner.start(source);
  stream
}

pub trait StreamShareReplayExt: Stream + Sized
where
  Self: Send + 'static,
  Self::Item: Clone + Send + 'static,
{
  /// Lossless sharing with independent replay history and live capacity.
  ///
  /// This must be called from within a Tokio runtime.
  fn share_replay(self, buffer_size: usize, capacity: usize) -> ShareReplayStream<Self> {
    create_share_replay_stream(self, buffer_size, capacity, false)
  }

  /// Replay plus drop-oldest live delivery.
  ///
  /// This must be called from within a Tokio runtime.
  fn share_replay_overflow(self, buffer_size: usize, capacity: usize) -> ShareReplayStream<Self> {
    create_share_replay_stream(self, buffer_size, capacity, true)
  }

  /// Equivalent to `share_replay_overflow(1, 1)`.
  fn share_replay_latest(self) -> ShareReplayStream<Self> {
    self.share_replay_overflow(1, 1)
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
  use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
  };

  use futures::{Stream, StreamExt};
  use lits::duration;

  use super::StreamShareReplayExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn replay_latest_runs_and_records_before_first_poll() {
    let shared = futures::stream::iter([1, 2, 3, 4, 5]).share_replay_latest();
    tokio::time::sleep(duration!("10ms")).await;

    let late = shared.clone();
    assert_eq!(late.collect::<Vec<_>>().await, vec![5]);
  }

  #[tokio::test]
  async fn replay_snapshot_is_captured_when_subscriber_is_created() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut first = MpscStream(rx).share_replay_overflow(2, 4);

    tx.send(1).unwrap();
    assert_eq!(first.next().await, Some(1));

    let mut second = first.clone();
    tx.send(2).unwrap();
    tokio::time::sleep(duration!("10ms")).await;

    assert_eq!(second.next().await, Some(1));
    assert_eq!(second.next().await, Some(2));
    assert_eq!(first.next().await, Some(2));
  }

  #[tokio::test]
  async fn zero_replay_buffer_has_only_post_subscription_live_values() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut first = MpscStream(rx).share_replay(0, 2);

    tx.send(1).unwrap();
    assert_eq!(first.next().await, Some(1));

    let mut late = first.clone();
    tx.send(2).unwrap();
    assert_eq!(late.next().await, Some(2));
  }

  #[tokio::test]
  async fn replay_and_live_handoff_has_no_duplicate() {
    let shared = futures::stream::iter([1, 2, 3]).share_replay_overflow(3, 1);
    tokio::time::sleep(duration!("10ms")).await;
    let late = shared.clone();

    assert_eq!(late.collect::<Vec<_>>().await, vec![1, 2, 3]);
  }

  #[tokio::test]
  async fn weak_upgrade_captures_replay_immediately() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut current = MpscStream(rx).share_replay_latest();
    let weak = current.downgrade();

    tx.send(1).unwrap();
    assert_eq!(current.next().await, Some(1));

    let mut replacement = weak.upgrade().unwrap();
    drop(current);
    assert_eq!(replacement.next().await, Some(1));

    tx.send(2).unwrap();
    assert_eq!(replacement.next().await, Some(2));
  }

  #[tokio::test]
  async fn weak_cannot_upgrade_after_source_completion() {
    let mut shared = futures::stream::iter([1]).share_replay_latest();
    let weak = shared.downgrade();

    assert_eq!(shared.next().await, Some(1));
    assert_eq!(shared.next().await, None);
    assert!(weak.upgrade().is_none());
  }

  #[tokio::test]
  async fn weak_does_not_keep_source_alive() {
    let weak = futures::stream::pending::<u32>()
      .share_replay_latest()
      .downgrade();
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
    .share_replay_latest();
    drop(retained);

    drop(shared);
    tokio::task::yield_now().await;
    assert!(weak.upgrade().is_none());
  }

  #[test]
  #[should_panic(expected = "capacity must be greater than zero")]
  fn replay_rejects_zero_live_capacity() {
    let _ = futures::stream::empty::<u32>().share_replay(1, 0);
  }
}
