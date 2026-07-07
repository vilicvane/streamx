use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::{
  Arc, Weak,
  atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};

struct Inner<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  sender: async_broadcast::Sender<T::Item>,
  seed_receiver: Mutex<Option<async_broadcast::Receiver<T::Item>>>,
  source: tokio::sync::Mutex<Option<T>>,
  task: Mutex<Option<tokio::task::JoinHandle<()>>>,
  subscription_count: AtomicUsize,
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
        if inner.sender.broadcast(item).await.is_err() {
          break;
        }
      }

      inner.sender.close();
    });

    *task = Some(handle);
  }
}

/// A clonable stream that multicasts items from a single upstream stream to all clones.
///
/// This is similar in spirit to Rx's `share()`: it ensures the upstream is polled only once,
/// while multiple downstream consumers can observe the same sequence of items.
///
/// Notes:
/// - Requires `Item: Clone` because `async_broadcast` delivers a cloned item per receiver.
/// - The upstream is started lazily on the first poll of any `ShareStream`.
/// - If all receivers are dropped, the upstream task stops (it cannot be restarted).
pub struct ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  inner: Arc<Inner<T>>,
  receiver: Option<async_broadcast::Receiver<T::Item>>,
}

impl<T> ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  /// Create a weak handle that does not keep the shared stream alive.
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
    self
      .inner
      .subscription_count
      .fetch_add(1, Ordering::Relaxed);

    Self {
      inner: Arc::clone(&self.inner),
      receiver: None,
    }
  }
}

impl<T> Drop for ShareStream<T>
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

impl<T> Stream for ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  type Item = T::Item;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    if self.receiver.is_none() {
      // Keep the broadcast channel alive by reusing the initial receiver created by `broadcast()`.
      // This avoids the channel being closed before the first subscriber starts polling.
      let seed = self
        .inner
        .seed_receiver
        .lock()
        .ok()
        .and_then(|mut g| g.take());
      self.receiver = Some(seed.unwrap_or_else(|| self.inner.sender.new_receiver()));
    }

    self.inner.start();

    // Safety: we always initialize `receiver` above.
    Pin::new(
      self
        .receiver
        .as_mut()
        .expect("receiver must be initialized."),
    )
    .poll_next(cx)
  }
}

/// A weak handle to a `ShareStream`.
///
/// This behaves similarly to `std::sync::Weak`: it can be upgraded to a new
/// `ShareStream` while the underlying shared state is still alive, but does
/// not keep that state alive on its own.
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
  /// Attempt to upgrade to a strong `ShareStream`.
  ///
  /// Returns `None` if the shared stream has been deallocated, or if it can no
  /// longer produce items: once the last strong handle drops (`stop()`) or the
  /// source ends, the sender is closed and the consumed source cannot be
  /// restarted, so upgrading would only yield a dead subscriber.
  pub fn upgrade(&self) -> Option<ShareStream<T>> {
    let inner = self.inner.upgrade()?;

    if inner.sender.is_closed() {
      return None;
    }

    // The inner allocation may briefly outlive the last strong handle (e.g. the
    // aborted forwarding task still holds it), so a successful `Weak::upgrade`
    // alone is not enough: refuse to resurrect an inner whose subscription
    // count already reached zero.
    inner
      .subscription_count
      .fetch_update(Ordering::AcqRel, Ordering::Acquire, |subscription_count| {
        (subscription_count > 0).then_some(subscription_count + 1)
      })
      .ok()?;

    Some(ShareStream {
      inner,
      receiver: None,
    })
  }
}

fn create_share_stream<T>(source: T, capacity: usize, overflow: bool) -> ShareStream<T>
where
  T: Stream + Send + 'static,
  T::Item: Clone + Send + 'static,
{
  let (mut sender, receiver) = async_broadcast::broadcast(capacity);

  // Wait for at least one receiver to be actively polled before sending.
  // This avoids immediately failing/dropping items when the first subscriber hasn't yet
  // started polling.
  sender.set_await_active(true);
  sender.set_overflow(overflow);

  let inner = Arc::new(Inner {
    sender,
    seed_receiver: Mutex::new(Some(receiver)),
    source: tokio::sync::Mutex::new(Some(source)),
    task: Mutex::new(None),
    subscription_count: AtomicUsize::new(1),
  });

  ShareStream {
    inner: Arc::clone(&inner),
    receiver: None,
  }
}

/// Share-related stream extensions (e.g. `share()`).
pub trait StreamShareExt: Stream + Sized
where
  Self: Send + 'static,
  Self::Item: Clone + Send + 'static,
{
  /// Share the upstream stream across multiple subscribers (clones), using a broadcast channel.
  ///
  /// The `capacity` controls channel buffering and lag/backpressure behavior.
  fn share(self, capacity: usize) -> ShareStream<Self> {
    create_share_stream(self, capacity, false)
  }

  /// Like `share()`, but uses overflow mode to drop oldest queued values when full.
  ///
  /// This avoids global backpressure from lagging subscribers at the cost of silent drops.
  fn share_overflow(self, capacity: usize) -> ShareStream<Self> {
    create_share_stream(self, capacity, true)
  }

  /// Share in latest mode.
  ///
  /// This is equivalent to `share_overflow(1)`: each subscriber gets the newest queued value
  /// when lagging, and intermediate values may be dropped silently.
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
  use super::StreamShareExt;
  use futures::StreamExt;
  use lits::duration;
  use std::pin::Pin;
  use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  };
  use std::task::{Context, Poll};
  use tokio::sync::oneshot;

  #[tokio::test]
  async fn share_multicasts_to_clones() {
    let shared = futures::stream::iter([1_u32, 2, 3, 4, 5]).share(16);

    let a = shared.clone();
    let b = shared.clone();

    let ta = tokio::spawn(async move { a.collect::<Vec<_>>().await });
    let tb = tokio::spawn(async move { b.collect::<Vec<_>>().await });

    let va = ta.await.unwrap();
    let vb = tb.await.unwrap();

    assert_eq!(va, vec![1, 2, 3, 4, 5]);
    assert_eq!(vb, vec![1, 2, 3, 4, 5]);
  }

  #[tokio::test]
  async fn share_overflow_drops_oldest_for_lagging_subscribers() {
    let shared = futures::stream::iter([1_u32, 2, 3, 4, 5]).share_overflow(1);

    let slow = shared.clone();

    // Let upstream run before the slow subscriber starts polling.
    tokio::time::sleep(duration!("10ms")).await;

    let slow_values = slow.collect::<Vec<_>>().await;
    assert_eq!(slow_values, vec![5]);
  }

  #[tokio::test]
  async fn share_latest_is_alias_of_share_overflow_capacity_one() {
    let shared = futures::stream::iter([1_u32, 2, 3, 4, 5]).share_latest();
    let slow = shared.clone();

    tokio::time::sleep(duration!("10ms")).await;

    let slow_values = slow.collect::<Vec<_>>().await;
    assert_eq!(slow_values, vec![5]);
  }

  struct CountingStream {
    cur: u32,
    end: u32,
    produced: Arc<AtomicUsize>,
  }

  impl futures::Stream for CountingStream {
    type Item = u32;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      if self.cur >= self.end {
        return Poll::Ready(None);
      }
      let v = self.cur;
      self.cur += 1;
      self.produced.fetch_add(1, Ordering::Relaxed);
      Poll::Ready(Some(v))
    }
  }

  #[tokio::test]
  async fn share_only_consumes_upstream_once() {
    let produced = Arc::new(AtomicUsize::new(0));
    let n = 100u32;

    let shared = CountingStream {
      cur: 0,
      end: n,
      produced: Arc::clone(&produced),
    }
    .share(16);

    let a = shared.clone();
    let b = shared.clone();

    let ta = tokio::spawn(async move { a.collect::<Vec<_>>().await });
    let tb = tokio::spawn(async move { b.collect::<Vec<_>>().await });

    let va = ta.await.unwrap();
    let vb = tb.await.unwrap();

    assert_eq!(va.len() as u32, n);
    assert_eq!(vb.len() as u32, n);
    assert_eq!(produced.load(Ordering::Relaxed) as u32, n);
  }

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn share_does_not_replay_to_late_subscribers() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let shared = MpscStream(rx).share(16);

    let mut a = shared.clone();

    let (ready_tx, ready_rx) = oneshot::channel();
    let ta = tokio::spawn(async move {
      // Ensure we observe the first value before creating the late subscriber.
      let first = a.next().await;
      ready_tx.send(()).ok();

      let mut rest = a.collect::<Vec<_>>().await;
      let mut all = Vec::new();
      if let Some(v) = first {
        all.push(v);
      }
      all.append(&mut rest);
      all
    });

    tx.send(1).unwrap();
    ready_rx.await.unwrap();

    let late = shared.clone();
    let tb = tokio::spawn(async move { late.collect::<Vec<_>>().await });

    tx.send(2).unwrap();
    tx.send(3).unwrap();
    drop(tx);

    let va = ta.await.unwrap();
    let vb = tb.await.unwrap();

    assert_eq!(va, vec![1, 2, 3]);
    assert_eq!(vb, vec![2, 3]);
  }

  #[tokio::test]
  async fn share_stops_when_all_receivers_dropped() {
    let produced = Arc::new(AtomicUsize::new(0));
    let shared = CountingStream {
      cur: 0,
      end: 10_000,
      produced: Arc::clone(&produced),
    }
    .share(16);

    // Start the forwarder by polling once, then drop the only receiver.
    let mut one = shared;
    let _ = one.next().await;
    drop(one);

    // Give the spawned task a moment to notice receiver_count == 0 and stop.
    tokio::time::sleep(duration!("25ms")).await;

    let after = produced.load(Ordering::Relaxed);
    tokio::time::sleep(duration!("25ms")).await;
    let after2 = produced.load(Ordering::Relaxed);

    assert_eq!(
      after, after2,
      "upstream kept being consumed after all receivers were dropped"
    );
  }

  #[tokio::test]
  async fn weak_can_upgrade_to_shared_stream() {
    let shared = futures::stream::iter([1_u32, 2, 3]).share(16);
    let weak = shared.downgrade();

    let upgraded = weak.upgrade().expect("shared stream should be alive");
    drop(shared);

    let collected = upgraded.collect::<Vec<_>>().await;
    assert_eq!(collected, vec![1, 2, 3]);
  }

  #[test]
  fn weak_does_not_keep_shared_stream_alive() {
    let weak = {
      let shared = futures::stream::empty::<u32>().share(16);
      shared.downgrade()
    };

    assert!(weak.upgrade().is_none());
  }

  #[tokio::test]
  async fn weak_upgrade_refuses_stopped_stream() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let shared = MpscStream(rx).share(16);
    let weak = shared.downgrade();

    // Drive the stream so the forwarding task starts and takes the source.
    let mut subscriber = shared.clone();
    tx.send(1).unwrap();
    assert_eq!(subscriber.next().await, Some(1));

    // Dropping the last strong handle stops the stream for good. The aborted
    // forwarding task may still keep the inner allocation alive for a moment,
    // but upgrading must not resurrect the stopped stream.
    drop(subscriber);
    drop(shared);

    assert!(weak.upgrade().is_none());
  }

  #[tokio::test]
  async fn weak_upgrade_refuses_ended_stream() {
    let shared = futures::stream::iter([1_u32, 2, 3]).share(16);
    let weak = shared.downgrade();

    let values = shared.clone().collect::<Vec<_>>().await;
    assert_eq!(values, vec![1, 2, 3]);

    // A strong handle still exists, but the source has ended and the sender is
    // closed — an upgraded subscriber could never receive anything.
    assert!(weak.upgrade().is_none());
  }

  /// A stream that tracks when it's being polled and can block waiting for items.
  struct PollTrackingStream {
    receiver: tokio::sync::mpsc::UnboundedReceiver<u32>,
    poll_count: Arc<AtomicUsize>,
  }

  impl futures::Stream for PollTrackingStream {
    type Item = u32;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.poll_count.fetch_add(1, Ordering::Relaxed);
      self.receiver.poll_recv(cx)
    }
  }

  /// Test that demonstrates the leak: when all receivers are dropped while the task
  /// is blocked waiting on source.next().await, the task continues to hold resources
  /// and poll the source stream until it gets an item.
  ///
  /// The leak manifests as: the spawned task keeps the Arc<Inner> and source stream
  /// alive even after all ShareStream receivers are dropped, because it can't detect
  /// that receivers are gone while blocked on source.next().await.
  #[tokio::test]
  async fn share_leak_when_blocked_on_source() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let poll_count = Arc::new(AtomicUsize::new(0));

    let shared = PollTrackingStream {
      receiver: rx,
      poll_count: Arc::clone(&poll_count),
    }
    .share(16);

    // Start consuming to spawn the task and get it to block on source.next().await
    let receiver = shared.clone();
    let mut receiver_for_spawn = receiver.clone();
    let handle = tokio::spawn(async move {
      // This will cause the task to start and block waiting for the first item
      let _ = receiver_for_spawn.next().await;
    });

    // Wait for the task to start and block on source.next().await
    // (waiting for an item that will never come)
    tokio::time::sleep(duration!("10ms")).await;

    // Record initial poll count (task has started and is waiting)
    let initial_polls = poll_count.load(Ordering::Relaxed);
    assert!(
      initial_polls > 0,
      "Task should have started and polled the source"
    );

    // Drop all receivers - this should trigger shutdown
    // (receiver_for_spawn will be dropped when handle is dropped)
    drop(shared);
    drop(receiver);
    drop(handle); // This drops receiver_for_spawn, triggering shutdown

    // Wait a bit to ensure shutdown is processed
    tokio::time::sleep(duration!("10ms")).await;

    // The poll count shouldn't have increased (task is blocked, not polling)
    // but the resources are still held
    let polls_after_drop = poll_count.load(Ordering::Relaxed);
    assert_eq!(
      polls_after_drop, initial_polls,
      "Poll count should not increase while blocked"
    );

    // Now send an item - with the fix, the task should detect shutdown
    // and stop immediately. It might poll once more to get the item (if it arrives
    // before shutdown is checked), but should then stop without processing it.
    tx.send(42).unwrap();

    // Give the task time to process
    tokio::time::sleep(duration!("25ms")).await;

    let polls_after_item = poll_count.load(Ordering::Relaxed);

    // After the fix, the task should stop when receivers are dropped.
    // It might poll a few times if items arrive during the shutdown process
    // (this is acceptable async behavior due to race conditions), but should stop quickly.
    // The poll count should increase by at most a few polls before stopping.
    let additional_polls = polls_after_item.saturating_sub(polls_after_drop);
    assert!(
      additional_polls <= 5,
      "Leak detected: source stream was polled {} additional times after receivers dropped (expected <= 5). \
       The task should have stopped when receivers were dropped, but it continued to poll the source stream excessively.",
      additional_polls
    );
  }
}
