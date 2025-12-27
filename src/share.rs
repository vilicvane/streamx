use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Mutex as StdMutex;
use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};

struct Inner<S>
where
  S: Stream + Send + 'static,
  S::Item: Clone + Send + 'static,
{
  sender: async_broadcast::Sender<S::Item>,
  seed_receiver: StdMutex<Option<async_broadcast::Receiver<S::Item>>>,
  source: tokio::sync::Mutex<Option<S>>,
  started: AtomicBool,
}

impl<S> Inner<S>
where
  S: Stream + Send + 'static,
  S::Item: Clone + Send + 'static,
{
  fn start(self: &Arc<Self>) {
    if self.started.swap(true, Ordering::AcqRel) {
      return;
    }

    let inner = Arc::clone(self);

    tokio::spawn(async move {
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
  }
}

/// A clonable stream that multicasts items from a single upstream stream to all clones.
///
/// This is similar in spirit to Rx's `share()`: it ensures the upstream is polled only once,
/// while multiple downstream consumers can observe the same sequence of items.
///
/// Notes:
/// - Requires `Item: Clone` because `async_broadcast` delivers a cloned item per receiver.
/// - The upstream is started lazily on the first poll of any `SharedStream`.
/// - If all receivers are dropped, the upstream task stops (it cannot be restarted).
pub struct SharedStream<S>
where
  S: Stream + Send + 'static,
  S::Item: Clone + Send + 'static,
{
  inner: Arc<Inner<S>>,
  receiver: Option<async_broadcast::Receiver<S::Item>>,
}

impl<S> Clone for SharedStream<S>
where
  S: Stream + Send + 'static,
  S::Item: Clone + Send + 'static,
{
  fn clone(&self) -> Self {
    Self {
      inner: Arc::clone(&self.inner),
      receiver: None,
    }
  }
}

impl<S> Stream for SharedStream<S>
where
  S: Stream + Send + 'static,
  S::Item: Clone + Send + 'static,
{
  type Item = S::Item;

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

/// Share-related stream extensions (e.g. `share()`).
pub trait StreamShareExt: Stream + Sized
where
  Self: Send + 'static,
  Self::Item: Clone + Send + 'static,
{
  /// Share the upstream stream across multiple subscribers (clones), using a broadcast channel.
  ///
  /// Equivalent to `share_with_capacity(16)`.
  fn share(self) -> SharedStream<Self> {
    self.share_with_capacity(16)
  }

  /// Like `share()`, but with a custom channel capacity.
  fn share_with_capacity(self, capacity: usize) -> SharedStream<Self> {
    let (mut sender, receiver) = async_broadcast::broadcast(capacity);

    // Wait for at least one receiver to be actively polled before sending.
    // This avoids immediately failing/dropping items when the first subscriber hasn't yet
    // started polling.
    sender.set_await_active(true);

    let inner = Arc::new(Inner {
      sender,
      seed_receiver: StdMutex::new(Some(receiver)),
      source: tokio::sync::Mutex::new(Some(self)),
      started: AtomicBool::new(false),
    });

    SharedStream {
      inner,
      receiver: None,
    }
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
    let shared = futures::stream::iter([1_u32, 2, 3, 4, 5]).share();

    let a = shared.clone();
    let b = shared.clone();

    let ta = tokio::spawn(async move { a.collect::<Vec<_>>().await });
    let tb = tokio::spawn(async move { b.collect::<Vec<_>>().await });

    let va = ta.await.unwrap();
    let vb = tb.await.unwrap();

    assert_eq!(va, vec![1, 2, 3, 4, 5]);
    assert_eq!(vb, vec![1, 2, 3, 4, 5]);
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
    .share();

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
    let shared = MpscStream(rx).share();

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
    .share();

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
}
