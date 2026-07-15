use std::{
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use crate::hot::{HotStream, WORK_BUDGET};

/// A hot stream that retains at most one unconsumed upstream item.
///
/// The upstream is polled by a Tokio task as soon as this value is constructed.
/// Downstream polling only observes the newest available item and never controls
/// upstream progress. Dropping this stream aborts the task and releases upstream.
pub struct LatestStream<TSource>
where
  TSource: Stream,
{
  inner: HotStream<TSource::Item>,
  source: PhantomData<fn() -> TSource>,
}

impl<TSource> Stream for LatestStream<TSource>
where
  TSource: Stream,
{
  type Item = TSource::Item;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.inner).poll_next(cx)
  }
}

impl<TSource> LatestStream<TSource>
where
  TSource: Stream,
{
  pub(crate) fn driver_started(&self) -> bool {
    self.inner.driver_started()
  }
}

/// Extension trait that adds [`latest`](StreamLatestExt::latest) to streams.
pub trait StreamLatestExt: Stream + Sized {
  /// Actively poll this stream and retain only its newest unconsumed item.
  ///
  /// This method must be called from within a Tokio runtime. Both the stream and
  /// its items must be `Send + 'static` because the upstream is moved into a
  /// background task.
  fn latest(self) -> LatestStream<Self>
  where
    Self: Send + 'static,
    Self::Item: Send + 'static,
  {
    let inner = HotStream::spawn(1, |output| async move {
      let mut source = Box::pin(self);
      let mut work = 0;

      while let Some(item) = source.next().await {
        output.send(item);
        work += 1;

        if work == WORK_BUDGET {
          work = 0;
          tokio::task::yield_now().await;
        }
      }
    });

    LatestStream {
      inner,
      source: PhantomData,
    }
  }
}

impl<T: Stream + Sized> StreamLatestExt for T {}

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

  use super::StreamLatestExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn latest_progresses_before_downstream_poll() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut latest = MpscStream(rx).latest();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();
    tokio::time::sleep(duration!("10ms")).await;

    assert_eq!(latest.next().await, Some(3));
  }

  #[tokio::test]
  async fn latest_conflates_between_polls() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut latest = MpscStream(rx).latest();

    tx.send(1).unwrap();
    assert_eq!(latest.next().await, Some(1));

    tx.send(2).unwrap();
    tx.send(3).unwrap();
    tokio::time::sleep(duration!("10ms")).await;
    assert_eq!(latest.next().await, Some(3));
  }

  #[tokio::test]
  async fn latest_drains_final_item_before_completion() {
    let mut latest = futures::stream::iter([1, 2, 3]).latest();

    assert_eq!(latest.next().await, Some(3));
    assert_eq!(latest.next().await, None);
  }

  #[tokio::test]
  async fn latest_starts_upstream_without_a_downstream_poll() {
    struct PollCounter {
      polls: Arc<AtomicUsize>,
    }

    impl Stream for PollCounter {
      type Item = ();

      fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Pending
      }
    }

    let polls = Arc::new(AtomicUsize::new(0));
    let latest = PollCounter {
      polls: Arc::clone(&polls),
    }
    .latest();

    tokio::time::sleep(duration!("10ms")).await;
    assert!(polls.load(Ordering::Relaxed) > 0);

    drop(latest);
  }

  #[tokio::test]
  async fn dropping_latest_releases_upstream() {
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
    let latest = DropTracker {
      receiver,
      _retained: Arc::clone(&retained),
    }
    .latest();
    drop(retained);

    drop(latest);
    tokio::task::yield_now().await;

    assert!(weak.upgrade().is_none());
  }
}
