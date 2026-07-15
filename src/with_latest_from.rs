use std::{
  pin::Pin,
  task::{Context, Poll},
};

use futures::Stream;

use crate::{LatestStream, StreamLatestExt, hot::WORK_BUDGET};

/// A pull-based primary stream paired with a hot, conflating secondary stream.
pub struct WithLatestFromStream<TSource, TFrom>
where
  TSource: Stream,
  TFrom: Stream,
  TFrom::Item: Clone,
{
  source: Pin<Box<TSource>>,
  from: Option<LatestStream<TFrom>>,
  latest_from: Option<TFrom::Item>,
  source_done: bool,
}

impl<TSource, TFrom> Stream for WithLatestFromStream<TSource, TFrom>
where
  TSource: Stream,
  TFrom: Stream,
  TFrom::Item: Clone,
{
  type Item = (TSource::Item, TFrom::Item);

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    // The pinned source is never moved.
    let this = unsafe { self.get_unchecked_mut() };

    if this.source_done {
      return Poll::Ready(None);
    }

    if let Some(from) = this.from.as_mut() {
      match Pin::new(from).poll_next(cx) {
        Poll::Ready(Some(value)) => this.latest_from = Some(value),
        Poll::Ready(None) => {
          // A completed secondary retains its last value, but no longer needs a
          // task or receiver.
          this.from = None;
        }
        Poll::Pending => {}
      }
    }

    // Spawning is immediate, scheduling is not. Do not let a synchronous
    // primary complete before the secondary driver has performed its initial
    // subscription poll.
    if this.latest_from.is_none()
      && this
        .from
        .as_ref()
        .is_some_and(|from| !from.driver_started())
    {
      return Poll::Pending;
    }

    for _ in 0..WORK_BUDGET {
      match this.source.as_mut().poll_next(cx) {
        Poll::Ready(Some(source_item)) => {
          if let Some(from_item) = this.latest_from.as_ref() {
            return Poll::Ready(Some((source_item, from_item.clone())));
          }
        }
        Poll::Ready(None) => {
          this.source_done = true;
          this.from = None;
          return Poll::Ready(None);
        }
        Poll::Pending => return Poll::Pending,
      }
    }

    // A synchronously-ready primary without secondary state must not monopolize
    // the executor while its discarded items are drained.
    cx.waker().wake_by_ref();
    Poll::Pending
  }
}

/// Extension trait that adds [`with_latest_from`](StreamWithLatestFromExt::with_latest_from).
pub trait StreamWithLatestFromExt: Stream + Sized {
  /// Pair primary items with the latest secondary item.
  ///
  /// Only primary items trigger output and remain downstream-backpressured. The
  /// secondary starts polling immediately and is conflated to one latest value.
  /// This must be called from within a Tokio runtime.
  fn with_latest_from<TFrom>(self, from: TFrom) -> WithLatestFromStream<Self, TFrom>
  where
    TFrom: Stream + Send + 'static,
    TFrom::Item: Clone + Send + 'static,
  {
    WithLatestFromStream {
      source: Box::pin(self),
      from: Some(from.latest()),
      latest_from: None,
      source_done: false,
    }
  }
}

impl<T: Stream + Sized> StreamWithLatestFromExt for T {}

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

  use super::StreamWithLatestFromExt;
  use crate::{StreamLatestExt, StreamShareReplayExt};

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn secondary_progresses_before_downstream_poll() {
    let (source_tx, source_rx) = tokio::sync::mpsc::unbounded_channel();
    let (from_tx, from_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut stream = MpscStream(source_rx).with_latest_from(MpscStream(from_rx));

    from_tx.send(10).unwrap();
    from_tx.send(20).unwrap();
    tokio::time::sleep(duration!("10ms")).await;
    source_tx.send(1).unwrap();

    assert_eq!(stream.next().await, Some((1, 20)));
  }

  #[tokio::test]
  async fn only_primary_items_trigger_output() {
    let (source_tx, source_rx) = tokio::sync::mpsc::unbounded_channel();
    let (from_tx, from_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut stream = MpscStream(source_rx).with_latest_from(MpscStream(from_rx));

    from_tx.send(10).unwrap();
    tokio::time::sleep(duration!("10ms")).await;
    assert!(
      tokio::time::timeout(duration!("20ms"), stream.next())
        .await
        .is_err()
    );

    source_tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some((1, 10)));
  }

  #[tokio::test]
  async fn primary_is_not_polled_until_downstream_demands() {
    struct CountPending {
      polls: Arc<AtomicUsize>,
    }

    impl Stream for CountPending {
      type Item = u32;

      fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Pending
      }
    }

    let primary_polls = Arc::new(AtomicUsize::new(0));
    let secondary_polls = Arc::new(AtomicUsize::new(0));
    let stream = CountPending {
      polls: Arc::clone(&primary_polls),
    }
    .with_latest_from(CountPending {
      polls: Arc::clone(&secondary_polls),
    });

    tokio::time::sleep(duration!("10ms")).await;
    assert_eq!(primary_polls.load(Ordering::Relaxed), 0);
    assert!(secondary_polls.load(Ordering::Relaxed) > 0);
    drop(stream);
  }

  #[tokio::test]
  async fn completed_secondary_reuses_its_last_value() {
    let source = futures::stream::iter([1, 2, 3]);
    let from = futures::stream::iter([10, 20]);
    let stream = source.with_latest_from(from);

    assert_eq!(
      stream.collect::<Vec<_>>().await,
      vec![(1, 20), (2, 20), (3, 20)]
    );
  }

  #[tokio::test]
  async fn empty_secondary_discards_primary_and_completes_with_it() {
    let stream = futures::stream::iter([1, 2, 3]).with_latest_from(futures::stream::empty::<u32>());

    assert_eq!(stream.collect::<Vec<_>>().await, vec![]);
  }

  #[tokio::test]
  async fn infinite_ready_primary_yields_without_secondary_state() {
    let mut stream =
      futures::stream::repeat(1_u32).with_latest_from(futures::stream::pending::<u32>());

    assert!(
      tokio::time::timeout(duration!("10ms"), stream.next())
        .await
        .is_err()
    );
  }

  #[tokio::test]
  async fn restarted_chain_uses_replayed_secondary_without_a_new_update() {
    let (from_tx, from_rx) = tokio::sync::mpsc::unbounded_channel();
    let shared_from = MpscStream(from_rx).share_replay_latest();

    from_tx.send(10).unwrap();
    tokio::time::sleep(duration!("10ms")).await;

    let first = futures::stream::pending::<u32>()
      .with_latest_from(shared_from.clone())
      .latest();
    drop(first);

    let (source_tx, source_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut restarted = MpscStream(source_rx)
      .with_latest_from(shared_from.clone())
      .latest();

    source_tx.send(1).unwrap();
    assert_eq!(
      tokio::time::timeout(duration!("1s"), restarted.next()).await,
      Ok(Some((1, 10)))
    );
  }
}
