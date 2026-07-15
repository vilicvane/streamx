use std::{
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures::{Stream, StreamExt, future::BoxFuture};

use crate::{
  hot::{HotStream, WORK_BUDGET},
  scheduler::Scheduler,
};

/// A hot debounce stream with a bounded, drop-oldest output queue.
///
/// Upstream arrival time and the scheduler determine which items survive. The
/// downstream only drains completed debounce results and cannot delay upstream.
pub struct DebounceStream<TSource>
where
  TSource: Stream,
{
  inner: HotStream<TSource::Item>,
  source: PhantomData<fn() -> TSource>,
}

impl<TSource> Stream for DebounceStream<TSource>
where
  TSource: Stream,
{
  type Item = TSource::Item;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.inner).poll_next(cx)
  }
}

/// Extension trait that adds [`debounce`](StreamDebounceExt::debounce) to streams.
pub trait StreamDebounceExt: Stream + Sized {
  /// Debounce this stream using `scheduler`.
  ///
  /// `capacity` bounds completed results that have not yet been consumed. When
  /// full, the oldest completed result is dropped. The pending item waiting for
  /// its deadline is separate from this queue. This must be called from within
  /// a Tokio runtime.
  fn debounce<TScheduler>(self, scheduler: TScheduler, capacity: usize) -> DebounceStream<Self>
  where
    Self: Send + 'static,
    Self::Item: Send + 'static,
    TScheduler: Into<Scheduler>,
  {
    let scheduler = scheduler.into();
    let inner = HotStream::spawn(capacity, |output| async move {
      let mut source = Box::pin(self);
      let mut pending = None;
      let mut deadline: Option<BoxFuture<'static, ()>> = None;
      let mut work = 0;

      loop {
        if let Some(mut scheduled) = deadline.take() {
          tokio::select! {
            biased;

            item = source.next() => {
              match item {
                Some(item) => {
                  pending = Some(item);
                  deadline = Some(scheduler.schedule());
                }
                None => {
                  if let Some(item) = pending.take() {
                    output.send(item);
                  }
                  break;
                }
              }
            }
            _ = &mut scheduled => {
              if let Some(item) = pending.take() {
                output.send(item);
              }
            }
          }
        } else {
          match source.next().await {
            Some(item) => {
              pending = Some(item);
              deadline = Some(scheduler.schedule());
            }
            None => break,
          }
        }

        work += 1;
        if work == WORK_BUDGET {
          work = 0;
          tokio::task::yield_now().await;
        }
      }
    });

    DebounceStream {
      inner,
      source: PhantomData,
    }
  }
}

impl<T: Stream + Sized> StreamDebounceExt for T {}

#[cfg(test)]
mod tests {
  use std::{
    pin::Pin,
    task::{Context, Poll},
  };

  use futures::{Stream, StreamExt};
  use lits::duration;

  use super::StreamDebounceExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn debounce_is_driven_without_downstream_polling() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut stream = MpscStream(rx).debounce(duration!("20ms"), 4);

    tx.send(1).unwrap();
    tokio::time::sleep(duration!("10ms")).await;
    tx.send(2).unwrap();
    tokio::time::sleep(duration!("30ms")).await;

    assert_eq!(stream.next().await, Some(2));
  }

  #[tokio::test]
  async fn debounce_completed_queue_drops_oldest_at_capacity() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let stream = MpscStream(rx).debounce(duration!("5ms"), 2);

    for value in 1..=3 {
      tx.send(value).unwrap();
      tokio::time::sleep(duration!("10ms")).await;
    }
    drop(tx);
    tokio::time::sleep(duration!("10ms")).await;

    assert_eq!(stream.collect::<Vec<_>>().await, vec![2, 3]);
  }

  #[tokio::test]
  async fn debounce_flushes_pending_item_when_source_completes() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut stream = MpscStream(rx).debounce(duration!("1h"), 1);

    tx.send(42).unwrap();
    drop(tx);

    assert_eq!(stream.next().await, Some(42));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn upstream_wins_when_source_and_deadline_are_ready() {
    let mut stream = futures::stream::iter([1, 2, 3]).debounce(|| async {}, 1);

    assert_eq!(stream.next().await, Some(3));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn debounce_does_not_require_clone_items() {
    struct NotClone(u32);

    let mut stream = futures::stream::iter([NotClone(7)]).debounce(|| async {}, 1);
    assert_eq!(stream.next().await.map(|item| item.0), Some(7));
  }

  #[test]
  #[should_panic(expected = "capacity must be greater than zero")]
  fn debounce_rejects_zero_capacity() {
    let _ = futures::stream::empty::<()>().debounce(|| async {}, 0);
  }
}
