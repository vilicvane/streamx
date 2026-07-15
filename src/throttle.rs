use std::{
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt, future::BoxFuture};

use crate::{
  hot::{HotStream, WORK_BUDGET},
  scheduler::Scheduler,
};

/// A hot leading-edge throttle stream with a bounded, drop-oldest output queue.
pub struct ThrottleStream<TSource>
where
  TSource: Stream,
{
  inner: HotStream<TSource::Item>,
  source: PhantomData<fn() -> TSource>,
}

impl<TSource> Stream for ThrottleStream<TSource>
where
  TSource: Stream,
{
  type Item = TSource::Item;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.inner).poll_next(cx)
  }
}

/// Extension trait that adds [`throttle`](StreamThrottleExt::throttle) to streams.
pub trait StreamThrottleExt: Stream + Sized {
  /// Emit the leading item of each scheduler window.
  ///
  /// `capacity` bounds completed, unconsumed results. A full queue drops its
  /// oldest result; downstream polling never delays the source or scheduler.
  /// This must be called from within a Tokio runtime.
  fn throttle<TScheduler>(self, scheduler: TScheduler, capacity: usize) -> ThrottleStream<Self>
  where
    Self: Send + 'static,
    Self::Item: Send + 'static,
    TScheduler: Into<Scheduler>,
  {
    let scheduler = scheduler.into();
    let inner = HotStream::spawn(capacity, |output| async move {
      let mut source = Box::pin(self);
      let mut deadline: Option<BoxFuture<'static, ()>> = None;
      let mut work = 0;

      loop {
        if let Some(mut scheduled) = deadline.take() {
          tokio::select! {
            biased;

            item = source.next() => {
              match item {
                Some(_) => {
                  // The source wins ties with the deadline, so this item still
                  // belongs to the old throttle window and is discarded. Poll
                  // the old deadline once afterward so a tie ends the window
                  // instead of letting a ready source starve it indefinitely.
                  if scheduled.as_mut().now_or_never().is_none() {
                    deadline = Some(scheduled);
                  }
                }
                None => break,
              }
            }
            _ = &mut scheduled => {}
          }
        } else {
          match source.next().await {
            Some(item) => {
              output.send(item);
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

    ThrottleStream {
      inner,
      source: PhantomData,
    }
  }
}

impl<T: Stream + Sized> StreamThrottleExt for T {}

#[cfg(test)]
mod tests {
  use std::{
    pin::Pin,
    task::{Context, Poll},
  };

  use futures::{Stream, StreamExt};
  use lits::duration;

  use super::StreamThrottleExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn throttle_is_driven_without_downstream_polling() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut stream = MpscStream(rx).throttle(duration!("20ms"), 4);

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tokio::time::sleep(duration!("30ms")).await;
    tx.send(3).unwrap();
    tokio::time::sleep(duration!("10ms")).await;

    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, Some(3));
  }

  #[tokio::test]
  async fn throttle_completed_queue_drops_oldest_at_capacity() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let stream = MpscStream(rx).throttle(duration!("5ms"), 2);

    for value in 1..=3 {
      tx.send(value).unwrap();
      tokio::time::sleep(duration!("10ms")).await;
    }
    drop(tx);
    tokio::time::sleep(duration!("10ms")).await;

    assert_eq!(stream.collect::<Vec<_>>().await, vec![2, 3]);
  }

  #[tokio::test]
  async fn throttle_has_no_trailing_flush() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let stream = MpscStream(rx).throttle(duration!("1h"), 2);

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    drop(tx);

    assert_eq!(stream.collect::<Vec<_>>().await, vec![1]);
  }

  #[tokio::test]
  async fn source_wins_a_deadline_tie() {
    let stream = futures::stream::iter([1, 2, 3]).throttle(|| async {}, 2);

    assert_eq!(stream.collect::<Vec<_>>().await, vec![1, 3]);
  }

  #[test]
  #[should_panic(expected = "capacity must be greater than zero")]
  fn throttle_rejects_zero_capacity() {
    let _ = futures::stream::empty::<()>().throttle(|| async {}, 0);
  }
}
