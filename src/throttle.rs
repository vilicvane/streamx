use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use futures::future::BoxFuture;

use crate::scheduler::Scheduler;

/// Internal state for the throttle stream.
enum ThrottleState {
  /// The stream can emit the next source value immediately.
  Ready,
  /// The stream is within the throttle window, dropping source values.
  Throttled,
  /// Stream is done.
  Done,
}

/// A stream that emits at most one value per scheduler window.
///
/// This is a leading-edge throttle operator: the first value in each window
/// is emitted immediately, and subsequent values are dropped until the
/// scheduler completes.
///
/// Behavior:
/// - When a value arrives while the stream is ready, it is emitted immediately
/// - A scheduler window starts after each emitted value
/// - Values arriving while throttled are dropped
/// - When the scheduler window completes, the stream becomes ready again
pub struct ThrottleStream<TSource>
where
  TSource: Stream,
{
  source: Pin<Box<TSource>>,
  scheduler: Scheduler,
  state: ThrottleState,
  scheduled_future: Option<BoxFuture<'static, ()>>,
}

impl<TSource> Stream for ThrottleStream<TSource>
where
  TSource: Stream,
{
  type Item = TSource::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = unsafe { self.get_unchecked_mut() };

    loop {
      match this.state {
        ThrottleState::Done => {
          return Poll::Ready(None);
        }

        ThrottleState::Ready => match this.source.as_mut().poll_next(cx) {
          Poll::Ready(Some(value)) => {
            // Emit immediately, then enter throttled state.
            this.scheduled_future = Some(this.scheduler.schedule());
            this.state = ThrottleState::Throttled;
            return Poll::Ready(Some(value));
          }
          Poll::Ready(None) => {
            this.state = ThrottleState::Done;
            return Poll::Ready(None);
          }
          Poll::Pending => {
            return Poll::Pending;
          }
        },

        ThrottleState::Throttled => {
          // While throttled, keep draining and dropping immediately available values.
          match this.source.as_mut().poll_next(cx) {
            Poll::Ready(Some(_)) => {
              continue;
            }
            Poll::Ready(None) => {
              this.scheduled_future = None;
              this.state = ThrottleState::Done;
              return Poll::Ready(None);
            }
            Poll::Pending => {
              // No value ready right now; check if throttle window finished.
              if let Some(ref mut future) = this.scheduled_future {
                match future.as_mut().poll(cx) {
                  Poll::Ready(()) => {
                    this.scheduled_future = None;
                    this.state = ThrottleState::Ready;
                    continue;
                  }
                  Poll::Pending => {
                    return Poll::Pending;
                  }
                }
              } else {
                // Should not happen in Throttled state, but avoid stalling.
                this.state = ThrottleState::Ready;
                continue;
              }
            }
          }
        }
      }
    }
  }
}

/// Extension trait that adds `.throttle()` to streams.
pub trait StreamThrottleExt: Stream + Sized {
  /// Throttles the stream using the provided scheduler.
  ///
  /// This operator emits the first value immediately, then suppresses subsequent
  /// values until the scheduler completes. It is useful when you want to limit
  /// how frequently downstream work is triggered.
  ///
  /// # Arguments
  ///
  /// * `scheduler` - Something that can be converted into an `AnyScheduler`
  ///   (e.g., `Duration` or an async function).
  ///
  /// # Example
  ///
  /// ```
  /// use std::time::Duration;
  /// use futures::StreamExt;
  /// use streamx::StreamThrottleExt;
  ///
  /// # async fn example() {
  /// let stream = futures::stream::iter([1, 2, 3]);
  ///
  /// // Using Duration directly
  /// let throttled = stream.throttle(Duration::from_millis(100));
  /// # }
  /// ```
  ///
  /// ```
  /// use std::time::Duration;
  /// use futures::StreamExt;
  /// use streamx::StreamThrottleExt;
  ///
  /// # async fn example() {
  /// let stream = futures::stream::iter([1, 2, 3]);
  ///
  /// // Using an async function
  /// let throttled = stream.throttle(|| async {
  ///   tokio::time::sleep(Duration::from_millis(100)).await;
  /// });
  /// # }
  /// ```
  fn throttle<TScheduler>(self, scheduler: TScheduler) -> ThrottleStream<Self>
  where
    TScheduler: Into<Scheduler>,
  {
    ThrottleStream {
      source: Box::pin(self),
      scheduler: scheduler.into(),
      state: ThrottleState::Ready,
      scheduled_future: None,
    }
  }
}

impl<T: Stream + Sized> StreamThrottleExt for T {}

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;
  use lits::duration;

  use super::StreamThrottleExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn throttle_emits_first_value_immediately() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();

    let mut stream = MpscStream(rx).throttle(duration!("100ms"));

    assert_eq!(stream.next().await, Some(1));
  }

  #[tokio::test]
  async fn throttle_drops_values_during_window() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).throttle(duration!("100ms"));

    tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some(1));

    tx.send(2).unwrap();
    tx.send(3).unwrap();

    // Should not emit while still throttled.
    let result = tokio::time::timeout(duration!("40ms"), stream.next()).await;
    assert!(result.is_err());
  }

  #[tokio::test]
  async fn throttle_emits_values_with_sufficient_gaps() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).throttle(duration!("30ms"));

    tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some(1));

    tokio::time::sleep(duration!("50ms")).await;
    let result = tokio::time::timeout(duration!("20ms"), stream.next()).await;
    assert!(result.is_err());
    tx.send(2).unwrap();
    assert_eq!(stream.next().await, Some(2));

    tokio::time::sleep(duration!("50ms")).await;
    let result = tokio::time::timeout(duration!("20ms"), stream.next()).await;
    assert!(result.is_err());
    tx.send(3).unwrap();
    assert_eq!(stream.next().await, Some(3));

    drop(tx);
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn throttle_drops_queued_values_until_window_reopens() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).throttle(duration!("100ms"));

    tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some(1));

    // These values are sent during the throttle window, so they should be dropped.
    tx.send(2).unwrap();
    tx.send(3).unwrap();

    // Advance polling after the throttle duration so the window reopens.
    tokio::time::sleep(duration!("120ms")).await;
    let result = tokio::time::timeout(duration!("20ms"), stream.next()).await;
    assert!(result.is_err());

    tx.send(4).unwrap();
    assert_eq!(stream.next().await, Some(4));

    drop(tx);
  }

  #[tokio::test]
  async fn throttle_completes_when_source_ends_during_window() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).throttle(duration!("100ms"));

    tx.send(42).unwrap();
    assert_eq!(stream.next().await, Some(42));

    // Source ends before throttle window completes.
    drop(tx);
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn throttle_handles_empty_stream() {
    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    drop(_tx);

    let mut stream = MpscStream(rx).throttle(duration!("50ms"));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn throttle_with_iter_stream_emits_only_first_value() {
    let mut stream = futures::stream::iter([1_u32, 2, 3, 4, 5]).throttle(duration!("50ms"));

    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn throttle_with_async_fn_scheduler() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).throttle(|| async {
      tokio::time::sleep(duration!("50ms")).await;
    });

    tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some(1));

    tx.send(2).unwrap();
    // Value 2 is within the throttle window and should be dropped.
    let result = tokio::time::timeout(duration!("20ms"), stream.next()).await;
    assert!(result.is_err());

    // Reopen throttle window first, then send a fresh value.
    tokio::time::sleep(duration!("70ms")).await;
    let result = tokio::time::timeout(duration!("20ms"), stream.next()).await;
    assert!(result.is_err());

    tx.send(3).unwrap();
    assert_eq!(stream.next().await, Some(3));

    drop(tx);
  }
}
