use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use futures::future::BoxFuture;

use crate::scheduler::Scheduler;

/// Internal state for the debounce stream.
enum DebounceState<T> {
  /// No value cached, waiting for source.
  Idle,
  /// Value cached, waiting for scheduler to complete.
  Pending { value: T },
  /// Source completed, need to emit cached value (if any) then complete.
  SourceCompleted { cached_value: Option<T> },
  /// Stream is done.
  Done,
}

/// A stream that emits values from the source only after a particular time span
/// (determined by a scheduler) has passed without another source emission.
///
/// This is a rate-limiting operator that delays emissions and drops previous
/// pending emissions if a new value arrives before the scheduled time passes.
///
/// Behavior:
/// - When a value arrives from the source, it is cached and a new scheduler is created
/// - If another value arrives before the scheduler completes, the previous value is dropped
///   and a new scheduler is created for the new value
/// - When the scheduler completes without interruption, the cached value is emitted
/// - If the source completes while a value is pending, that value is emitted before completion
pub struct DebounceStream<TSource>
where
  TSource: Stream,
{
  source: Pin<Box<TSource>>,
  scheduler: Scheduler,
  state: DebounceState<TSource::Item>,
  scheduled_future: Option<BoxFuture<'static, ()>>,
}

impl<TSource> Stream for DebounceStream<TSource>
where
  TSource: Stream,
  TSource::Item: Clone,
{
  type Item = TSource::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = unsafe { self.get_unchecked_mut() };

    loop {
      match &mut this.state {
        DebounceState::Done => {
          return Poll::Ready(None);
        }

        DebounceState::SourceCompleted { cached_value } => {
          let value = cached_value.take();
          this.state = DebounceState::Done;
          return Poll::Ready(value);
        }

        DebounceState::Idle => {
          // Poll the source for a new value
          match this.source.as_mut().poll_next(cx) {
            Poll::Ready(Some(value)) => {
              // Create a new scheduled future
              this.scheduled_future = Some(this.scheduler.schedule());
              this.state = DebounceState::Pending { value };
              // Continue looping to poll the scheduler
            }
            Poll::Ready(None) => {
              this.state = DebounceState::Done;
              return Poll::Ready(None);
            }
            Poll::Pending => {
              return Poll::Pending;
            }
          }
        }

        DebounceState::Pending { value: _ } => {
          // First, check if a new value has arrived from the source (non-blocking)
          match this.source.as_mut().poll_next(cx) {
            Poll::Ready(Some(new_value)) => {
              // New value arrived, replace the cached value and restart the scheduler
              this.scheduled_future = Some(this.scheduler.schedule());
              this.state = DebounceState::Pending { value: new_value };
              // Continue looping to poll the new scheduler
              continue;
            }
            Poll::Ready(None) => {
              // Source completed while we have a pending value
              // We need to emit the pending value, then complete
              let pending_value = std::mem::replace(
                &mut this.state,
                DebounceState::SourceCompleted { cached_value: None },
              );
              if let DebounceState::Pending { value } = pending_value {
                this.state = DebounceState::SourceCompleted {
                  cached_value: Some(value),
                };
              }
              // Continue to handle SourceCompleted state
              continue;
            }
            Poll::Pending => {
              // No new value from source, check if the scheduler has completed
              if let Some(ref mut future) = this.scheduled_future {
                match future.as_mut().poll(cx) {
                  Poll::Ready(()) => {
                    // Scheduler completed, emit the cached value
                    this.scheduled_future = None;
                    let pending_value = std::mem::replace(&mut this.state, DebounceState::Idle);
                    if let DebounceState::Pending { value } = pending_value {
                      return Poll::Ready(Some(value));
                    }
                    // Should not happen, but continue looping
                    continue;
                  }
                  Poll::Pending => {
                    // Still waiting for scheduler
                    return Poll::Pending;
                  }
                }
              } else {
                // No scheduled future (shouldn't happen in Pending state)
                return Poll::Pending;
              }
            }
          }
        }
      }
    }
  }
}

/// Extension trait that adds `.debounce()` to streams.
pub trait StreamDebounceExt: Stream + Sized {
  /// Debounces the stream using the provided scheduler.
  ///
  /// This operator delays emissions from the source stream. When a value arrives,
  /// it is cached and a timer starts based on the scheduler. If another value
  /// arrives before the timer completes, the previous value is dropped and the
  /// timer restarts. When the timer completes without interruption, the cached
  /// value is emitted.
  ///
  /// This is useful for rate-limiting, such as handling rapid user input where you
  /// only want to react to the final value after the user stops typing.
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
  /// use streamx::StreamDebounceExt;
  ///
  /// # async fn example() {
  /// let stream = futures::stream::iter([1, 2, 3]);
  ///
  /// // Using Duration directly
  /// let debounced = stream.debounce(Duration::from_millis(100));
  /// # }
  /// ```
  ///
  /// ```
  /// use std::time::Duration;
  /// use futures::StreamExt;
  /// use streamx::StreamDebounceExt;
  ///
  /// # async fn example() {
  /// let stream = futures::stream::iter([1, 2, 3]);
  ///
  /// // Using an async function
  /// let debounced = stream.debounce(|| async {
  ///   tokio::time::sleep(Duration::from_millis(100)).await;
  /// });
  /// # }
  /// ```
  fn debounce<TScheduler>(self, scheduler: TScheduler) -> DebounceStream<Self>
  where
    Self::Item: Clone,
    TScheduler: Into<Scheduler>,
  {
    DebounceStream {
      source: Box::pin(self),
      scheduler: scheduler.into(),
      state: DebounceState::Idle,
      scheduled_future: None,
    }
  }
}

impl<T: Stream + Sized> StreamDebounceExt for T {}

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;
  use lits::duration;

  use super::StreamDebounceExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn debounce_emits_last_value_after_silence() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).debounce(duration!("50ms"));

    // Send values rapidly
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();

    // Wait for debounce period
    tokio::time::sleep(duration!("100ms")).await;

    // Should only get the last value
    let value = tokio::time::timeout(duration!("50ms"), stream.next())
      .await
      .unwrap();
    assert_eq!(value, Some(3));

    drop(tx);
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn debounce_resets_timer_on_new_value() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).debounce(duration!("100ms"));
    let mut collected = Vec::new();

    // Helper macro to poll stream while sleeping
    macro_rules! sleep_while_polling {
      ($duration:expr) => {{
        let sleep = tokio::time::sleep($duration);
        tokio::pin!(sleep);
        loop {
          tokio::select! {
            biased;
            value = stream.next() => {
              if let Some(v) = value {
                collected.push(v);
              }
            }
            _ = &mut sleep => break,
          }
        }
      }};
    }

    // Send first value
    tx.send(1).unwrap();

    // Wait less than debounce duration while polling
    sleep_while_polling!(duration!("50ms"));

    // Send second value (should reset timer)
    tx.send(2).unwrap();

    // Wait less than debounce duration again while polling
    sleep_while_polling!(duration!("50ms"));

    // Send third value (should reset timer again)
    tx.send(3).unwrap();

    // Now wait for the full debounce duration + buffer while polling
    sleep_while_polling!(duration!("150ms"));

    // Should only have collected the last value
    assert_eq!(collected, vec![3]);

    drop(tx);
  }

  #[tokio::test]
  async fn debounce_emits_cached_value_on_source_complete() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).debounce(duration!("100ms"));

    // Send a value
    tx.send(42).unwrap();

    // Drop sender before debounce completes
    tokio::time::sleep(duration!("25ms")).await;
    drop(tx);

    // Should still get the cached value
    let value = stream.next().await;
    assert_eq!(value, Some(42));

    // Then complete
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn debounce_handles_empty_stream() {
    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    // Drop sender immediately
    drop(_tx);

    let mut stream = MpscStream(rx).debounce(duration!("50ms"));

    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn debounce_with_iter_stream() {
    // When using iter stream, values come synchronously so only the last value
    // survives the debounce
    let stream = futures::stream::iter([1_u32, 2, 3, 4, 5]);
    let mut debounced = stream.debounce(duration!("50ms"));

    // Wait for debounce
    tokio::time::sleep(duration!("100ms")).await;

    let value = tokio::time::timeout(duration!("50ms"), debounced.next())
      .await
      .unwrap();
    assert_eq!(value, Some(5));

    assert_eq!(debounced.next().await, None);
  }

  #[tokio::test]
  async fn debounce_emits_multiple_values_with_sufficient_gaps() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).debounce(duration!("30ms"));

    // Send first value
    tx.send(1).unwrap();
    tokio::time::sleep(duration!("50ms")).await;

    let value = tokio::time::timeout(duration!("50ms"), stream.next())
      .await
      .unwrap();
    assert_eq!(value, Some(1));

    // Send second value with sufficient gap
    tx.send(2).unwrap();
    tokio::time::sleep(duration!("50ms")).await;

    let value = tokio::time::timeout(duration!("50ms"), stream.next())
      .await
      .unwrap();
    assert_eq!(value, Some(2));

    // Send third value
    tx.send(3).unwrap();
    tokio::time::sleep(duration!("50ms")).await;

    let value = tokio::time::timeout(duration!("50ms"), stream.next())
      .await
      .unwrap();
    assert_eq!(value, Some(3));

    drop(tx);
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn debounce_with_async_fn_scheduler() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    // Using an async function as scheduler
    let mut stream = MpscStream(rx).debounce(|| async {
      tokio::time::sleep(duration!("50ms")).await;
    });

    tx.send(1).unwrap();
    tx.send(2).unwrap();

    // Wait for debounce
    tokio::time::sleep(duration!("100ms")).await;

    let value = tokio::time::timeout(duration!("50ms"), stream.next())
      .await
      .unwrap();
    assert_eq!(value, Some(2));

    drop(tx);
  }
}
