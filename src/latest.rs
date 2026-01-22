use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

/// A stream that yields only the latest value from the upstream when polled.
///
/// When polled, this stream drains all immediately available items from the
/// upstream and yields only the most recent one. All previously buffered items
/// are discarded.
///
/// This is useful when you only care about the most recent state and want to
/// skip intermediate values that arrived while the downstream was not polling.
pub struct LatestStream<T> {
  source: Pin<Box<T>>,
}

impl<T: Stream> Stream for LatestStream<T> {
  type Item = T::Item;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let mut latest: Option<T::Item> = None;

    loop {
      match self.source.as_mut().poll_next(cx) {
        Poll::Ready(Some(item)) => {
          // Keep draining and store only the latest
          latest = Some(item);
        }
        Poll::Ready(None) => {
          // Stream ended - return any pending latest or None
          return Poll::Ready(latest);
        }
        Poll::Pending => {
          // No more ready items - return latest if we got one
          if let Some(item) = latest {
            return Poll::Ready(Some(item));
          }
          return Poll::Pending;
        }
      }
    }
  }
}

/// Extension trait that adds `.latest()` to streams.
pub trait StreamLatestExt: Stream + Sized {
  /// Wraps this stream so that only the latest available value is yielded when polled.
  ///
  /// This is useful when you have a fast producer and a slow consumer, and you
  /// only care about the most recent value rather than processing every item.
  ///
  /// # Example
  ///
  /// ```
  /// use futures::StreamExt;
  /// use futures::executor::block_on;
  /// use streamx::StreamLatestExt;
  ///
  /// let mut latest = futures::stream::iter([1_u32, 2, 3]).latest();
  ///
  /// // The iterator is immediately ready, so only the latest value (3) is returned.
  /// let value = block_on(async { latest.next().await });
  /// assert_eq!(value, Some(3));
  /// ```
  fn latest(self) -> LatestStream<Self> {
    LatestStream {
      source: Box::pin(self),
    }
  }
}

impl<T: Stream + Sized> StreamLatestExt for T {}

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;

  use super::StreamLatestExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn latest_returns_most_recent_value() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();

    let mut latest = MpscStream(rx).latest();

    // Should skip 1 and 2, returning only 3
    let value = latest.next().await;
    assert_eq!(value, Some(3));
  }

  #[tokio::test]
  async fn latest_returns_each_value_when_polled_immediately() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut latest = MpscStream(rx).latest();

    tx.send(1).unwrap();
    assert_eq!(latest.next().await, Some(1));

    tx.send(2).unwrap();
    assert_eq!(latest.next().await, Some(2));

    tx.send(3).unwrap();
    assert_eq!(latest.next().await, Some(3));
  }

  #[tokio::test]
  async fn latest_returns_none_when_stream_ends() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut latest = MpscStream(rx).latest();

    drop(tx);

    assert_eq!(latest.next().await, None);
  }

  #[tokio::test]
  async fn latest_returns_final_value_when_stream_ends_with_pending_items() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    drop(tx);

    let mut latest = MpscStream(rx).latest();

    // Should return the latest value (2) even though stream is closed
    assert_eq!(latest.next().await, Some(2));
    // Then return None
    assert_eq!(latest.next().await, None);
  }

  #[tokio::test]
  async fn latest_works_with_iter_stream() {
    let mut latest = futures::stream::iter([1_u32, 2, 3, 4, 5]).latest();

    // All values are immediately ready, so should return only the last
    assert_eq!(latest.next().await, Some(5));
    assert_eq!(latest.next().await, None);
  }

  #[tokio::test]
  async fn latest_skips_intermediate_values_between_polls() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut latest = MpscStream(rx).latest();

    // First batch
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(latest.next().await, Some(2));

    // Second batch
    tx.send(10).unwrap();
    tx.send(20).unwrap();
    tx.send(30).unwrap();
    assert_eq!(latest.next().await, Some(30));

    // Single value
    tx.send(100).unwrap();
    assert_eq!(latest.next().await, Some(100));

    drop(tx);
    assert_eq!(latest.next().await, None);
  }
}
