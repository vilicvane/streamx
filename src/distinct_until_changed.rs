use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

/// A stream that only emits values when they differ from the previous value.
///
/// This stream filters out consecutive duplicate values, only yielding items
/// when they are different from the immediately preceding item.
///
/// This is useful when you want to skip duplicate consecutive values and only
/// react to changes.
pub struct DistinctUntilChangedStream<T>
where
  T: Stream,
  T::Item: Clone,
{
  source: Pin<Box<T>>,
  previous: Option<T::Item>,
}

impl<T> Stream for DistinctUntilChangedStream<T>
where
  T: Stream,
  T::Item: PartialEq + Clone,
{
  type Item = T::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = unsafe { self.get_unchecked_mut() };
    loop {
      match this.source.as_mut().poll_next(cx) {
        Poll::Ready(Some(item)) => {
          match &this.previous {
            Some(previous) if previous == &item => {
              // Skip this duplicate value
              continue;
            }
            _ => {
              // This is a new value, emit it
              this.previous = Some(item.clone());
              return Poll::Ready(Some(item));
            }
          }
        }
        Poll::Ready(None) => {
          return Poll::Ready(None);
        }
        Poll::Pending => {
          return Poll::Pending;
        }
      }
    }
  }
}

/// A stream that only emits values when they differ from the previous value,
/// using a custom comparison function.
///
/// This stream filters out consecutive duplicate values based on a custom
/// comparison function, only yielding items when the comparison function
/// returns false (indicating they are different).
pub struct DistinctUntilChangedByStream<T, F>
where
  T: Stream,
  T::Item: Clone,
{
  source: Pin<Box<T>>,
  compare: F,
  previous: Option<T::Item>,
}

impl<T, F> Stream for DistinctUntilChangedByStream<T, F>
where
  T: Stream,
  T::Item: Clone,
  F: FnMut(&T::Item, &T::Item) -> bool,
{
  type Item = T::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = unsafe { self.get_unchecked_mut() };
    loop {
      match this.source.as_mut().poll_next(cx) {
        Poll::Ready(Some(item)) => {
          match &this.previous {
            Some(previous) if (this.compare)(previous, &item) => {
              // Skip this duplicate value
              continue;
            }
            _ => {
              // This is a new value, emit it
              this.previous = Some(item.clone());
              return Poll::Ready(Some(item));
            }
          }
        }
        Poll::Ready(None) => {
          return Poll::Ready(None);
        }
        Poll::Pending => {
          return Poll::Pending;
        }
      }
    }
  }
}

/// Extension trait that adds `.distinct_until_changed()` to streams.
pub trait StreamDistinctUntilChangedExt: Stream + Sized {
  /// Wraps this stream so that only values different from the previous value are yielded.
  ///
  /// This filters out consecutive duplicate values, only emitting items when they
  /// differ from the immediately preceding item. Uses `PartialEq` for comparison.
  ///
  /// # Example
  ///
  /// ```
  /// use futures::StreamExt;
  /// use futures::executor::block_on;
  /// use streamx::StreamDistinctUntilChangedExt;
  ///
  /// let mut stream = futures::stream::iter([1, 1, 2, 2, 3, 1, 1]).distinct_until_changed();
  ///
  /// assert_eq!(block_on(async { stream.next().await }), Some(1));
  /// assert_eq!(block_on(async { stream.next().await }), Some(2));
  /// assert_eq!(block_on(async { stream.next().await }), Some(3));
  /// assert_eq!(block_on(async { stream.next().await }), Some(1));
  /// assert_eq!(block_on(async { stream.next().await }), None);
  /// ```
  fn distinct_until_changed(self) -> DistinctUntilChangedStream<Self>
  where
    Self::Item: PartialEq + Clone,
  {
    DistinctUntilChangedStream {
      source: Box::pin(self),
      previous: None,
    }
  }

  /// Like `distinct_until_changed()`, but uses a custom comparison function.
  ///
  /// The comparison function should return `true` if two items are considered equal
  /// (and the second should be skipped), or `false` if they are different (and the
  /// second should be emitted).
  ///
  /// # Example
  ///
  /// ```
  /// use futures::StreamExt;
  /// use futures::executor::block_on;
  /// use streamx::StreamDistinctUntilChangedExt;
  ///
  /// let mut stream = futures::stream::iter([1, 2, 3, 4, 5])
  ///   .distinct_until_changed_by(|a, b| a % 2 == b % 2);
  ///
  /// // Only emits when parity changes
  /// assert_eq!(block_on(async { stream.next().await }), Some(1));
  /// assert_eq!(block_on(async { stream.next().await }), Some(2));
  /// assert_eq!(block_on(async { stream.next().await }), Some(3));
  /// assert_eq!(block_on(async { stream.next().await }), Some(4));
  /// assert_eq!(block_on(async { stream.next().await }), Some(5));
  /// ```
  fn distinct_until_changed_by<F>(self, compare: F) -> DistinctUntilChangedByStream<Self, F>
  where
    Self::Item: Clone,
    F: FnMut(&Self::Item, &Self::Item) -> bool,
  {
    DistinctUntilChangedByStream {
      source: Box::pin(self),
      compare,
      previous: None,
    }
  }
}

impl<T: Stream + Sized> StreamDistinctUntilChangedExt for T {}

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;

  use super::StreamDistinctUntilChangedExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn distinct_until_changed_filters_consecutive_duplicates() {
    let mut stream = futures::stream::iter([1, 1, 2, 2, 3, 1, 1]).distinct_until_changed();

    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, Some(2));
    assert_eq!(stream.next().await, Some(3));
    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn distinct_until_changed_emits_first_value() {
    let mut stream = futures::stream::iter([1, 2, 3]).distinct_until_changed();

    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, Some(2));
    assert_eq!(stream.next().await, Some(3));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn distinct_until_changed_handles_all_duplicates() {
    let mut stream = futures::stream::iter([1, 1, 1, 1]).distinct_until_changed();

    assert_eq!(stream.next().await, Some(1));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn distinct_until_changed_handles_empty_stream() {
    let mut stream = futures::stream::iter([] as [i32; 0]).distinct_until_changed();

    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn distinct_until_changed_handles_single_value() {
    let mut stream = futures::stream::iter([42]).distinct_until_changed();

    assert_eq!(stream.next().await, Some(42));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn distinct_until_changed_works_with_mpsc() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).distinct_until_changed();

    tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some(1));

    tx.send(1).unwrap();
    // Should skip this duplicate
    tx.send(2).unwrap();
    assert_eq!(stream.next().await, Some(2));

    tx.send(2).unwrap();
    tx.send(2).unwrap();
    tx.send(3).unwrap();
    assert_eq!(stream.next().await, Some(3));

    drop(tx);
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn distinct_until_changed_by_uses_custom_comparison() {
    let mut stream =
      futures::stream::iter([1, 2, 3, 4, 5]).distinct_until_changed_by(|a, b| a % 2 == b % 2);

    // Should emit when parity changes
    assert_eq!(stream.next().await, Some(1)); // odd
    assert_eq!(stream.next().await, Some(2)); // even (parity changed)
    assert_eq!(stream.next().await, Some(3)); // odd (parity changed)
    assert_eq!(stream.next().await, Some(4)); // even (parity changed)
    assert_eq!(stream.next().await, Some(5)); // odd (parity changed)
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn distinct_until_changed_by_filters_based_on_custom_logic() {
    let mut stream = futures::stream::iter([1, 2, 3, 4, 5, 6])
      .distinct_until_changed_by(|a, b| (a / 3) == (b / 3));

    // Should emit when the value divided by 3 changes
    assert_eq!(stream.next().await, Some(1)); // 1/3 = 0
    assert_eq!(stream.next().await, Some(3)); // 3/3 = 1 (changed)
    assert_eq!(stream.next().await, Some(6)); // 6/3 = 2 (changed)
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn distinct_until_changed_by_works_with_mpsc() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(rx).distinct_until_changed_by(|a, b| a == b);

    tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some(1));

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    assert_eq!(stream.next().await, Some(2));

    drop(tx);
    assert_eq!(stream.next().await, None);
  }
}
