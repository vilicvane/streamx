use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

/// A stream that emits `(source_item, latest_from_item)` when the source emits.
///
/// This mirrors Rx's `withLatestFrom` behavior for a single secondary stream:
/// - The output emits only when the source stream emits.
/// - Source values are ignored until the secondary stream has produced at least one value.
/// - The latest secondary value is reused for subsequent source emissions.
/// - The output completes when the source completes.
pub struct WithLatestFromStream<TSource, TFrom>
where
  TSource: Stream,
  TFrom: Stream,
  TFrom::Item: Clone,
{
  source: Pin<Box<TSource>>,
  from: Pin<Box<TFrom>>,
  latest_from: Option<TFrom::Item>,
  from_done: bool,
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
    let this = unsafe { self.get_unchecked_mut() };

    if this.source_done {
      return Poll::Ready(None);
    }

    loop {
      if !this.from_done {
        loop {
          match this.from.as_mut().poll_next(cx) {
            Poll::Ready(Some(value)) => {
              this.latest_from = Some(value);
            }
            Poll::Ready(None) => {
              this.from_done = true;
              break;
            }
            Poll::Pending => {
              break;
            }
          }
        }
      }

      match this.source.as_mut().poll_next(cx) {
        Poll::Ready(Some(source_value)) => {
          if let Some(from_value) = this.latest_from.as_ref() {
            return Poll::Ready(Some((source_value, from_value.clone())));
          }
          // No latest value from `from` yet, so ignore this source item.
          continue;
        }
        Poll::Ready(None) => {
          this.source_done = true;
          return Poll::Ready(None);
        }
        Poll::Pending => {
          return Poll::Pending;
        }
      }
    }
  }
}

/// Extension trait that adds `.with_latest_from()` to streams.
pub trait StreamWithLatestFromExt: Stream + Sized {
  /// Pair each source item with the latest value from another stream.
  ///
  /// This is a simplified variant of Rx's `withLatestFrom` that accepts one
  /// secondary stream.
  ///
  /// # Example
  ///
  /// ```
  /// use futures::StreamExt;
  /// use streamx::StreamWithLatestFromExt;
  ///
  /// # async fn example() {
  /// let source = futures::stream::iter([1_u32, 2, 3]);
  /// let from = futures::stream::iter([10_u32, 20]);
  ///
  /// let mut stream = source.with_latest_from(from);
  /// assert_eq!(stream.next().await, Some((1, 20)));
  /// assert_eq!(stream.next().await, Some((2, 20)));
  /// assert_eq!(stream.next().await, Some((3, 20)));
  /// assert_eq!(stream.next().await, None);
  /// # }
  /// ```
  fn with_latest_from<TFrom>(self, from: TFrom) -> WithLatestFromStream<Self, TFrom>
  where
    TFrom: Stream,
    TFrom::Item: Clone,
  {
    WithLatestFromStream {
      source: Box::pin(self),
      from: Box::pin(from),
      latest_from: None,
      from_done: false,
      source_done: false,
    }
  }
}

impl<T: Stream + Sized> StreamWithLatestFromExt for T {}

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;
  use lits::duration;

  use super::StreamWithLatestFromExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn with_latest_from_waits_until_from_has_value() {
    let (source_tx, source_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (from_tx, from_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(source_rx).with_latest_from(MpscStream(from_rx));

    source_tx.send(1).unwrap();
    source_tx.send(2).unwrap();

    let timed = tokio::time::timeout(duration!("25ms"), stream.next()).await;
    assert!(timed.is_err());

    from_tx.send(10).unwrap();
    source_tx.send(3).unwrap();
    assert_eq!(stream.next().await, Some((3, 10)));
  }

  #[tokio::test]
  async fn with_latest_from_uses_latest_from_value() {
    let (source_tx, source_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (from_tx, from_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(source_rx).with_latest_from(MpscStream(from_rx));

    from_tx.send(10).unwrap();
    source_tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some((1, 10)));

    from_tx.send(20).unwrap();
    source_tx.send(2).unwrap();
    assert_eq!(stream.next().await, Some((2, 20)));

    from_tx.send(30).unwrap();
    source_tx.send(3).unwrap();
    assert_eq!(stream.next().await, Some((3, 30)));
  }

  #[tokio::test]
  async fn with_latest_from_continues_after_from_completes() {
    let (source_tx, source_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (from_tx, from_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(source_rx).with_latest_from(MpscStream(from_rx));

    from_tx.send(10).unwrap();
    drop(from_tx);

    source_tx.send(1).unwrap();
    source_tx.send(2).unwrap();

    assert_eq!(stream.next().await, Some((1, 10)));
    assert_eq!(stream.next().await, Some((2, 10)));
  }

  #[tokio::test]
  async fn with_latest_from_completes_when_source_completes() {
    let (source_tx, source_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (from_tx, from_rx) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut stream = MpscStream(source_rx).with_latest_from(MpscStream(from_rx));

    from_tx.send(10).unwrap();
    source_tx.send(1).unwrap();
    assert_eq!(stream.next().await, Some((1, 10)));

    drop(source_tx);
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn with_latest_from_iter_streams() {
    let source = futures::stream::iter([1_u32, 2, 3]);
    let from = futures::stream::iter([10_u32, 20, 30]);

    let mut stream = source.with_latest_from(from);

    assert_eq!(stream.next().await, Some((1, 30)));
    assert_eq!(stream.next().await, Some((2, 30)));
    assert_eq!(stream.next().await, Some((3, 30)));
    assert_eq!(stream.next().await, None);
  }

  #[tokio::test]
  async fn with_latest_from_from_without_any_value_emits_nothing() {
    let source = futures::stream::iter([1_u32, 2, 3]);
    let from = futures::stream::iter(Vec::<u32>::new());

    let mut stream = source.with_latest_from(from);

    assert_eq!(stream.next().await, None);
  }
}
