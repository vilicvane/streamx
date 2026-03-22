use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

/// A stream created by [`combine_latest_all`] or [`StreamCombineLatestAllExt::combine_latest_all`].
pub struct CombineLatestIterStream<TStream>
where
  TStream: Stream,
  TStream::Item: Clone,
{
  streams: Vec<Pin<Box<TStream>>>,
  latest: Vec<Option<TStream::Item>>,
  done: Vec<bool>,
}

impl<TStream> CombineLatestIterStream<TStream>
where
  TStream: Stream,
  TStream::Item: Clone,
{
  pub fn new<TInto>(streams: TInto) -> Self
  where
    TInto: IntoIterator<Item = TStream>,
  {
    let streams: Vec<Pin<Box<TStream>>> = streams.into_iter().map(Box::pin).collect();
    let len = streams.len();
    Self {
      streams,
      latest: vec![None; len],
      done: vec![false; len],
    }
  }
}

impl<TStream> Stream for CombineLatestIterStream<TStream>
where
  TStream: Stream,
  TStream::Item: Clone,
{
  type Item = Vec<TStream::Item>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    // Safety: we never move any `Pin<Box<..>>` fields. We only poll them and update the
    // non-pinned `Option<Item>` state.
    let this = unsafe { self.get_unchecked_mut() };

    if this.streams.is_empty() {
      return Poll::Ready(None);
    }

    let mut updated = false;

    loop {
      let mut made_progress = false;

      for index in 0..this.streams.len() {
        if this.done[index] {
          continue;
        }

        match this.streams[index].as_mut().poll_next(cx) {
          Poll::Ready(Some(item)) => {
            this.latest[index] = Some(item);
            updated = true;
            made_progress = true;
          }
          Poll::Ready(None) => {
            this.done[index] = true;
            if this.latest[index].is_none() {
              // This stream ended before its first item, so the combined stream can never
              // produce a full vector.
              return Poll::Ready(None);
            }
          }
          Poll::Pending => {}
        }
      }

      if !made_progress {
        break;
      }
    }

    if updated && this.latest.iter().all(|value| value.is_some()) {
      let values = this
        .latest
        .iter()
        .map(|value| {
          value
            .as_ref()
            .expect("latest must exist when all latest are present")
            .clone()
        })
        .collect();
      return Poll::Ready(Some(values));
    }

    if this.done.iter().all(|done| *done) {
      return Poll::Ready(None);
    }

    Poll::Pending
  }
}

/// Combine the latest values from a collection of streams.
///
/// This yields a vector with one item per input stream, matching the iterator order.
pub fn combine_latest_all<TInto, TStream>(streams: TInto) -> CombineLatestIterStream<TStream>
where
  TInto: IntoIterator<Item = TStream>,
  TStream: Stream,
  TStream::Item: Clone,
{
  CombineLatestIterStream::new(streams)
}

/// Combine the latest values from a collection of streams.
///
/// This yields a vector with one item per input stream, matching the iterator order.
pub trait StreamCombineLatestAllExt<TStream>
where
  TStream: Stream,
  TStream::Item: Clone,
{
  /// Create a combined stream that yields vectors of the latest items.
  ///
  /// # Example
  ///
  /// ```
  /// use futures::StreamExt;
  /// use futures::executor::block_on;
  /// use streamx::StreamCombineLatestAllExt;
  ///
  /// let streams = vec![
  ///   futures::stream::iter([1_u32, 2]),
  ///   futures::stream::iter([10_u32, 11]),
  /// ];
  ///
  /// let mut combined = streams.combine_latest_all();
  /// let value = block_on(async { combined.next().await });
  /// assert_eq!(value, Some(vec![2, 11]));
  /// ```
  fn combine_latest_all(self) -> CombineLatestIterStream<TStream>;
}

impl<TInto, TStream> StreamCombineLatestAllExt<TStream> for TInto
where
  TInto: IntoIterator<Item = TStream>,
  TStream: Stream,
  TStream::Item: Clone,
{
  fn combine_latest_all(self) -> CombineLatestIterStream<TStream> {
    combine_latest_all(self)
  }
}

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;
  use lits::duration;

  use super::{StreamCombineLatestAllExt, combine_latest_all};

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn combine_latest_iter_yields_latest_values() {
    let streams = vec![
      futures::stream::iter(vec![1_u32, 2, 3]),
      futures::stream::iter(vec![10_u32, 11]),
    ];

    let mut combined = streams.combine_latest_all();

    assert_eq!(combined.next().await, Some(vec![3, 11]));
    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_all_fn_yields_latest_values() {
    let streams = vec![
      futures::stream::iter(vec![1_u32, 2, 3]),
      futures::stream::iter(vec![10_u32, 11]),
    ];

    let mut combined = combine_latest_all(streams);

    assert_eq!(combined.next().await, Some(vec![3, 11]));
    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_ends_if_any_stream_has_no_first_item() {
    let streams = vec![
      futures::stream::iter(vec![1_u32, 2, 3]),
      futures::stream::iter(Vec::<u32>::new()),
    ];

    let mut combined = streams.combine_latest_all();

    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_empty_collection() {
    let streams: Vec<futures::stream::Iter<std::vec::IntoIter<u32>>> = vec![];

    let mut combined = streams.combine_latest_all();

    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_single_stream() {
    let streams = vec![futures::stream::iter(vec![1_u32, 2, 3])];

    let mut combined = streams.combine_latest_all();

    assert_eq!(combined.next().await, Some(vec![3]));
    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_emits_on_updates() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let streams = vec![MpscStream(rx1), MpscStream(rx2)];
    let mut combined = streams.combine_latest_all();

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some(vec![1, 10]));

    tx1.send(2).unwrap();
    assert_eq!(combined.next().await, Some(vec![2, 10]));

    tx2.send(11).unwrap();
    assert_eq!(combined.next().await, Some(vec![2, 11]));
  }

  #[tokio::test]
  async fn combine_latest_iter_waits_for_all_first_items() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let streams = vec![MpscStream(rx1), MpscStream(rx2)];
    let mut combined = streams.combine_latest_all();

    tx1.send(1).unwrap();

    // Should not yield yet because stream2 has no first value.
    let timed = tokio::time::timeout(duration!("25ms"), combined.next()).await;
    assert!(timed.is_err());

    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some(vec![1, 10]));
  }

  #[tokio::test]
  async fn combine_latest_iter_ends_when_all_streams_end() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let streams = vec![MpscStream(rx1), MpscStream(rx2)];
    let mut combined = streams.combine_latest_all();

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some(vec![1, 10]));

    drop(tx1);
    drop(tx2);

    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_continues_when_some_streams_end() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let streams = vec![MpscStream(rx1), MpscStream(rx2)];
    let mut combined = streams.combine_latest_all();

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some(vec![1, 10]));

    // Close stream1 but keep stream2 open.
    drop(tx1);

    tx2.send(11).unwrap();
    assert_eq!(combined.next().await, Some(vec![1, 11]));

    tx2.send(12).unwrap();
    assert_eq!(combined.next().await, Some(vec![1, 12]));

    drop(tx2);
    assert_eq!(combined.next().await, None);
  }
}
