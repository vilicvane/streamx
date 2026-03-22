use futures::Stream;
use futures::stream::{SelectAll, select_all};

/// A stream created by [`merge_all`] or [`StreamMergeAllExt::merge_all`].
pub type MergeAllStream<TStream> = SelectAll<TStream>;

/// Merge a collection of streams into a single stream.
///
/// Items are yielded as soon as any input stream produces them. The merged
/// stream completes after all input streams have completed.
pub fn merge_all<TInto, TStream>(streams: TInto) -> MergeAllStream<TStream>
where
  TInto: IntoIterator<Item = TStream>,
  TStream: Stream + Unpin,
{
  select_all(streams)
}

/// Extension trait that adds `.merge_all()` to collections of streams.
pub trait StreamMergeAllExt<TStream>
where
  TStream: Stream + Unpin,
{
  /// Merge a collection of streams into a single stream.
  ///
  /// # Example
  ///
  /// ```
  /// use futures::StreamExt;
  /// use streamx::StreamMergeAllExt;
  ///
  /// # async fn example() {
  /// let streams = vec![
  ///   futures::stream::iter([1_u32, 2]),
  ///   futures::stream::iter([10_u32, 11]),
  /// ];
  ///
  /// let mut merged = streams.merge_all();
  ///
  /// let mut values = vec![];
  /// while let Some(value) = merged.next().await {
  ///   values.push(value);
  /// }
  ///
  /// values.sort_unstable();
  /// assert_eq!(values, vec![1, 2, 10, 11]);
  /// # }
  /// ```
  fn merge_all(self) -> MergeAllStream<TStream>;
}

impl<TInto, TStream> StreamMergeAllExt<TStream> for TInto
where
  TInto: IntoIterator<Item = TStream>,
  TStream: Stream + Unpin,
{
  fn merge_all(self) -> MergeAllStream<TStream> {
    merge_all(self)
  }
}

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;

  use super::{StreamMergeAllExt, merge_all};

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn merge_all_ext_merges_all_values() {
    let streams = vec![
      futures::stream::iter(vec![1_u32, 2]),
      futures::stream::iter(vec![10_u32, 11]),
    ];

    let values = streams.merge_all().collect::<Vec<_>>().await;

    assert_eq!(values, vec![1, 10, 2, 11]);
  }

  #[tokio::test]
  async fn merge_all_fn_merges_all_values() {
    let streams = vec![
      futures::stream::iter(vec![1_u32, 2]),
      futures::stream::iter(vec![10_u32, 11]),
    ];

    let values = merge_all(streams).collect::<Vec<_>>().await;

    assert_eq!(values, vec![1, 10, 2, 11]);
  }

  #[tokio::test]
  async fn merge_all_empty_collection() {
    let streams: Vec<futures::stream::Iter<std::vec::IntoIter<u32>>> = vec![];

    let values = streams.merge_all().collect::<Vec<_>>().await;

    assert!(values.is_empty());
  }

  #[tokio::test]
  async fn merge_all_continues_until_all_streams_end() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let streams = vec![MpscStream(rx1), MpscStream(rx2)];
    let mut merged = streams.merge_all();

    tx1.send(1).unwrap();
    assert_eq!(merged.next().await, Some(1));

    drop(tx1);

    tx2.send(10).unwrap();
    assert_eq!(merged.next().await, Some(10));

    drop(tx2);
    assert_eq!(merged.next().await, None);
  }
}
