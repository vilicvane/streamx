use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

/// Combine the latest values from multiple streams.
///
/// This is the `streamx` equivalent of Rx's `combineLatest`.
///
/// Semantics:
/// - No item is yielded until each input stream has produced at least one item.
/// - If any stream ends before producing its first item, the combined stream ends immediately.
/// - After the first full set is available, the combined stream yields a new tuple whenever any
///   input stream produces a new item.
/// - The combined stream ends once all input streams have ended.
///
/// Notes:
/// - Requires each `Item: Clone`, since previously seen values may be emitted multiple times.
#[macro_export]
macro_rules! combine_latest {
  ($a:expr, $b:expr $(,)?) => {
    $crate::combine_latest::CombineLatest2Stream::new($a, $b)
  };
  ($a:expr, $b:expr, $c:expr $(,)?) => {
    $crate::combine_latest::CombineLatest3Stream::new($a, $b, $c)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
    $crate::combine_latest::CombineLatest4Stream::new($a, $b, $c, $d)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr $(,)?) => {
    $crate::combine_latest::CombineLatest5Stream::new($a, $b, $c, $d, $e)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr $(,)?) => {
    $crate::combine_latest::CombineLatest6Stream::new($a, $b, $c, $d, $e, $f)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr $(,)?) => {
    $crate::combine_latest::CombineLatest7Stream::new($a, $b, $c, $d, $e, $f, $g)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr $(,)?) => {
    $crate::combine_latest::CombineLatest8Stream::new($a, $b, $c, $d, $e, $f, $g, $h)
  };
}

macro_rules! impl_combine_latest {
  (
    $name:ident<$($S:ident),+> {
      $(
        stream: $stream:ident : $SType:ident,
        latest: $latest:ident,
        done: $done:ident
      ),+ $(,)?
    }
    => ($($out:ident),+)
  ) => {
    /// A stream created by [`combine_latest!`].
    pub struct $name<$($S),+>
    where
      $(
        $S: Stream,
        <$S as Stream>::Item: Clone,
      )+
    {
      $(
        $stream: Pin<Box<$SType>>,
        $latest: Option<<$SType as Stream>::Item>,
        $done: bool,
      )+
    }

    impl<$($S),+> $name<$($S),+>
    where
      $(
        $S: Stream,
        <$S as Stream>::Item: Clone,
      )+
    {
      #[allow(clippy::too_many_arguments)]
      pub fn new($($stream: $S),+) -> Self {
        Self {
          $(
            $stream: Box::pin($stream),
            $latest: None,
            $done: false,
          )+
        }
      }
    }

    impl<$($S),+> Stream for $name<$($S),+>
    where
      $(
        $S: Stream,
        <$S as Stream>::Item: Clone,
      )+
    {
      type Item = ($(<$S as Stream>::Item),+);

      fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Safety: we never move any `Pin<Box<..>>` fields. We only poll them and update the
        // non-pinned `Option<Item>` state.
        let this = unsafe { self.get_unchecked_mut() };

        let mut updated = false;

        loop {
          let mut made_progress = false;

          $(
            if !this.$done {
              match this.$stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                  this.$latest = Some(item);
                  updated = true;
                  made_progress = true;
                }
                Poll::Ready(None) => {
                  this.$done = true;
                  if this.$latest.is_none() {
                    // This stream ended before its first item, so the combined stream can never
                    // produce a full tuple.
                    return Poll::Ready(None);
                  }
                }
                Poll::Pending => {}
              }
            }
          )+

          if !made_progress {
            break;
          }
        }

        if updated && true $(&& this.$latest.is_some())+ {
          return Poll::Ready(Some((
            $(
              this.$out
                .as_ref()
                .expect("latest must exist when all latest are present")
                .clone(),
            )+
          )));
        }

        if true $(&& this.$done)+ {
          return Poll::Ready(None);
        }

        Poll::Pending
      }
    }
  };
}

impl_combine_latest!(
  CombineLatest2Stream<S1, S2> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
  }
  => (latest1, latest2)
);

impl_combine_latest!(
  CombineLatest3Stream<S1, S2, S3> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
  }
  => (latest1, latest2, latest3)
);

impl_combine_latest!(
  CombineLatest4Stream<S1, S2, S3, S4> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
    stream: stream4: S4, latest: latest4, done: done4,
  }
  => (latest1, latest2, latest3, latest4)
);

impl_combine_latest!(
  CombineLatest5Stream<S1, S2, S3, S4, S5> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
    stream: stream4: S4, latest: latest4, done: done4,
    stream: stream5: S5, latest: latest5, done: done5,
  }
  => (latest1, latest2, latest3, latest4, latest5)
);

impl_combine_latest!(
  CombineLatest6Stream<S1, S2, S3, S4, S5, S6> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
    stream: stream4: S4, latest: latest4, done: done4,
    stream: stream5: S5, latest: latest5, done: done5,
    stream: stream6: S6, latest: latest6, done: done6,
  }
  => (latest1, latest2, latest3, latest4, latest5, latest6)
);

impl_combine_latest!(
  CombineLatest7Stream<S1, S2, S3, S4, S5, S6, S7> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
    stream: stream4: S4, latest: latest4, done: done4,
    stream: stream5: S5, latest: latest5, done: done5,
    stream: stream6: S6, latest: latest6, done: done6,
    stream: stream7: S7, latest: latest7, done: done7,
  }
  => (latest1, latest2, latest3, latest4, latest5, latest6, latest7)
);

impl_combine_latest!(
  CombineLatest8Stream<S1, S2, S3, S4, S5, S6, S7, S8> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
    stream: stream4: S4, latest: latest4, done: done4,
    stream: stream5: S5, latest: latest5, done: done5,
    stream: stream6: S6, latest: latest6, done: done6,
    stream: stream7: S7, latest: latest7, done: done7,
    stream: stream8: S8, latest: latest8, done: done8,
  }
  => (latest1, latest2, latest3, latest4, latest5, latest6, latest7, latest8)
);

/// A stream created by [`StreamCombineLatestExt::combine_latest`].
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
pub trait StreamCombineLatestExt<TStream>
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
  /// use streamx::StreamCombineLatestExt;
  ///
  /// let streams = vec![
  ///   futures::stream::iter([1_u32, 2]),
  ///   futures::stream::iter([10_u32, 11]),
  /// ];
  ///
  /// let mut combined = streams.combine_latest();
  /// let value = block_on(async { combined.next().await });
  /// assert_eq!(value, Some(vec![2, 11]));
  /// ```
  fn combine_latest(self) -> CombineLatestIterStream<TStream>;
}

impl<TInto, TStream> StreamCombineLatestExt<TStream> for TInto
where
  TInto: IntoIterator<Item = TStream>,
  TStream: Stream,
  TStream::Item: Clone,
{
  fn combine_latest(self) -> CombineLatestIterStream<TStream> {
    CombineLatestIterStream::new(self)
  }
}

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;
  use lits::duration;

  use super::StreamCombineLatestExt;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> futures::Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn combine_latest_waits_for_all_first_items() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut combined = combine_latest!(MpscStream(rx1), MpscStream(rx2));

    tx1.send(1).unwrap();

    // Should not yield yet because stream2 has no first value.
    let timed = tokio::time::timeout(duration!("25ms"), combined.next()).await;
    assert!(timed.is_err());

    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some((1, 10)));
  }

  #[tokio::test]
  async fn combine_latest_emits_on_updates() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut combined = combine_latest!(MpscStream(rx1), MpscStream(rx2));

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some((1, 10)));

    tx1.send(2).unwrap();
    assert_eq!(combined.next().await, Some((2, 10)));

    tx2.send(11).unwrap();
    assert_eq!(combined.next().await, Some((2, 11)));
  }

  #[tokio::test]
  async fn combine_latest_ends_immediately_if_a_stream_ends_before_first_item() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (_tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    // Close stream2 without any items.
    drop(_tx2);

    let mut combined = combine_latest!(MpscStream(rx1), MpscStream(rx2));

    tx1.send(1).unwrap();

    // Once polled, it should notice stream2 ended before first item.
    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_ends_when_all_streams_end() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut combined = combine_latest!(MpscStream(rx1), MpscStream(rx2));

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some((1, 10)));

    drop(tx1);
    drop(tx2);

    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_supports_three_streams() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx3, rx3) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let mut combined = combine_latest!(MpscStream(rx1), MpscStream(rx2), MpscStream(rx3));

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    let timed = tokio::time::timeout(duration!("25ms"), combined.next()).await;
    assert!(timed.is_err());

    tx3.send(100).unwrap();
    assert_eq!(combined.next().await, Some((1, 10, 100)));

    tx2.send(11).unwrap();
    assert_eq!(combined.next().await, Some((1, 11, 100)));
  }

  #[tokio::test]
  async fn combine_latest_iter_yields_latest_values() {
    let streams = vec![
      futures::stream::iter(vec![1_u32, 2, 3]),
      futures::stream::iter(vec![10_u32, 11]),
    ];

    let mut combined = streams.combine_latest();

    assert_eq!(combined.next().await, Some(vec![3, 11]));
    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_ends_if_any_stream_has_no_first_item() {
    let streams = vec![
      futures::stream::iter(vec![1_u32, 2, 3]),
      futures::stream::iter(Vec::<u32>::new()),
    ];

    let mut combined = streams.combine_latest();

    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_empty_collection() {
    let streams: Vec<futures::stream::Iter<std::vec::IntoIter<u32>>> = vec![];

    let mut combined = streams.combine_latest();

    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_single_stream() {
    let streams = vec![futures::stream::iter(vec![1_u32, 2, 3])];

    let mut combined = streams.combine_latest();

    assert_eq!(combined.next().await, Some(vec![3]));
    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn combine_latest_iter_emits_on_updates() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel::<u32>();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel::<u32>();

    let streams = vec![MpscStream(rx1), MpscStream(rx2)];
    let mut combined = streams.combine_latest();

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
    let mut combined = streams.combine_latest();

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
    let mut combined = streams.combine_latest();

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
    let mut combined = streams.combine_latest();

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
