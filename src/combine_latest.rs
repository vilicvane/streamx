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
    $crate::combine_latest::CombineLatest2::new($a, $b)
  };
  ($a:expr, $b:expr, $c:expr $(,)?) => {
    $crate::combine_latest::CombineLatest3::new($a, $b, $c)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
    $crate::combine_latest::CombineLatest4::new($a, $b, $c, $d)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr $(,)?) => {
    $crate::combine_latest::CombineLatest5::new($a, $b, $c, $d, $e)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr $(,)?) => {
    $crate::combine_latest::CombineLatest6::new($a, $b, $c, $d, $e, $f)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr $(,)?) => {
    $crate::combine_latest::CombineLatest7::new($a, $b, $c, $d, $e, $f, $g)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr $(,)?) => {
    $crate::combine_latest::CombineLatest8::new($a, $b, $c, $d, $e, $f, $g, $h)
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
  CombineLatest2<S1, S2> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
  }
  => (latest1, latest2)
);

impl_combine_latest!(
  CombineLatest3<S1, S2, S3> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
  }
  => (latest1, latest2, latest3)
);

impl_combine_latest!(
  CombineLatest4<S1, S2, S3, S4> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
    stream: stream4: S4, latest: latest4, done: done4,
  }
  => (latest1, latest2, latest3, latest4)
);

impl_combine_latest!(
  CombineLatest5<S1, S2, S3, S4, S5> {
    stream: stream1: S1, latest: latest1, done: done1,
    stream: stream2: S2, latest: latest2, done: done2,
    stream: stream3: S3, latest: latest3, done: done3,
    stream: stream4: S4, latest: latest4, done: done4,
    stream: stream5: S5, latest: latest5, done: done5,
  }
  => (latest1, latest2, latest3, latest4, latest5)
);

impl_combine_latest!(
  CombineLatest6<S1, S2, S3, S4, S5, S6> {
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
  CombineLatest7<S1, S2, S3, S4, S5, S6, S7> {
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
  CombineLatest8<S1, S2, S3, S4, S5, S6, S7, S8> {
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

#[cfg(test)]
mod tests {
  use std::pin::Pin;
  use std::task::{Context, Poll};

  use futures::StreamExt;
  use lits::duration;

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
}
