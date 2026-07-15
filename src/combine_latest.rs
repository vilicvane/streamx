use std::{
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures::{Stream, StreamExt};

use crate::hot::{HotStream, WORK_BUDGET};

/// Combine heterogeneous streams into a hot, conflating state stream.
///
/// Every input is actively polled from construction. The output retains only
/// the newest unconsumed tuple and starts producing after all inputs have a
/// value. All inputs and items must be `Send + 'static`.
#[macro_export]
macro_rules! combine_latest {
  ($a:expr, $b:expr $(,)?) => {
    $crate::CombineLatest2Stream::new($a, $b)
  };
  ($a:expr, $b:expr, $c:expr $(,)?) => {
    $crate::CombineLatest3Stream::new($a, $b, $c)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
    $crate::CombineLatest4Stream::new($a, $b, $c, $d)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr $(,)?) => {
    $crate::CombineLatest5Stream::new($a, $b, $c, $d, $e)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr $(,)?) => {
    $crate::CombineLatest6Stream::new($a, $b, $c, $d, $e, $f)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr $(,)?) => {
    $crate::CombineLatest7Stream::new($a, $b, $c, $d, $e, $f, $g)
  };
  ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr, $g:expr, $h:expr $(,)?) => {
    $crate::CombineLatest8Stream::new($a, $b, $c, $d, $e, $f, $g, $h)
  };
}

macro_rules! impl_combine_latest {
  (
    $name:ident<$($S:ident),+> {
      $(stream: $stream:ident : $SType:ident, latest: $latest:ident, done: $done:ident),+ $(,)?
    }
    => ($($out:ident),+)
  ) => {
    /// A hot stream created by [`combine_latest!`].
    pub struct $name<$($S),+>
    where
      $($S: Stream,)+
    {
      inner: HotStream<($(<$S as Stream>::Item,)+)>,
      sources: PhantomData<fn() -> ($($S,)+)>,
    }

    impl<$($S),+> $name<$($S),+>
    where
      $(
        $S: Stream + Send + 'static,
        <$S as Stream>::Item: Clone + Send + 'static,
      )+
    {
      #[allow(clippy::too_many_arguments)]
      pub fn new($($stream: $S),+) -> Self {
        let inner = HotStream::spawn(1, |output| async move {
          $(
            let mut $stream = Box::pin($stream);
            let mut $latest = None;
            let mut $done = false;
          )+
          let mut work = 0;

          loop {
            if true $(&& $done)+ {
              break;
            }

            let mut updated = false;
            let mut impossible = false;

            tokio::select! {
              $(
                item = $stream.next(), if !$done => {
                  match item {
                    Some(item) => {
                      $latest = Some(item);
                      updated = true;
                    }
                    None => {
                      $done = true;
                      impossible = $latest.is_none();
                    }
                  }
                }
              )+
              else => break,
            }

            if impossible {
              break;
            }

            if updated && true $(&& $latest.is_some())+ {
              output.send(($(
                $out
                  .as_ref()
                  .expect("all latest values were checked")
                  .clone(),
              )+));
            }

            work += 1;
            if work == WORK_BUDGET {
              work = 0;
              tokio::task::yield_now().await;
            }
          }
        });

        Self {
          inner,
          sources: PhantomData,
        }
      }
    }

    impl<$($S),+> Stream for $name<$($S),+>
    where
      $($S: Stream,)+
    {
      type Item = ($(<$S as Stream>::Item,)+);

      fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
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

#[cfg(test)]
mod tests {
  use std::{
    pin::Pin,
    task::{Context, Poll},
  };

  use futures::{Stream, StreamExt};
  use lits::duration;

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn waits_for_every_initial_value() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel();
    let mut combined = combine_latest!(MpscStream(rx1), MpscStream(rx2));

    tx1.send(1).unwrap();
    assert!(
      tokio::time::timeout(duration!("20ms"), combined.next())
        .await
        .is_err()
    );

    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some((1, 10)));
  }

  #[tokio::test]
  async fn conflates_updates_while_downstream_is_idle() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel();
    let mut combined = combine_latest!(MpscStream(rx1), MpscStream(rx2));

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    tokio::time::sleep(duration!("10ms")).await;
    assert_eq!(combined.next().await, Some((1, 10)));

    tx1.send(2).unwrap();
    tx2.send(11).unwrap();
    tx1.send(3).unwrap();
    tokio::time::sleep(duration!("10ms")).await;
    assert_eq!(combined.next().await, Some((3, 11)));
  }

  #[tokio::test]
  async fn completion_retains_existing_latest_values() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel();
    let mut combined = combine_latest!(MpscStream(rx1), MpscStream(rx2));

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some((1, 10)));
    drop(tx1);

    tx2.send(11).unwrap();
    assert_eq!(combined.next().await, Some((1, 11)));
    drop(tx2);
    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn ends_if_an_input_completes_without_a_value() {
    let combined = combine_latest!(futures::stream::iter([1]), futures::stream::empty::<u32>(),);

    assert_eq!(combined.collect::<Vec<_>>().await, vec![]);
  }

  #[tokio::test]
  async fn supports_three_inputs() {
    let combined = combine_latest!(
      futures::stream::iter([1, 2]),
      futures::stream::iter([10, 20]),
      futures::stream::iter([100, 200]),
    );

    assert_eq!(combined.collect::<Vec<_>>().await, vec![(2, 20, 200)]);
  }
}
