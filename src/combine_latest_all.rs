use std::{
  marker::PhantomData,
  pin::Pin,
  task::{Context, Poll},
};

use futures::{
  Stream, StreamExt,
  stream::{self, BoxStream},
};

use crate::hot::{HotStream, WORK_BUDGET};

enum Event<T> {
  Item(usize, T),
  Done(usize),
}

/// A hot, conflating combination of homogeneous input streams.
pub struct CombineLatestIterStream<TStream>
where
  TStream: Stream,
{
  inner: HotStream<Vec<TStream::Item>>,
  streams: PhantomData<fn() -> TStream>,
}

impl<TStream> CombineLatestIterStream<TStream>
where
  TStream: Stream + Send + 'static,
  TStream::Item: Clone + Send + 'static,
{
  pub fn new<TInto>(streams: TInto) -> Self
  where
    TInto: IntoIterator<Item = TStream>,
  {
    let streams = streams.into_iter().collect::<Vec<_>>();
    let inner = HotStream::spawn(1, |output| async move {
      let stream_count = streams.len();
      if stream_count == 0 {
        return;
      }

      let event_streams = streams
        .into_iter()
        .enumerate()
        .map(|(index, source)| {
          source
            .map(move |item| Event::Item(index, item))
            .chain(stream::once(async move { Event::Done(index) }))
            .boxed()
        })
        .collect::<Vec<BoxStream<'static, Event<TStream::Item>>>>();

      let mut events = stream::select_all(event_streams);
      let mut latest = std::iter::repeat_with(|| None)
        .take(stream_count)
        .collect::<Vec<Option<TStream::Item>>>();
      let mut done_count = 0;
      let mut work = 0;

      while let Some(event) = events.next().await {
        match event {
          Event::Item(index, item) => {
            latest[index] = Some(item);

            if latest.iter().all(Option::is_some) {
              output.send(
                latest
                  .iter()
                  .map(|item| {
                    item
                      .as_ref()
                      .expect("all latest values were checked")
                      .clone()
                  })
                  .collect(),
              );
            }
          }
          Event::Done(index) => {
            if latest[index].is_none() {
              break;
            }

            done_count += 1;
            if done_count == stream_count {
              break;
            }
          }
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
      streams: PhantomData,
    }
  }
}

impl<TStream> Stream for CombineLatestIterStream<TStream>
where
  TStream: Stream,
{
  type Item = Vec<TStream::Item>;

  fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.inner).poll_next(cx)
  }
}

/// Combine a collection of streams into a hot, conflating state stream.
///
/// This must be called from within a Tokio runtime.
pub fn combine_latest_all<TInto, TStream>(streams: TInto) -> CombineLatestIterStream<TStream>
where
  TInto: IntoIterator<Item = TStream>,
  TStream: Stream + Send + 'static,
  TStream::Item: Clone + Send + 'static,
{
  CombineLatestIterStream::new(streams)
}

/// Extension trait for [`combine_latest_all`](StreamCombineLatestAllExt::combine_latest_all).
pub trait StreamCombineLatestAllExt<TStream>
where
  TStream: Stream + Send + 'static,
  TStream::Item: Clone + Send + 'static,
{
  fn combine_latest_all(self) -> CombineLatestIterStream<TStream>;
}

impl<TInto, TStream> StreamCombineLatestAllExt<TStream> for TInto
where
  TInto: IntoIterator<Item = TStream>,
  TStream: Stream + Send + 'static,
  TStream::Item: Clone + Send + 'static,
{
  fn combine_latest_all(self) -> CombineLatestIterStream<TStream> {
    combine_latest_all(self)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    pin::Pin,
    task::{Context, Poll},
  };

  use futures::{Stream, StreamExt};
  use lits::duration;

  use super::{StreamCombineLatestAllExt, combine_latest_all};

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  #[tokio::test]
  async fn combines_and_conflates_while_unpolled() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel();
    let mut combined = vec![MpscStream(rx1), MpscStream(rx2)].combine_latest_all();

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    tx1.send(2).unwrap();
    tx2.send(11).unwrap();
    tokio::time::sleep(duration!("10ms")).await;

    assert_eq!(combined.next().await, Some(vec![2, 11]));
  }

  #[tokio::test]
  async fn completed_inputs_retain_their_latest_value() {
    let (tx1, rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, rx2) = tokio::sync::mpsc::unbounded_channel();
    let mut combined = combine_latest_all(vec![MpscStream(rx1), MpscStream(rx2)]);

    tx1.send(1).unwrap();
    tx2.send(10).unwrap();
    assert_eq!(combined.next().await, Some(vec![1, 10]));
    drop(tx1);

    tx2.send(11).unwrap();
    assert_eq!(combined.next().await, Some(vec![1, 11]));
    drop(tx2);
    assert_eq!(combined.next().await, None);
  }

  #[tokio::test]
  async fn ends_if_any_input_has_no_value() {
    let combined = vec![
      futures::stream::iter(vec![1, 2]),
      futures::stream::iter(Vec::<u32>::new()),
    ]
    .combine_latest_all();

    assert_eq!(combined.collect::<Vec<_>>().await, Vec::<Vec<u32>>::new());
  }

  #[tokio::test]
  async fn empty_collection_completes() {
    let streams: Vec<futures::stream::Empty<u32>> = vec![];
    assert_eq!(streams.combine_latest_all().next().await, None);
  }
}
