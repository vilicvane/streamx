use std::{
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  task::{Context, Poll},
};

use futures::{
  Stream,
  task::{ArcWake, AtomicWaker},
};

use crate::hot::WORK_BUDGET;

/// An item with a canonical key used for ordered stream merging.
///
/// The key is extracted once when an item becomes the current head of an input
/// stream. Equal keys are allowed; [`merge_ordered`] resolves them by input
/// order.
pub trait Ordered {
  /// The canonical key used to merge this item with other ordered items.
  type Key: Ord;

  /// Return this item's canonical merge key.
  fn order_key(&self) -> Self::Key;
}

struct Head<TItem>
where
  TItem: Ordered,
{
  item: TItem,
  key: TItem::Key,
}

struct Input<TStream>
where
  TStream: Stream,
  TStream::Item: Ordered,
{
  stream: Pin<Box<TStream>>,
  head: Option<Head<TStream::Item>>,
  done: bool,
}

struct ScanWake {
  generation: Arc<AtomicUsize>,
  downstream: Arc<AtomicWaker>,
}

impl ArcWake for ScanWake {
  fn wake_by_ref(arc_self: &Arc<Self>) {
    arc_self.generation.fetch_add(1, Ordering::AcqRel);
    arc_self.downstream.wake();
  }
}

/// A pull-based, lossless merge of streams ordered by [`Ordered::order_key`].
///
/// Every input must already be ordered by nondecreasing key. The merge retains
/// at most one item from each unfinished input. If an unfinished input has no
/// available head item, no output can be selected until that input produces an
/// item or completes.
pub struct MergeOrderedStream<TStream>
where
  TStream: Stream,
  TStream::Item: Ordered,
{
  inputs: Vec<Input<TStream>>,
  missing_heads: usize,
  poll_cursor: usize,
  scan_remaining: usize,
  scan_generation: usize,
  wake_generation: Arc<AtomicUsize>,
  downstream_waker: Arc<AtomicWaker>,
  terminated: bool,
}

// Input streams are pinned independently in `Pin<Box<_>>`; no field of the
// outer stream relies on structural pinning.
impl<TStream> Unpin for MergeOrderedStream<TStream>
where
  TStream: Stream,
  TStream::Item: Ordered,
{
}

impl<TStream> Stream for MergeOrderedStream<TStream>
where
  TStream: Stream,
  TStream::Item: Ordered,
{
  type Item = TStream::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = self.get_mut();

    if this.terminated {
      return Poll::Ready(None);
    }

    if this.missing_heads > 0 {
      this.downstream_waker.register(cx.waker());

      if this.scan_remaining == 0 {
        this.scan_remaining = this.inputs.len();
        this.scan_generation = this.wake_generation.load(Ordering::Acquire);
      }

      let input_waker = futures::task::waker(Arc::new(ScanWake {
        generation: Arc::clone(&this.wake_generation),
        downstream: Arc::clone(&this.downstream_waker),
      }));
      let mut input_cx = Context::from_waker(&input_waker);
      let mut work = 0;
      while this.missing_heads > 0 && this.scan_remaining > 0 && work < WORK_BUDGET {
        let index = this.poll_cursor;
        this.poll_cursor = (this.poll_cursor + 1) % this.inputs.len();
        this.scan_remaining -= 1;
        work += 1;

        let input = &mut this.inputs[index];
        if input.done || input.head.is_some() {
          continue;
        }

        match input.stream.as_mut().poll_next(&mut input_cx) {
          Poll::Ready(Some(item)) => {
            let key = item.order_key();
            input.head = Some(Head { item, key });
            this.missing_heads -= 1;
          }
          Poll::Ready(None) => {
            input.done = true;
            this.missing_heads -= 1;
          }
          Poll::Pending => {}
        }
      }

      if this.missing_heads > 0 {
        let upstream_woke = this.wake_generation.load(Ordering::Acquire) != this.scan_generation;
        if this.scan_remaining > 0 || upstream_woke {
          cx.waker().wake_by_ref();
        }
        return Poll::Pending;
      }

      this.scan_remaining = 0;
    }

    let mut selected: Option<usize> = None;
    for (index, input) in this.inputs.iter().enumerate() {
      let Some(head) = input.head.as_ref() else {
        continue;
      };

      let should_select = selected.is_none_or(|selected_index| {
        let selected_head = this.inputs[selected_index]
          .head
          .as_ref()
          .expect("selected input must have a head");
        head.key < selected_head.key
      });

      if should_select {
        selected = Some(index);
      }
    }

    let Some(selected) = selected else {
      this.terminated = true;
      return Poll::Ready(None);
    };

    let head = this.inputs[selected]
      .head
      .take()
      .expect("selected input must have a head");
    this.missing_heads += 1;
    this.poll_cursor = selected;
    this.scan_remaining = 1;
    this.scan_generation = this.wake_generation.load(Ordering::Acquire);

    Poll::Ready(Some(head.item))
  }
}

/// Merge streams whose items have a canonical [`Ordered`] key.
///
/// Each input must already be ordered by a nondecreasing key. The result is
/// pull-based and lossless, preserves the order within every input, and emits
/// equal-key items from earlier inputs first. No `Unpin`, `Send`, `'static`, or
/// `Clone` bound is required.
pub fn merge_ordered<TStreams, TStream>(streams: TStreams) -> MergeOrderedStream<TStream>
where
  TStreams: IntoIterator<Item = TStream>,
  TStream: Stream,
  TStream::Item: Ordered,
{
  let inputs = streams
    .into_iter()
    .map(|stream| Input {
      stream: Box::pin(stream),
      head: None,
      done: false,
    })
    .collect::<Vec<_>>();
  let missing_heads = inputs.len();
  let wake_generation = Arc::new(AtomicUsize::new(0));
  let downstream_waker = Arc::new(AtomicWaker::new());

  MergeOrderedStream {
    inputs,
    missing_heads,
    poll_cursor: 0,
    scan_remaining: 0,
    scan_generation: 0,
    wake_generation,
    downstream_waker,
    terminated: false,
  }
}

/// Extension trait that adds `.merge_ordered()` to collections of streams.
pub trait StreamMergeOrderedExt<TStream>: Sized
where
  TStream: Stream,
  TStream::Item: Ordered,
{
  /// Merge these individually ordered streams by their items' canonical keys.
  fn merge_ordered(self) -> MergeOrderedStream<TStream>;
}

impl<TStreams, TStream> StreamMergeOrderedExt<TStream> for TStreams
where
  TStreams: IntoIterator<Item = TStream>,
  TStream: Stream,
  TStream::Item: Ordered,
{
  fn merge_ordered(self) -> MergeOrderedStream<TStream> {
    merge_ordered(self)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
      Arc,
      atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
  };

  use futures::{Stream, StreamExt, task::ArcWake};
  use lits::duration;

  use super::{Ordered, StreamMergeOrderedExt, merge_ordered};
  use crate::hot::WORK_BUDGET;

  #[derive(Debug, PartialEq, Eq)]
  struct Key(u32);

  impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
      Some(self.cmp(other))
    }
  }

  impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
      self.0.cmp(&other.0)
    }
  }

  #[derive(Debug, PartialEq, Eq)]
  struct Event {
    order: u32,
    id: u32,
  }

  impl Ordered for Event {
    type Key = Key;

    fn order_key(&self) -> Self::Key {
      Key(self.order)
    }
  }

  fn event(order: u32, id: u32) -> Event {
    Event { order, id }
  }

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

  struct WakeOnceStream {
    item: Option<Event>,
    wake_before_item: bool,
  }

  impl Stream for WakeOnceStream {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      if self.wake_before_item {
        self.wake_before_item = false;
        cx.waker().wake_by_ref();
        return Poll::Pending;
      }

      Poll::Ready(self.item.take())
    }
  }

  struct WakeCounter(AtomicUsize);

  impl ArcWake for WakeCounter {
    fn wake_by_ref(arc_self: &Arc<Self>) {
      arc_self.0.fetch_add(1, Ordering::Relaxed);
    }
  }

  #[test]
  fn free_function_merges_in_nondecreasing_order() {
    let streams = vec![
      futures::stream::iter(vec![event(1, 10), event(3, 30), event(5, 50)]),
      futures::stream::iter(vec![event(2, 20), event(4, 40), event(6, 60)]),
    ];

    let values = futures::executor::block_on(merge_ordered(streams).collect::<Vec<_>>());

    assert_eq!(
      values,
      vec![
        event(1, 10),
        event(2, 20),
        event(3, 30),
        event(4, 40),
        event(5, 50),
        event(6, 60),
      ]
    );
  }

  #[test]
  fn extension_uses_input_order_for_equal_keys() {
    let streams = vec![
      futures::stream::iter(vec![event(1, 10), event(1, 11)]),
      futures::stream::iter(vec![event(1, 20), event(1, 21)]),
    ];

    let values = futures::executor::block_on(streams.merge_ordered().collect::<Vec<_>>());

    assert_eq!(
      values,
      vec![event(1, 10), event(1, 11), event(1, 20), event(1, 21)]
    );
  }

  #[tokio::test]
  async fn waits_until_every_unfinished_input_has_a_head() {
    let (first_tx, first_rx) = tokio::sync::mpsc::unbounded_channel();
    let (second_tx, second_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut merged = vec![MpscStream(first_rx), MpscStream(second_rx)].merge_ordered();

    first_tx.send(event(2, 20)).unwrap();
    assert!(
      tokio::time::timeout(duration!("20ms"), merged.next())
        .await
        .is_err()
    );

    second_tx.send(event(1, 10)).unwrap();
    assert_eq!(merged.next().await, Some(event(1, 10)));

    drop(second_tx);
    assert_eq!(merged.next().await, Some(event(2, 20)));

    drop(first_tx);
    assert_eq!(merged.next().await, None);
    assert_eq!(merged.next().await, None);
  }

  #[tokio::test]
  async fn construction_is_lazy_and_each_input_retains_only_one_head() {
    struct CountStream {
      items: VecDeque<Event>,
      polls: Arc<AtomicUsize>,
    }

    impl Stream for CountStream {
      type Item = Event;

      fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.items.pop_front())
      }
    }

    let first_polls = Arc::new(AtomicUsize::new(0));
    let second_polls = Arc::new(AtomicUsize::new(0));
    let streams = vec![
      CountStream {
        items: VecDeque::from([event(1, 10), event(3, 30)]),
        polls: Arc::clone(&first_polls),
      },
      CountStream {
        items: VecDeque::from([event(2, 20), event(4, 40)]),
        polls: Arc::clone(&second_polls),
      },
    ];
    let mut merged = streams.merge_ordered();

    assert_eq!(first_polls.load(Ordering::Relaxed), 0);
    assert_eq!(second_polls.load(Ordering::Relaxed), 0);

    assert_eq!(merged.next().await, Some(event(1, 10)));
    assert_eq!(first_polls.load(Ordering::Relaxed), 1);
    assert_eq!(second_polls.load(Ordering::Relaxed), 1);

    assert_eq!(merged.next().await, Some(event(2, 20)));
    assert_eq!(first_polls.load(Ordering::Relaxed), 2);
    assert_eq!(second_polls.load(Ordering::Relaxed), 1);
  }

  #[test]
  fn empty_collection_and_empty_inputs_complete() {
    let empty: Vec<futures::stream::Iter<std::vec::IntoIter<Event>>> = vec![];
    let values = futures::executor::block_on(empty.merge_ordered().collect::<Vec<_>>());
    assert!(values.is_empty());

    let streams = vec![
      futures::stream::iter(Vec::<Event>::new()),
      futures::stream::iter(vec![event(1, 10)]),
    ];
    let values = futures::executor::block_on(streams.merge_ordered().collect::<Vec<_>>());
    assert_eq!(values, vec![event(1, 10)]);
  }

  #[tokio::test]
  async fn accepts_non_unpin_inputs() {
    let source = futures::stream::once(async { event(1, 10) });
    let values = [source].merge_ordered().collect::<Vec<_>>().await;

    assert_eq!(values, vec![event(1, 10)]);
  }

  #[tokio::test]
  async fn many_synchronously_ready_inputs_yield_cooperatively() {
    let streams = (0..=WORK_BUDGET)
      .map(|value| futures::stream::iter(vec![event(value as u32, value as u32)]))
      .collect::<Vec<_>>();

    let values = streams.merge_ordered().collect::<Vec<_>>().await;
    assert_eq!(values.len(), WORK_BUDGET + 1);
    assert!(
      values
        .windows(2)
        .all(|items| items[0].order <= items[1].order)
    );
  }

  #[test]
  fn wake_during_a_budgeted_scan_is_not_lost() {
    let streams = (0..=WORK_BUDGET)
      .map(|value| WakeOnceStream {
        item: Some(event(value as u32, value as u32)),
        wake_before_item: value == 0,
      })
      .collect::<Vec<_>>();
    let mut merged = streams.merge_ordered();
    let wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = futures::task::waker(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(
      Pin::new(&mut merged).poll_next(&mut cx),
      Poll::Pending
    ));

    // Consume the continuation and upstream wakes as an executor may coalesce
    // them into the next poll.
    wakes.0.store(0, Ordering::Relaxed);
    assert!(matches!(
      Pin::new(&mut merged).poll_next(&mut cx),
      Poll::Pending
    ));

    // Completing the first scan must schedule a fresh scan because input zero
    // woke during the earlier chunk.
    assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
    assert_eq!(
      Pin::new(&mut merged).poll_next(&mut cx),
      Poll::Ready(Some(event(0, 0)))
    );
  }

  #[test]
  fn pending_input_uses_the_latest_downstream_waker() {
    let mut first_tx = None;
    let streams = (0..=WORK_BUDGET)
      .map(|value| {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        if value == 0 {
          first_tx = Some(tx);
        } else {
          tx.send(event(value as u32, value as u32)).unwrap();
        }
        MpscStream(rx)
      })
      .collect::<Vec<_>>();
    let mut merged = streams.merge_ordered();
    let wakes_a = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let wakes_b = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker_a = futures::task::waker(Arc::clone(&wakes_a));
    let waker_b = futures::task::waker(Arc::clone(&wakes_b));
    let mut cx_a = Context::from_waker(&waker_a);
    let mut cx_b = Context::from_waker(&waker_b);

    assert!(matches!(
      Pin::new(&mut merged).poll_next(&mut cx_a),
      Poll::Pending
    ));
    wakes_a.0.store(0, Ordering::Relaxed);

    assert!(matches!(
      Pin::new(&mut merged).poll_next(&mut cx_b),
      Poll::Pending
    ));
    assert_eq!(wakes_a.0.load(Ordering::Relaxed), 0);
    assert_eq!(wakes_b.0.load(Ordering::Relaxed), 0);

    first_tx.unwrap().send(event(0, 0)).unwrap();
    assert_eq!(wakes_a.0.load(Ordering::Relaxed), 0);
    assert_eq!(wakes_b.0.load(Ordering::Relaxed), 1);
    assert_eq!(
      Pin::new(&mut merged).poll_next(&mut cx_b),
      Poll::Ready(Some(event(0, 0)))
    );
  }
}
