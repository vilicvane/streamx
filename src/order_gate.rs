use std::{
  collections::BTreeMap,
  convert::Infallible,
  pin::Pin,
  sync::{Arc, Mutex, MutexGuard},
  task::{Context, Poll},
};

use futures::{Stream, TryStream, task::AtomicWaker};

use crate::Ordered;

struct Member<TKey> {
  head: Option<TKey>,
  signal: Arc<AtomicWaker>,
}

enum TakeOutcome<TKey> {
  Blocked,
  Released { retired_key: Option<TKey> },
}

struct State<TKey> {
  frontier: Option<TKey>,
  next_member_id: usize,
  members: BTreeMap<usize, Member<TKey>>,
}

impl<TKey> State<TKey>
where
  TKey: Ord,
{
  fn register(&mut self, signal: Arc<AtomicWaker>) -> usize {
    let member_id = self.next_member_id;
    self.next_member_id = self
      .next_member_id
      .checked_add(1)
      .expect("OrderGate member id overflowed");
    let previous = self
      .members
      .insert(member_id, Member { head: None, signal });
    debug_assert!(previous.is_none());
    member_id
  }

  fn publish_head(&mut self, member_id: usize, key: TKey) {
    let member = self
      .members
      .get_mut(&member_id)
      .expect("active OrderGate member must be registered");
    debug_assert!(member.head.is_none());
    member.head = Some(key);
  }

  fn take_if_eligible(&mut self, member_id: usize) -> TakeOutcome<TKey> {
    let is_behind_frontier = self.frontier.as_ref().is_some_and(|frontier| {
      self
        .members
        .get(&member_id)
        .and_then(|member| member.head.as_ref())
        .is_some_and(|key| key < frontier)
    });

    let is_selected = if is_behind_frontier {
      true
    } else if self.members.values().any(|member| member.head.is_none()) {
      false
    } else {
      self.selected_member() == Some(member_id)
    };

    if !is_selected {
      return TakeOutcome::Blocked;
    }

    let member = self
      .members
      .get_mut(&member_id)
      .expect("eligible OrderGate member must be registered");
    let key = member
      .head
      .take()
      .expect("eligible OrderGate member must have a head");
    let retired_key = if is_behind_frontier {
      Some(key)
    } else {
      self.frontier.replace(key)
    };

    TakeOutcome::Released { retired_key }
  }

  fn selected_member(&self) -> Option<usize> {
    let mut selected = None;

    for (&member_id, member) in &self.members {
      let Some(key) = member.head.as_ref() else {
        continue;
      };

      let should_select = selected.is_none_or(|selected_id| {
        let selected_key = self.members[&selected_id]
          .head
          .as_ref()
          .expect("selected OrderGate member must have a head");
        key < selected_key
      });

      if should_select {
        selected = Some(member_id);
      }
    }

    selected
  }

  fn eligible_members(&self) -> Vec<usize> {
    if let Some(frontier) = self.frontier.as_ref() {
      let behind = self
        .members
        .iter()
        .filter_map(|(&member_id, member)| {
          member
            .head
            .as_ref()
            .is_some_and(|key| key < frontier)
            .then_some(member_id)
        })
        .collect::<Vec<_>>();

      if !behind.is_empty() {
        return behind;
      }
    }

    if self.members.values().any(|member| member.head.is_none()) {
      Vec::new()
    } else {
      self.selected_member().into_iter().collect()
    }
  }

  fn eligible_signals(&self) -> Vec<Arc<AtomicWaker>> {
    self
      .eligible_members()
      .into_iter()
      .map(|member_id| Arc::clone(&self.members[&member_id].signal))
      .collect()
  }
}

struct Shared<TKey> {
  state: Mutex<State<TKey>>,
}

impl<TKey> Shared<TKey>
where
  TKey: Ord,
{
  fn new() -> Self {
    Self {
      state: Mutex::new(State {
        frontier: None,
        next_member_id: 0,
        members: BTreeMap::new(),
      }),
    }
  }

  fn lock(&self) -> MutexGuard<'_, State<TKey>> {
    self
      .state
      .lock()
      .unwrap_or_else(std::sync::PoisonError::into_inner)
  }

  fn register(&self) -> (usize, Arc<AtomicWaker>) {
    let signal = Arc::new(AtomicWaker::new());
    let member_id = self.lock().register(Arc::clone(&signal));
    (member_id, signal)
  }

  fn publish_and_try_take(&self, member_id: usize, key: TKey) -> bool {
    let (outcome, signals) = {
      let mut state = self.lock();
      state.publish_head(member_id, key);
      let outcome = state.take_if_eligible(member_id);
      let signals = state.eligible_signals();
      (outcome, signals)
    };

    finish_transition(outcome, signals)
  }

  fn try_take(&self, member_id: usize) -> bool {
    let (outcome, signals) = {
      let mut state = self.lock();
      let outcome = state.take_if_eligible(member_id);
      let signals = state.eligible_signals();
      (outcome, signals)
    };

    finish_transition(outcome, signals)
  }

  fn remove(&self, member_id: usize) {
    let (removed, signals) = {
      let mut state = self.lock();
      let Some(removed) = state.members.remove(&member_id) else {
        return;
      };
      let signals = state.eligible_signals();
      (removed, signals)
    };

    drop(removed);
    wake_all(signals);
  }
}

fn finish_transition<TKey>(outcome: TakeOutcome<TKey>, signals: Vec<Arc<AtomicWaker>>) -> bool {
  let (released, retired_key) = match outcome {
    TakeOutcome::Blocked => (false, None),
    TakeOutcome::Released { retired_key } => (true, retired_key),
  };

  drop(retired_key);
  wake_all(signals);
  released
}

fn wake_all(signals: Vec<Arc<AtomicWaker>>) {
  for signal in signals {
    signal.wake();
  }
}

struct GateMemberCore<TStream, TItem, TKey>
where
  TKey: Ord,
{
  stream: Pin<Box<TStream>>,
  item: Option<TItem>,
  shared: Arc<Shared<TKey>>,
  member_id: usize,
  signal: Arc<AtomicWaker>,
  done: bool,
}

impl<TStream, TItem, TKey> GateMemberCore<TStream, TItem, TKey>
where
  TItem: Ordered<Key = TKey>,
  TKey: Ord,
{
  fn new(stream: TStream, shared: Arc<Shared<TKey>>) -> Self {
    let (member_id, signal) = shared.register();

    Self {
      stream: Box::pin(stream),
      item: None,
      shared,
      member_id,
      signal,
      done: false,
    }
  }

  fn poll_next<TError, TPoll>(
    &mut self,
    cx: &mut Context<'_>,
    poll_stream: TPoll,
  ) -> Poll<Option<Result<TItem, TError>>>
  where
    TPoll: FnOnce(Pin<&mut TStream>, &mut Context<'_>) -> Poll<Option<Result<TItem, TError>>>,
  {
    if self.done {
      return Poll::Ready(None);
    }

    self.signal.register(cx.waker());

    if self.item.is_some() {
      if self.shared.try_take(self.member_id) {
        drop(self.signal.take());
        return Poll::Ready(Some(Ok(
          self
            .item
            .take()
            .expect("released member must retain its item"),
        )));
      }

      return Poll::Pending;
    }

    match poll_stream(self.stream.as_mut(), cx) {
      Poll::Ready(Some(Ok(item))) => {
        let key = item.order_key();
        self.item = Some(item);

        if self.shared.publish_and_try_take(self.member_id, key) {
          drop(self.signal.take());
          Poll::Ready(Some(Ok(
            self
              .item
              .take()
              .expect("released member must retain its item"),
          )))
        } else {
          Poll::Pending
        }
      }
      Poll::Ready(Some(Err(error))) => {
        drop(self.signal.take());
        Poll::Ready(Some(Err(error)))
      }
      Poll::Ready(None) => {
        self.done = true;
        self.shared.remove(self.member_id);
        drop(self.signal.take());
        Poll::Ready(None)
      }
      Poll::Pending => Poll::Pending,
    }
  }
}

impl<TStream, TItem, TKey> Drop for GateMemberCore<TStream, TItem, TKey>
where
  TKey: Ord,
{
  fn drop(&mut self) {
    if !self.done {
      self.done = true;
      self.shared.remove(self.member_id);
      drop(self.signal.take());
    }
  }
}

/// Coordinates separately consumed ordered streams against one shared
/// monotonic frontier.
///
/// [`OrderGate::gate`] and [`OrderGate::try_gate`] each register an active
/// member immediately. Every member retains its own output type and is polled
/// independently, while successful items share these release rules:
///
/// - an item whose key is below the current frontier passes through without
///   moving the frontier;
/// - otherwise every active member must have a retained head before the
///   smallest head can pass, with equal keys resolved by registration order;
/// - after a member passes an item, it is missing a head again until that
///   wrapper is polled to another item or completion.
///
/// Members can be added dynamically. Consequently, values observed across all
/// wrappers are not necessarily globally nondecreasing: a newly registered
/// member may pass values below the frontier while it catches up. The frontier
/// itself never moves backward and remains available for later registrations
/// even if the member set temporarily becomes empty.
///
/// The gate is pull-based and lossless. All active wrappers must continue to
/// be polled; an unpolled or pending member without a head blocks frontier
/// advancement. Completing or dropping a wrapper removes it from the gate.
/// Use [`merge_ordered`](crate::merge_ordered) instead when a single homogeneous
/// output stream is wanted.
pub struct OrderGate<TKey>
where
  TKey: Ord,
{
  shared: Arc<Shared<TKey>>,
}

impl<TKey> OrderGate<TKey>
where
  TKey: Ord,
{
  /// Creates an empty order gate with no frontier.
  #[allow(clippy::new_without_default)]
  pub fn new() -> Self {
    Self {
      shared: Arc::new(Shared::new()),
    }
  }

  /// Registers an ordered stream as an active member of this gate.
  ///
  /// Registration itself does not poll `stream`, but the new member is
  /// immediately part of the missing-head barrier. Each item must belong to a
  /// nondecreasing sequence; the gate coordinates streams and does not validate
  /// their internal ordering.
  pub fn gate<TStream>(&self, stream: TStream) -> OrderGatedStream<TStream, TKey>
  where
    TStream: Stream,
    TStream::Item: Ordered<Key = TKey>,
  {
    OrderGatedStream {
      core: GateMemberCore::new(stream, Arc::clone(&self.shared)),
    }
  }

  /// Registers a fallible ordered stream as an active member of this gate.
  ///
  /// Successful items follow the same rules as [`OrderGate::gate`]. An observed
  /// error passes through its own wrapper immediately, does not move the
  /// frontier or discard any retained head, and leaves that member active and
  /// missing a successful head.
  pub fn try_gate<TStream>(&self, stream: TStream) -> TryOrderGatedStream<TStream, TKey>
  where
    TStream: TryStream,
    TStream::Ok: Ordered<Key = TKey>,
  {
    TryOrderGatedStream {
      core: GateMemberCore::new(stream, Arc::clone(&self.shared)),
    }
  }
}

/// A stream registered through [`OrderGate::gate`].
pub struct OrderGatedStream<TStream, TKey>
where
  TStream: Stream,
  TStream::Item: Ordered<Key = TKey>,
  TKey: Ord,
{
  core: GateMemberCore<TStream, TStream::Item, TKey>,
}

// The source is pinned independently in `Pin<Box<_>>`; no field of the outer
// stream relies on structural pinning.
impl<TStream, TKey> Unpin for OrderGatedStream<TStream, TKey>
where
  TStream: Stream,
  TStream::Item: Ordered<Key = TKey>,
  TKey: Ord,
{
}

impl<TStream, TKey> Stream for OrderGatedStream<TStream, TKey>
where
  TStream: Stream,
  TStream::Item: Ordered<Key = TKey>,
  TKey: Ord,
{
  type Item = TStream::Item;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let this = self.get_mut();

    match this
      .core
      .poll_next::<Infallible, _>(cx, |stream, stream_cx| {
        match Stream::poll_next(stream, stream_cx) {
          Poll::Ready(Some(item)) => Poll::Ready(Some(Ok(item))),
          Poll::Ready(None) => Poll::Ready(None),
          Poll::Pending => Poll::Pending,
        }
      }) {
      Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(item)),
      Poll::Ready(Some(Err(error))) => match error {},
      Poll::Ready(None) => Poll::Ready(None),
      Poll::Pending => Poll::Pending,
    }
  }
}

/// A fallible stream registered through [`OrderGate::try_gate`].
///
/// Successful items participate in the shared gate. Observed errors pass
/// through immediately and do not terminate membership.
pub struct TryOrderGatedStream<TStream, TKey>
where
  TStream: TryStream,
  TStream::Ok: Ordered<Key = TKey>,
  TKey: Ord,
{
  core: GateMemberCore<TStream, TStream::Ok, TKey>,
}

// The source is pinned independently in `Pin<Box<_>>`; no field of the outer
// stream relies on structural pinning.
impl<TStream, TKey> Unpin for TryOrderGatedStream<TStream, TKey>
where
  TStream: TryStream,
  TStream::Ok: Ordered<Key = TKey>,
  TKey: Ord,
{
}

impl<TStream, TKey> Stream for TryOrderGatedStream<TStream, TKey>
where
  TStream: TryStream,
  TStream::Ok: Ordered<Key = TKey>,
  TKey: Ord,
{
  type Item = Result<TStream::Ok, TStream::Error>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    self.get_mut().core.poll_next(cx, |stream, stream_cx| {
      TryStream::try_poll_next(stream, stream_cx)
    })
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

  use futures::{
    Stream, StreamExt,
    task::{ArcWake, noop_waker},
  };

  use super::{OrderGate, Ordered};

  #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
  struct Key(u32);

  #[derive(Debug, PartialEq, Eq)]
  struct Event {
    key: u32,
    id: u32,
  }

  impl Ordered for Event {
    type Key = Key;

    fn order_key(&self) -> Self::Key {
      Key(self.key)
    }
  }

  fn event(key: u32, id: u32) -> Event {
    Event { key, id }
  }

  #[derive(Debug, PartialEq, Eq)]
  struct TestError(&'static str);

  struct MpscStream<T>(tokio::sync::mpsc::UnboundedReceiver<T>);

  impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
      self.0.poll_recv(cx)
    }
  }

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

  struct WakeCounter(AtomicUsize);

  impl ArcWake for WakeCounter {
    fn wake_by_ref(arc_self: &Arc<Self>) {
      arc_self.0.fetch_add(1, Ordering::Relaxed);
    }
  }

  fn poll_next<TStream>(stream: &mut TStream, cx: &mut Context<'_>) -> Poll<Option<TStream::Item>>
  where
    TStream: Stream + Unpin,
  {
    Pin::new(stream).poll_next(cx)
  }

  #[test]
  fn coordinates_members_in_nondecreasing_order() {
    let gate = OrderGate::<Key>::new();
    let mut first = gate.gate(futures::stream::iter(vec![event(1, 10), event(3, 30)]));
    let mut second = gate.gate(futures::stream::iter(vec![event(2, 20), event(4, 40)]));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(poll_next(&mut first, &mut cx), Poll::Pending);
    assert_eq!(poll_next(&mut second, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut first, &mut cx),
      Poll::Ready(Some(event(1, 10)))
    );

    assert_eq!(poll_next(&mut first, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut second, &mut cx),
      Poll::Ready(Some(event(2, 20)))
    );

    assert_eq!(poll_next(&mut second, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut first, &mut cx),
      Poll::Ready(Some(event(3, 30)))
    );

    assert_eq!(poll_next(&mut first, &mut cx), Poll::Ready(None));
    assert_eq!(
      poll_next(&mut second, &mut cx),
      Poll::Ready(Some(event(4, 40)))
    );
    assert_eq!(poll_next(&mut second, &mut cx), Poll::Ready(None));
    assert_eq!(poll_next(&mut second, &mut cx), Poll::Ready(None));
  }

  #[test]
  fn equal_keys_use_registration_order() {
    let gate = OrderGate::<Key>::new();
    let mut first = gate.gate(futures::stream::iter(vec![event(1, 10)]));
    let mut second = gate.gate(futures::stream::iter(vec![event(1, 20)]));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(poll_next(&mut first, &mut cx), Poll::Pending);
    assert_eq!(poll_next(&mut second, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut first, &mut cx),
      Poll::Ready(Some(event(1, 10)))
    );

    assert_eq!(poll_next(&mut first, &mut cx), Poll::Ready(None));
    assert_eq!(
      poll_next(&mut second, &mut cx),
      Poll::Ready(Some(event(1, 20)))
    );
  }

  #[test]
  fn construction_is_lazy_and_each_member_retains_one_head() {
    let gate = OrderGate::<Key>::new();
    let first_polls = Arc::new(AtomicUsize::new(0));
    let second_polls = Arc::new(AtomicUsize::new(0));
    let mut first = gate.gate(CountStream {
      items: VecDeque::from([event(1, 10), event(3, 30)]),
      polls: Arc::clone(&first_polls),
    });
    let mut second = gate.gate(CountStream {
      items: VecDeque::from([event(2, 20), event(4, 40)]),
      polls: Arc::clone(&second_polls),
    });
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(first_polls.load(Ordering::Relaxed), 0);
    assert_eq!(second_polls.load(Ordering::Relaxed), 0);

    assert_eq!(poll_next(&mut first, &mut cx), Poll::Pending);
    assert_eq!(first_polls.load(Ordering::Relaxed), 1);
    assert_eq!(poll_next(&mut first, &mut cx), Poll::Pending);
    assert_eq!(first_polls.load(Ordering::Relaxed), 1);

    assert_eq!(poll_next(&mut second, &mut cx), Poll::Pending);
    assert_eq!(second_polls.load(Ordering::Relaxed), 1);
    assert_eq!(
      poll_next(&mut first, &mut cx),
      Poll::Ready(Some(event(1, 10)))
    );
    assert_eq!(first_polls.load(Ordering::Relaxed), 1);

    assert_eq!(poll_next(&mut first, &mut cx), Poll::Pending);
    assert_eq!(first_polls.load(Ordering::Relaxed), 2);
    assert_eq!(poll_next(&mut first, &mut cx), Poll::Pending);
    assert_eq!(first_polls.load(Ordering::Relaxed), 2);
  }

  #[test]
  fn late_member_catches_up_below_frontier_but_equal_key_joins_barrier() {
    let gate = OrderGate::<Key>::new();
    let mut original = gate.gate(futures::stream::iter(vec![event(10, 10), event(20, 20)]));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(
      poll_next(&mut original, &mut cx),
      Poll::Ready(Some(event(10, 10)))
    );

    let mut late = gate.gate(futures::stream::iter(vec![
      event(5, 5),
      event(8, 8),
      event(10, 100),
      event(15, 15),
    ]));

    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(event(5, 5)))
    );
    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(event(8, 8)))
    );
    assert_eq!(poll_next(&mut late, &mut cx), Poll::Pending);

    assert_eq!(poll_next(&mut original, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(event(10, 100)))
    );
    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(event(15, 15)))
    );
    assert_eq!(poll_next(&mut late, &mut cx), Poll::Ready(None));
    assert_eq!(
      poll_next(&mut original, &mut cx),
      Poll::Ready(Some(event(20, 20)))
    );
  }

  #[test]
  fn dynamically_registered_missing_member_blocks_frontier_advancement() {
    let gate = OrderGate::<Key>::new();
    let mut original = gate.gate(futures::stream::iter(vec![event(10, 10), event(20, 20)]));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(
      poll_next(&mut original, &mut cx),
      Poll::Ready(Some(event(10, 10)))
    );

    let (_late_tx, late_rx) = tokio::sync::mpsc::unbounded_channel();
    let late = gate.gate(MpscStream::<Event>(late_rx));

    assert_eq!(poll_next(&mut original, &mut cx), Poll::Pending);
    drop(late);
    assert_eq!(
      poll_next(&mut original, &mut cx),
      Poll::Ready(Some(event(20, 20)))
    );
  }

  #[test]
  fn frontier_survives_when_every_member_has_left() {
    let gate = OrderGate::<Key>::new();
    let mut original = gate.gate(futures::stream::iter(vec![event(10, 10)]));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(
      poll_next(&mut original, &mut cx),
      Poll::Ready(Some(event(10, 10)))
    );
    assert_eq!(poll_next(&mut original, &mut cx), Poll::Ready(None));

    let mut late = gate.gate(futures::stream::iter(vec![event(5, 5)]));
    let (_pending_tx, pending_rx) = tokio::sync::mpsc::unbounded_channel();
    let _pending = gate.gate(MpscStream::<Event>(pending_rx));

    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(event(5, 5)))
    );
  }

  #[test]
  fn completion_and_drop_remove_missing_members_and_wake_candidate() {
    {
      let gate = OrderGate::<Key>::new();
      let mut candidate = gate.gate(futures::stream::iter(vec![event(1, 10)]));
      let mut completed = gate.gate(futures::stream::empty::<Event>());
      let wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
      let waker = futures::task::waker(Arc::clone(&wakes));
      let mut cx = Context::from_waker(&waker);

      assert_eq!(poll_next(&mut candidate, &mut cx), Poll::Pending);
      assert_eq!(poll_next(&mut completed, &mut cx), Poll::Ready(None));
      assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
      assert_eq!(
        poll_next(&mut candidate, &mut cx),
        Poll::Ready(Some(event(1, 10)))
      );
    }

    {
      let gate = OrderGate::<Key>::new();
      let mut candidate = gate.gate(futures::stream::iter(vec![event(1, 10)]));
      let (_pending_tx, pending_rx) = tokio::sync::mpsc::unbounded_channel();
      let pending = gate.gate(MpscStream::<Event>(pending_rx));
      let wakes = Arc::new(WakeCounter(AtomicUsize::new(0)));
      let waker = futures::task::waker(Arc::clone(&wakes));
      let mut cx = Context::from_waker(&waker);

      assert_eq!(poll_next(&mut candidate, &mut cx), Poll::Pending);
      drop(pending);
      assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
      assert_eq!(
        poll_next(&mut candidate, &mut cx),
        Poll::Ready(Some(event(1, 10)))
      );
    }
  }

  #[test]
  fn pending_member_uses_latest_downstream_waker() {
    let gate = OrderGate::<Key>::new();
    let mut first = gate.gate(futures::stream::iter(vec![event(1, 10)]));
    let mut second = gate.gate(futures::stream::iter(vec![event(2, 20)]));
    let wakes_a = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let wakes_b = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker_a = futures::task::waker(Arc::clone(&wakes_a));
    let waker_b = futures::task::waker(Arc::clone(&wakes_b));
    let mut cx_a = Context::from_waker(&waker_a);
    let mut cx_b = Context::from_waker(&waker_b);

    assert_eq!(poll_next(&mut first, &mut cx_a), Poll::Pending);
    assert_eq!(poll_next(&mut first, &mut cx_b), Poll::Pending);
    assert_eq!(poll_next(&mut second, &mut cx_a), Poll::Pending);

    assert_eq!(wakes_a.0.load(Ordering::Relaxed), 0);
    assert_eq!(wakes_b.0.load(Ordering::Relaxed), 1);
    assert_eq!(
      poll_next(&mut first, &mut cx_b),
      Poll::Ready(Some(event(1, 10)))
    );
  }

  #[test]
  fn try_error_passes_immediately_without_releasing_retained_heads() {
    let gate = OrderGate::<Key>::new();
    let mut ordinary = gate.gate(futures::stream::iter(vec![event(2, 20)]));
    let mut fallible = gate.try_gate(futures::stream::iter(vec![
      Err(TestError("gap")),
      Ok(event(1, 10)),
    ]));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(poll_next(&mut ordinary, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut fallible, &mut cx),
      Poll::Ready(Some(Err(TestError("gap"))))
    );
    assert_eq!(poll_next(&mut ordinary, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut fallible, &mut cx),
      Poll::Ready(Some(Ok(event(1, 10))))
    );
    assert_eq!(poll_next(&mut fallible, &mut cx), Poll::Ready(None));
    assert_eq!(
      poll_next(&mut ordinary, &mut cx),
      Poll::Ready(Some(event(2, 20)))
    );
  }

  #[test]
  fn try_error_and_success_below_frontier_both_pass_immediately() {
    let gate = OrderGate::<Key>::new();
    let mut original = gate.gate(futures::stream::iter(vec![event(10, 10), event(20, 20)]));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(
      poll_next(&mut original, &mut cx),
      Poll::Ready(Some(event(10, 10)))
    );

    let mut late = gate.try_gate(futures::stream::iter(vec![
      Err(TestError("gap")),
      Ok(event(5, 5)),
      Err(TestError("another gap")),
      Ok(event(8, 8)),
      Ok(event(10, 100)),
    ]));

    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(Err(TestError("gap"))))
    );
    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(Ok(event(5, 5))))
    );
    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(Err(TestError("another gap"))))
    );
    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(Ok(event(8, 8))))
    );
    assert_eq!(poll_next(&mut late, &mut cx), Poll::Pending);
    assert_eq!(poll_next(&mut original, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut late, &mut cx),
      Poll::Ready(Some(Ok(event(10, 100))))
    );
  }

  #[tokio::test]
  async fn accepts_non_unpin_sources_and_non_clone_keys() {
    let gate = OrderGate::<Key>::new();
    let source = futures::stream::once(async { event(1, 10) });
    let values = gate.gate(source).collect::<Vec<_>>().await;

    assert_eq!(values, vec![event(1, 10)]);
  }

  #[test]
  fn accepts_borrowing_non_send_streams_items_keys_and_errors() {
    use std::{cmp::Ordering as CmpOrdering, rc::Rc};

    struct LocalKey<'a> {
      value: u32,
      _local: &'a Rc<()>,
    }

    impl PartialEq for LocalKey<'_> {
      fn eq(&self, other: &Self) -> bool {
        self.value == other.value
      }
    }

    impl Eq for LocalKey<'_> {}

    impl PartialOrd for LocalKey<'_> {
      fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
      }
    }

    impl Ord for LocalKey<'_> {
      fn cmp(&self, other: &Self) -> CmpOrdering {
        self.value.cmp(&other.value)
      }
    }

    struct LocalEvent<'a> {
      value: u32,
      local: &'a Rc<()>,
    }

    impl<'a> Ordered for LocalEvent<'a> {
      type Key = LocalKey<'a>;

      fn order_key(&self) -> Self::Key {
        LocalKey {
          value: self.value,
          _local: self.local,
        }
      }
    }

    struct LocalError<'a> {
      _local: &'a Rc<()>,
    }

    let local = Rc::new(());
    let gate = OrderGate::<LocalKey<'_>>::new();
    let source = futures::stream::iter(vec![LocalEvent {
      value: 1,
      local: &local,
    }]);
    let value = futures::executor::block_on(gate.gate(source).next()).unwrap();

    assert_eq!(value.value, 1);

    let try_gate = OrderGate::<LocalKey<'_>>::new();
    let source = futures::stream::iter(vec![
      Err(LocalError { _local: &local }),
      Ok(LocalEvent {
        value: 2,
        local: &local,
      }),
    ]);
    let mut gated = try_gate.try_gate(source);

    assert!(matches!(
      futures::executor::block_on(gated.next()),
      Some(Err(_))
    ));
    let Some(Ok(value)) = futures::executor::block_on(gated.next()) else {
      panic!("stream must produce its successful item");
    };
    assert_eq!(value.value, 2);
  }

  #[test]
  fn heterogeneous_items_share_the_same_gate() {
    #[derive(Debug, PartialEq, Eq)]
    struct Trade(u32);

    impl Ordered for Trade {
      type Key = Key;

      fn order_key(&self) -> Self::Key {
        Key(self.0)
      }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct BookUpdate(u32);

    impl Ordered for BookUpdate {
      type Key = Key;

      fn order_key(&self) -> Self::Key {
        Key(self.0)
      }
    }

    let gate = OrderGate::<Key>::new();
    let mut trades = gate.gate(futures::stream::iter(vec![Trade(2)]));
    let mut books = gate.gate(futures::stream::iter(vec![BookUpdate(1)]));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(poll_next(&mut trades, &mut cx), Poll::Pending);
    assert_eq!(
      poll_next(&mut books, &mut cx),
      Poll::Ready(Some(BookUpdate(1)))
    );
    assert_eq!(poll_next(&mut books, &mut cx), Poll::Ready(None));
    assert_eq!(poll_next(&mut trades, &mut cx), Poll::Ready(Some(Trade(2))));
  }

  #[test]
  fn extracts_each_key_once_while_a_head_is_retained() {
    struct CountedEvent {
      key: u32,
      extractions: Arc<AtomicUsize>,
    }

    impl Ordered for CountedEvent {
      type Key = Key;

      fn order_key(&self) -> Self::Key {
        self.extractions.fetch_add(1, Ordering::Relaxed);
        Key(self.key)
      }
    }

    let gate = OrderGate::<Key>::new();
    let extractions = Arc::new(AtomicUsize::new(0));
    let mut counted = gate.gate(futures::stream::iter(vec![CountedEvent {
      key: 1,
      extractions: Arc::clone(&extractions),
    }]));
    let (_pending_tx, pending_rx) = tokio::sync::mpsc::unbounded_channel();
    let pending = gate.gate(MpscStream::<CountedEvent>(pending_rx));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(poll_next(&mut counted, &mut cx), Poll::Pending));
    assert!(matches!(poll_next(&mut counted, &mut cx), Poll::Pending));
    assert_eq!(extractions.load(Ordering::Relaxed), 1);

    drop(pending);
    assert!(matches!(
      poll_next(&mut counted, &mut cx),
      Poll::Ready(Some(_))
    ));
    assert_eq!(extractions.load(Ordering::Relaxed), 1);
  }
}
