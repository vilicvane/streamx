# StreamX Operator Semantics and Maintainer Guide

This document is the normative behavioral contract for StreamX operators. It
is written for maintainers and reviewers; the [README](README.md) is the user
entry point.

An implementation may change freely as long as the public API and every
observable behavior described here remain intact. A deliberate semantic change
must update this document, public Rust documentation, and focused tests in the
same change.

## Vocabulary

### Pull-based

An upstream advances only in response to downstream `poll_next`. Downstream
demand therefore provides back pressure.

### Hot

An upstream advances independently of downstream `poll_next` while the
returned operator is alive. In StreamX this is implemented with a Tokio
background task. `Hot` does not imply multicast.

### Lossless

Every relevant upstream event remains observable downstream. Lossless
operators must preserve back pressure rather than silently discard events.

### Conflating or drop-oldest

A conflating operator replaces stale state with newer state. A drop-oldest
queue keeps a bounded number of completed outputs and removes its oldest item
when full. In both cases downstream demand cannot preserve every upstream
event.

### Live capacity and replay history

Live capacity bounds values waiting for existing subscribers. Replay history
is a separate snapshot offered when a replay subscriber is created. They are
independent limits.

## Global invariants

### Back pressure follows semantic relevance

When every event matters, downstream demand must control upstream progress.
When an operator intentionally conflates or drops intermediate events,
delaying upstream polling only preserves stale work that will later be
discarded; that upstream must be driven actively.

### Runtime and ownership

Operators with a background task must be constructed inside a Tokio runtime.
Streams and items moved into those tasks require `Send + 'static`.

Keeping a hot operator or strong sharing handle alive expresses continued
interest in future values. Dropping it releases that interest:

- dropping a hot single-consumer operator aborts its driver;
- dropping a sharing handle unregisters that subscriber;
- dropping the last strong sharing handle stops and releases upstream;
- a weak handle neither keeps upstream alive nor restarts a stopped source.

Normal completion, abort, and task unwind must close downstream output rather
than leave receivers permanently pending.

### Fairness

No `poll_next` implementation or background task may consume an unbounded
sequence of synchronously ready items without yielding. Filtering,
conflating, and broadcasting loops must use a work budget or an equivalent
cooperative mechanism.

### Source silence is not completion

`Poll::Pending` means the source is still alive. Operators must not treat a
temporarily silent source, including a reconnecting WebSocket source, as
completed. Only `Poll::Ready(None)` is source completion.

## Semantic matrix

| Operator | Upstream progress | Retained output | Back pressure |
| --- | --- | --- | --- |
| `merge_all` | Downstream-driven | None beyond input streams | Lossless |
| `merge_ordered` | Downstream-driven | One head per unfinished input | Lossless; any missing head blocks selection |
| `try_merge_ordered` | Downstream-driven | One `Ok` head per unfinished input | Lossless; missing `Ok` heads block `Ok` selection, not observed errors |
| `distinct_until_changed(_by)` | Downstream-driven | Previous comparison item | Preserved for distinct transitions |
| `latest` | Background task | One latest item | None |
| `combine_latest!`, `combine_latest_all` | Background task | One latest combined snapshot | None |
| `debounce` | Background task and scheduler | One pending item plus `capacity` completed items | None; completed queue drops oldest |
| `throttle` | Background task and scheduler | `capacity` completed items | None; completed queue drops oldest |
| `with_latest_from` | Primary: downstream; secondary: background | One latest secondary item | Primary only |
| `share` | Background task until live capacity fills | `capacity` live items | Slowest strong subscriber |
| `share_overflow`, `share_latest` | Background task | `capacity` live items | None; live queue drops oldest |
| `share_replay` | Background task until live capacity fills | `buffer_size` replay plus `capacity` live | Slowest strong subscriber |
| `share_replay_overflow`, `share_replay_latest` | Background task | `buffer_size` replay plus `capacity` live | None; live queue drops oldest |

## Operator contracts

### `latest`

- Construction starts the upstream driver.
- At most one unconsumed item is retained.
- A new item replaces the retained item.
- Downstream polling observes the newest available item and never controls
  upstream progress.
- On source completion, the final retained item is emitted before completion.
- Dropping `LatestStream` aborts the driver and releases upstream.
- The source and item require `Send + 'static`.

### `combine_latest!` and `combine_latest_all`

- Construction starts every input driver.
- One latest value is retained for each input.
- No output exists until every input has produced at least one value.
- Each input update creates a new combined snapshot once all latest values
  exist.
- The output retains only one unconsumed snapshot; several input updates may
  therefore produce one downstream observation.
- If an input completes before its first value, the result completes
  immediately because a full snapshot is impossible.
- If an input completes after producing a value, its last value remains
  available. The result completes after every input completes.
- An empty `combine_latest_all` collection completes immediately.
- Dropping the result releases every input.
- Every input and item requires `Send + 'static`; items require `Clone` to
  construct snapshots.

These are conflating state-combination semantics, not event-for-event
ReactiveX `combineLatest` semantics.

### `debounce`

- Construction starts the source and scheduler driver.
- Each source item replaces the pending item and starts a new deadline.
- Source arrival order and scheduler readiness determine survivors; downstream
  polling never changes the debounce window.
- If source and deadline are ready in the same scheduling turn, source wins
  and restarts the deadline.
- The pending item is separate from the completed output queue.
- `capacity > 0` bounds completed, unconsumed output.
- A full completed queue drops its oldest value; it never delays source.
- Source completion immediately flushes the pending item, then the completed
  queue drains in order before completion.
- Dropping the result releases source and pending deadline.
- Items do not require `Clone`.

Public shape:

```rust
fn debounce<TScheduler>(
  self,
  scheduler: TScheduler,
  capacity: usize,
) -> DebounceStream<Self>
where
  Self: Send + 'static,
  Self::Item: Send + 'static,
  TScheduler: Into<Scheduler>;
```

### `throttle`

- Throttle is leading-edge only.
- The first item in a ready state enters the completed output queue and starts
  a window.
- Items received during the window are discarded; no trailing item is kept.
- Source timing and scheduler readiness determine the window; downstream
  polling does not.
- If source and deadline are ready together, the source item is discarded as
  part of the old window and that window becomes ready afterward. A ready
  source must not starve an already-ready deadline indefinitely.
- `capacity > 0` bounds completed, unconsumed output.
- A full completed queue drops its oldest value; it never delays source.
- Source completion does not synthesize or flush another value. Completed
  output drains before completion.
- Dropping the result releases source and active deadline.

Public shape:

```rust
fn throttle<TScheduler>(
  self,
  scheduler: TScheduler,
  capacity: usize,
) -> ThrottleStream<Self>
where
  Self: Send + 'static,
  Self::Item: Send + 'static,
  TScheduler: Into<Scheduler>;
```

### `with_latest_from`

- The primary is pull-based and is the only output trigger.
- The secondary is hot and conflating; construction starts its driver.
- A primary item produces `(primary, latest_secondary)` only after secondary
  has produced a value.
- Primary items observed before secondary state exists are discarded. The
  operator continues looking for a primary item that can produce output.
- Secondary updates never trigger output.
- Secondary completion retains its last value. If it completes empty, output
  remains impossible, but primary completion still controls result completion.
- Primary completion releases secondary immediately.
- No output capacity is needed because downstream demand controls primary.
- A startup handshake must prevent a synchronously ready primary from
  completing before the newly spawned secondary driver has performed its
  initial subscription poll.
- Draining a synchronously ready primary while no secondary state exists must
  obey the fairness budget.

Public shape:

```rust
fn with_latest_from<TFrom>(
  self,
  from: TFrom,
) -> WithLatestFromStream<Self, TFrom>
where
  TFrom: Stream + Send + 'static,
  TFrom::Item: Clone + Send + 'static;
```

### `merge_all`

- Merge is pull-based and lossless.
- Every item from every input remains observable.
- Inputs are polled fairly and ready items are emitted individually.
- The result completes after every input completes.
- An empty collection completes immediately.
- The `futures::stream::SelectAll` return type and `Unpin` input requirement
  are part of the public API.

### `merge_ordered`

- `Ordered` defines an item's canonical `Key: Ord` through `order_key()`.
- Every input must already be ordered by a nondecreasing key. The operator
  merges those sequences; it does not sort or validate an input sequence.
- Merge is pull-based and lossless. Construction does not poll any input.
- At most one head item and its extracted key are retained from each unfinished
  input. A key is extracted exactly once for each item.
- No output can be selected until every unfinished input has a head item or has
  completed. A pending input without a head therefore keeps the result pending.
- The smallest head is emitted. Equal-key heads are resolved by input iteration
  order, and order within each input is preserved.
- After emitting a head, only that input needs replenishment before the next
  selection. Downstream demand controls all replenishment.
- A completed input no longer participates in selection. The result completes
  after every input completes and all retained heads are emitted. An empty
  collection completes immediately.
- Dropping the result releases every input and retained head.
- Inputs, items, and keys require no `Unpin`, `Send`, `'static`, or `Clone`
  bound.

Public shape:

```rust
trait Ordered {
  type Key: Ord;

  fn order_key(&self) -> Self::Key;
}

fn merge_ordered<TStreams, TStream>(
  streams: TStreams,
) -> MergeOrderedStream<TStream>
where
  TStreams: IntoIterator<Item = TStream>,
  TStream: Stream,
  TStream::Item: Ordered;
```

#### `try_merge_ordered`

- Each input is a `TryStream`. Its `Ok` subsequence must already be ordered by
  a nondecreasing `Ordered::Key`; errors do not participate in that ordering.
- Removing every error from the result yields exactly the sequence produced by
  applying `merge_ordered` to the `Ok` subsequence of every input. Every
  observed error is also emitted exactly once.
- All `merge_ordered` selection rules apply to successful values: one retained
  head and extracted key per unfinished input, smallest-key selection,
  input-order ties, and per-input order preservation.
- An error observed while replenishing a missing input is emitted immediately
  without waiting for every unfinished input to have an `Ok` head. Errors are
  not globally ordered relative to successful values or errors from other
  inputs.
- Emitting an error neither discards retained successful heads nor completes
  the operator. Its input remains unfinished and missing a successful head,
  and is polled again on later downstream demand.
- The merge is pull-based and lossless for both successes and errors.
  Construction does not poll any input.
- The result completes after every input completes and all retained successful
  heads drain. An empty collection completes immediately.
- Inputs, successful items, errors, and keys require no `Unpin`, `Send`,
  `'static`, or `Clone` bound.

Public shape:

```rust
fn try_merge_ordered<TStreams, TStream>(
  streams: TStreams,
) -> TryMergeOrderedStream<TStream>
where
  TStreams: IntoIterator<Item = TStream>,
  TStream: TryStream,
  TStream::Ok: Ordered;
```

### `distinct_until_changed` and `distinct_until_changed_by`

- Both operators remain pull-based.
- The first item is always emitted.
- Consecutive items considered equal are discarded.
- Every distinct transition is emitted, so downstream demand still provides
  meaningful back pressure.
- Source completion completes the result.
- `Clone` is required because the current item is both retained for comparison
  and returned by value.
- An unbounded synchronously ready run of equal items must yield after a finite
  work budget.

### `share`, `share_overflow`, and `share_latest`

- Construction starts one upstream background task.
- Upstream is polled once. Each accepted live item is multicast to every strong
  subscriber registered when its broadcast is accepted.
- A new subscriber does not receive items already enqueued before registration.
  It may receive an upstream item that was already in flight but had not yet
  entered the live broadcast channel.
- The initial handle, a clone, and a successful weak upgrade are active
  subscriptions immediately. First polling has no subscription semantics.
- Creating a new subscriber does not replay earlier values.
- `capacity > 0` bounds live delivery.
- `share` waits when the live queue is full. The slowest retained strong
  subscriber therefore backpressures upstream.
- `share_overflow` removes the oldest live value when full. Lagging subscribers
  silently skip values removed before they observe them.
- `share_latest()` is exactly `share_overflow(1)`.
- Dropping a strong handle unregisters it. Dropping the last strong handle
  aborts and releases upstream.
- Weak handles do not own upstream. Upgrade fails after last-strong drop or
  source completion.
- Source completion closes live delivery after already queued values drain.

### `share_replay`, `share_replay_overflow`, and `share_replay_latest`

All non-replay sharing rules also apply, with these additions:

- `buffer_size` independently bounds global replay history and may be zero.
- A new strong subscriber captures replay history at subscriber creation, not
  at first poll.
- Values produced after subscriber creation belong to live delivery.
- The replay/live boundary must neither duplicate nor lose a value.
- `share_replay` uses lossless live delivery and slow-subscriber back pressure.
- `share_replay_overflow` uses drop-oldest live delivery. Overflow never
  changes the independently maintained replay history.
- `share_replay_latest()` is exactly `share_replay_overflow(1, 1)`.

## Implementation invariants

### Ordered merge

- Input streams are pinned internally; public callers do not need to provide
  `Unpin` streams.
- Missing heads are polled cooperatively with the shared work budget. Partially
  completed scans wake themselves to continue. A full scan that still has a
  missing head starts another scan if any input woke during that scan;
  otherwise the registered input wakers control the next attempt.
- Input wakers forward through shared state that is updated with the current
  downstream waker on every poll; a budgeted scan must not retain an obsolete
  downstream waker across poll calls.
- Head selection compares cached keys and must not call `order_key()` again.
- The lower input index wins an equal-key comparison.
- `merge_ordered` and `try_merge_ordered` share the same ordered-selection,
  work-budget, generation, and downstream-waker state machine.
- `try_merge_ordered` must never call `order_key()` for an error. Returning an
  error leaves every cached successful head intact and leaves its source
  unfinished and missing a head.
- After an error, the current scan round is abandoned while its advanced cursor
  is retained. The next downstream demand starts a complete scan, preventing a
  synchronously ready error from leaving the merge pending without a wake or
  monopolizing the cursor.

### Hot single-consumer channel

`src/hot.rs` is the common driver/output mechanism for hot single-consumer
operators.

- Output is a positive-capacity `VecDeque` protected by shared state.
- Sending into a full queue removes the oldest output.
- Driver start is observable through a startup marker used by hybrid
  operators.
- Sender drop marks output complete and wakes the receiver.
- Returned-stream drop aborts the Tokio task.
- Waker registration must not race an enqueue between the empty check and
  registration.

### Sharing lifecycle

- The strong subscription count, not `Arc::strong_count`, is the source of
  truth for upstream ownership.
- A forwarding task holding `Arc<Inner>` is not itself a subscriber.
- Last-strong drop must close and abort the forwarding task even while that
  task is pending on source.
- A task-scope close guard must close delivery on normal completion, abort, or
  unwind so receivers cannot remain permanently pending.
- A weak upgrade must reserve a strong subscription atomically and must not
  resurrect a count that has reached zero.

### Replay/live handoff

Replay production and subscription snapshotting use one synchronization
boundary:

1. Production allocates a sequence number and updates replay history before
   live broadcast.
2. Subscription creation registers an active live receiver and captures both
   replay history and the next sequence number while holding the same replay
   lock.
3. Captured replay items are emitted first.
4. Live items with a sequence below the captured boundary are skipped; items
   at or above it are live delivery.

This ordering covers the race where an item is recorded before subscription
but broadcast after subscription.

No synchronous mutex guard may be held across `.await`.

## Required test coverage

Every semantic change should preserve or add focused tests for the affected
dimensions:

- upstream progresses before first downstream poll for hot operators;
- pull-based primary streams do not progress without downstream demand;
- output conflation and drop-oldest capacity;
- positive-capacity validation;
- completion with and without pending output;
- source-first scheduler ties for debounce and throttle;
- fairness with an infinitely or synchronously ready source;
- upstream release on operator drop and last-strong drop;
- immediate strong subscription before first poll;
- weak non-ownership and refusal to upgrade completed/stopped streams;
- replay captured at subscriber creation;
- replay/live handoff without duplication or loss;
- restart of `with_latest_from(...).latest()` using replayed secondary state
  without requiring a new secondary update.
- ordered merge global ordering, equal-key input order, and per-input order;
- ordered merge pending-head blocking, one-head retention, pull-based startup,
  empty inputs, completion, non-`Unpin` inputs, and cooperative large-input
  polling, including a wake from an earlier chunk and downstream waker
  replacement across chunks of a budgeted scan.
- all ordered-merge dimensions apply to the `Ok` path of
  `try_merge_ordered`; additionally cover observed errors bypassing a missing-
  head barrier, retained-head preservation, continuation after errors,
  synchronous-error fairness, and an error interrupting a budgeted scan.

## Review checklist

Before adding or changing an operator, answer all of the following:

1. Which upstream events remain semantically relevant?
2. Does downstream demand provide meaningful back pressure?
3. Is each upstream pull-driven or background-driven?
4. What is retained when downstream is slow, and what is the exact bound?
5. What starts and stops upstream lifetime?
6. What happens when each input completes before and after its first value?
7. Can a synchronously ready source monopolize an executor turn?
8. Do public bounds accurately reflect task ownership and cloning needs?
9. Which tests demonstrate these behaviors independently of timing luck?
