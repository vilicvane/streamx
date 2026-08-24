# StreamX

[![CI](https://github.com/vilicvane/streamx/actions/workflows/ci.yml/badge.svg)](https://github.com/vilicvane/streamx/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/streamx)](https://crates.io/crates/streamx)
[![docs.rs](https://img.shields.io/docsrs/streamx)](https://docs.rs/streamx)
[![license](https://img.shields.io/crates/l/streamx)](https://github.com/vilicvane/streamx/blob/master/LICENSE)

StreamX extends [`futures::Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html)
with operators whose buffering, back-pressure, and lifetime behavior is
explicit.

The library is designed for real-time state and event streams. Some operators
are pull-based and preserve every event; others actively consume upstream and
intentionally conflate or drop intermediate values.

## Installation

```toml
[dependencies]
futures = "0.3"
streamx = "0.1"
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

Operators that start background tasks must be constructed inside a Tokio
runtime. Their upstream streams and items require `Send + 'static`.

## Choosing an operator

| Need | Operator | Delivery behavior |
| --- | --- | --- |
| Merge homogeneous event streams | `merge_all` | Pull-based and lossless |
| Merge streams already sorted by a canonical item key | `merge_ordered` | Pull-based and lossless; one head per unfinished input |
| Suppress consecutive duplicates | `distinct_until_changed` | Pull-based; distinct transitions remain lossless |
| Observe only the newest state | `latest` | Hot; one unconsumed value is retained |
| Combine the current state of several streams | `combine_latest!`, `combine_latest_all` | Hot; one unconsumed combined snapshot is retained |
| Emit after a quiet period | `debounce` | Hot; completed results use a bounded drop-oldest queue |
| Emit the leading value of each time window | `throttle` | Hot; completed results use a bounded drop-oldest queue |
| Sample state when a primary event arrives | `with_latest_from` | Pull-based primary, hot/conflating secondary |
| Multicast every event | `share` | Lossless; a slow strong subscriber backpressures upstream |
| Multicast without slow-subscriber back pressure | `share_overflow`, `share_latest` | Full live queues drop their oldest values |
| Multicast with history for new subscribers | `share_replay` | Lossless live delivery plus replay history |
| Multicast replayable state | `share_replay_overflow`, `share_replay_latest` | Replay history plus drop-oldest live delivery |

`hot` here means that upstream progress does not depend on downstream
`poll_next` calls. It does not imply multicast; `latest`, for example, still
has one consumer.

## Common patterns

### Keep only current state

```rust
use futures::{StreamExt, stream};
use streamx::StreamLatestExt;

#[tokio::main(flavor = "current_thread")]
async fn main() {
  let mut current = stream::iter([1, 2, 3]).latest();

  // The upstream is consumed independently, so intermediate state is conflated.
  assert_eq!(current.next().await, Some(3));
  assert_eq!(current.next().await, None);
}
```

### Combine current state

```rust
use futures::{StreamExt, stream};
use streamx::combine_latest;

#[tokio::main(flavor = "current_thread")]
async fn main() {
  let temperatures = stream::iter([20, 21]);
  let humidity = stream::iter([40, 41]);
  let mut conditions = combine_latest!(temperatures, humidity);

  assert_eq!(conditions.next().await, Some((21, 41)));
}
```

`combine_latest` is a state-combination operator. If several inputs update
before the output is observed, only the newest combined snapshot remains. It
does not preserve one output for every input event.

### Merge canonical event order

```rust
use futures::{StreamExt, stream};
use streamx::{Ordered, merge_ordered};

#[derive(Debug, PartialEq, Eq)]
struct Event {
  time: u64,
}

impl Ordered for Event {
  type Key = u64;

  fn order_key(&self) -> Self::Key {
    self.time
  }
}

# async fn example() {
let streams = [
  stream::iter([Event { time: 1 }, Event { time: 3 }]),
  stream::iter([Event { time: 2 }, Event { time: 4 }]),
];

let times = merge_ordered(streams)
  .map(|event| event.time)
  .collect::<Vec<_>>()
  .await;

assert_eq!(times, vec![1, 2, 3, 4]);
# }
```

Every input must already be ordered by a nondecreasing `Ordered::Key`.
`merge_ordered` retains one head from each unfinished input and cannot emit
until every such input has produced a head or completed. Equal keys are emitted
in input order. The operator does not poll upstream before downstream demand.

### Bound time-based output

```rust
use std::time::Duration;

use futures::Stream;
use streamx::{DebounceStream, StreamDebounceExt};

fn debounce_updates<S>(updates: S) -> DebounceStream<S>
where
  S: Stream + Send + 'static,
  S::Item: Send + 'static,
{
  updates.debounce(Duration::from_millis(100), 4)
}
```

The `capacity` argument bounds completed results that downstream has not yet
consumed. It does not slow the hot upstream task. When full, the oldest
completed result is dropped. `capacity` must be greater than zero.

`throttle(scheduler, capacity)` uses the same output-queue rule. The scheduler
may be a `Duration` or a `Send + 'static` function that creates a `Send`
future, allowing fixed intervals as well as application-defined timing.

### Share replayable state

```rust
use futures::stream;
use streamx::StreamShareReplayExt;

#[tokio::main(flavor = "current_thread")]
async fn main() {
  let shared = stream::pending::<u32>().share_replay_latest();
  let subscriber = shared.clone();
  let weak_cache_entry = shared.downgrade();

  drop(subscriber);
  drop(shared);
  assert!(weak_cache_entry.upgrade().is_none());
}
```

Every strong sharing handle is an active subscriber as soon as it is created,
including a clone or successful weak upgrade. First polling is not a
subscription event.

This matters for lossless `share` and `share_replay`: retaining an unpolled
strong handle can fill its live queue and backpressure the shared upstream.
Use a weak handle for caches that should not keep a subscription alive. Use an
overflow/latest variant when intermediate live values may be dropped.

## Capacity and replay

- `capacity` always refers to live or completed, unconsumed output and must be
  greater than zero.
- `buffer_size` on replay operators is separate replay history for a newly
  created subscriber and may be zero.
- `share_latest()` is `share_overflow(1)` and does not replay earlier live
  history to a new subscription.
- `share_replay_latest()` is `share_replay_overflow(1, 1)` and does replay the
  current value to a new subscriber.
- `latest` and both combine-latest operators have an implicit output capacity
  of one.

## Lifetime and completion

- Dropping a hot single-consumer operator aborts its background task and
  releases its upstream.
- Dropping the last strong sharing handle stops the shared upstream. Weak
  handles do not keep it alive.
- A weak handle cannot resurrect a stopped or completed shared stream.
- A temporarily silent source remains pending; only upstream completion
  (`Poll::Ready(None)`) completes an operator.

Each operator has more specific completion behavior. For example, `debounce`
flushes its pending value when upstream completes, while `throttle` has no
trailing flush. See the
[operator semantics](https://github.com/vilicvane/streamx/blob/master/OPERATOR_SEMANTICS.md)
for the complete contract.

## API index

Creation and collection operators:

- [`combine_latest!`](https://docs.rs/streamx/latest/streamx/macro.combine_latest.html)
- [`combine_latest_all`](https://docs.rs/streamx/latest/streamx/fn.combine_latest_all.html)
- [`merge_all`](https://docs.rs/streamx/latest/streamx/fn.merge_all.html)
- [`merge_ordered`](https://docs.rs/streamx/latest/streamx/fn.merge_ordered.html)

Stream extension operators:

- [`debounce`](https://docs.rs/streamx/latest/streamx/trait.StreamDebounceExt.html#method.debounce)
- [`distinct_until_changed`](https://docs.rs/streamx/latest/streamx/trait.StreamDistinctUntilChangedExt.html#method.distinct_until_changed)
- [`latest`](https://docs.rs/streamx/latest/streamx/trait.StreamLatestExt.html#method.latest)
- [`share`](https://docs.rs/streamx/latest/streamx/trait.StreamShareExt.html#method.share)
- [`share_replay`](https://docs.rs/streamx/latest/streamx/trait.StreamShareReplayExt.html#method.share_replay)
- [`throttle`](https://docs.rs/streamx/latest/streamx/trait.StreamThrottleExt.html#method.throttle)
- [`with_latest_from`](https://docs.rs/streamx/latest/streamx/trait.StreamWithLatestFromExt.html#method.with_latest_from)

## Maintainers

[OPERATOR_SEMANTICS.md](https://github.com/vilicvane/streamx/blob/master/OPERATOR_SEMANTICS.md)
is the normative behavior and maintainer guide. Changes to buffering, polling,
completion, or lifetime must update that document and add focused tests before
changing implementation.

## License

MIT License.
