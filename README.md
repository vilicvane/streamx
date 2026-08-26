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

## Operators

| Need | Operator | Delivery behavior |
| --- | --- | --- |
| Merge homogeneous event streams | [`merge_all`](https://docs.rs/streamx/latest/streamx/fn.merge_all.html) | Pull-based and lossless |
| Merge streams already sorted by a canonical item key | [`merge_ordered`](https://docs.rs/streamx/latest/streamx/fn.merge_ordered.html) | Pull-based and lossless; one head per unfinished input |
| ↳ Fallible inputs | [`try_merge_ordered`](https://docs.rs/streamx/latest/streamx/fn.try_merge_ordered.html) | Pull-based and lossless; observed errors pass through immediately |
| Suppress consecutive duplicates | [`distinct_until_changed`](https://docs.rs/streamx/latest/streamx/trait.StreamDistinctUntilChangedExt.html#method.distinct_until_changed), [`distinct_until_changed_by`](https://docs.rs/streamx/latest/streamx/trait.StreamDistinctUntilChangedExt.html#method.distinct_until_changed_by) | Pull-based; distinct transitions remain lossless |
| Observe only the newest state | [`latest`](https://docs.rs/streamx/latest/streamx/trait.StreamLatestExt.html#method.latest) | Hot; one unconsumed value is retained |
| Combine the current state of several streams | [`combine_latest!`](https://docs.rs/streamx/latest/streamx/macro.combine_latest.html), [`combine_latest_all`](https://docs.rs/streamx/latest/streamx/fn.combine_latest_all.html) | Hot; one unconsumed combined snapshot is retained |
| Emit after a quiet period | [`debounce`](https://docs.rs/streamx/latest/streamx/trait.StreamDebounceExt.html#method.debounce) | Hot; completed results use a bounded drop-oldest queue |
| Emit the leading value of each time window | [`throttle`](https://docs.rs/streamx/latest/streamx/trait.StreamThrottleExt.html#method.throttle) | Hot; completed results use a bounded drop-oldest queue |
| Sample state when a primary event arrives | [`with_latest_from`](https://docs.rs/streamx/latest/streamx/trait.StreamWithLatestFromExt.html#method.with_latest_from) | Pull-based primary, hot/conflating secondary |
| Multicast every event | [`share`](https://docs.rs/streamx/latest/streamx/trait.StreamShareExt.html#method.share) | Lossless; a slow strong subscriber backpressures upstream |
| Multicast without slow-subscriber back pressure | [`share_overflow`](https://docs.rs/streamx/latest/streamx/trait.StreamShareExt.html#method.share_overflow), [`share_latest`](https://docs.rs/streamx/latest/streamx/trait.StreamShareExt.html#method.share_latest) | Full live queues drop their oldest values |
| Multicast with history for new subscribers | [`share_replay`](https://docs.rs/streamx/latest/streamx/trait.StreamShareReplayExt.html#method.share_replay) | Lossless live delivery plus replay history |
| Multicast replayable state | [`share_replay_overflow`](https://docs.rs/streamx/latest/streamx/trait.StreamShareReplayExt.html#method.share_replay_overflow), [`share_replay_latest`](https://docs.rs/streamx/latest/streamx/trait.StreamShareReplayExt.html#method.share_replay_latest) | Replay history plus drop-oldest live delivery |

`hot` here means that upstream progress does not depend on downstream
`poll_next` calls. It does not imply multicast.

## Maintainers

[OPERATOR_SEMANTICS.md](https://github.com/vilicvane/streamx/blob/master/OPERATOR_SEMANTICS.md)
is the normative behavior and maintainer guide. Changes to buffering, polling,
completion, or lifetime must update that document and add focused tests before
changing implementation.

## License

MIT License.
