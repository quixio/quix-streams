# Stream Timeout Tracker

`StreamTimeoutTracker` is a sink-agnostic, per-key silence detector. It fires a user callback when a tracked "stream" (typically a Kafka message key) has been silent past a configurable threshold. The module is stdlib-only and has zero imports from any specific sink or from Quix-platform-specific types, so **any** `quixstreams` sink (core, community, or third-party) can compose it.

It lives at `quixstreams.sinks.core.stream_timeout_tracker`.

## When to use it

Use the tracker when your sink needs any of the following:

- Detect when individual message keys stop arriving (sensor drop-out, stream finished, session idle).
- Fire a callback exactly once per silence period per key, with fire-and-evict + re-arm semantics.
- Keep the detection logic identical across sinks — no need to reimplement timer loops, TTL sweeps, or lock discipline.

Use it as a drop-in component; do not subclass it.

## How It Works

One "stream" equals one key. Callers invoke `touch(stream_key)` every time a record arrives; the tracker records the last-seen wall-clock time and fires the configured callback exactly once per silence period per key.

### Raw-key pass-through

Keys are stored and surfaced **as-is**. Any hashable value is accepted: `str`, `bytes`, `int`, etc. The callback receives the exact object that was passed to `touch`. `None` keys are silently skipped.

This differs from earlier versions that decoded bytes → str before firing. Consumers that care about the string form of a bytes key should decode inside the callback.

### Fire-and-evict + re-arm

When a key crosses the silence threshold:

1. The entry is evicted from the in-memory dict.
2. The callback is invoked with the raw key.
3. An INFO log line is emitted.

A later `touch()` for the same key resurrects it as a fresh stream, eligible to fire again.

### TTL safety sweep

Any entry older than `3 × stream_timeout_ms` is dropped silently (WARNING logged, no callback). This bounds the dict size in degenerate cases.

### Check cadence

Two code paths drive checks:

- **Synchronous**: the host sink calls `check_now()` at the end of each flush.
- **Background daemon thread**: started by `start()`, runs `check_now()` every `max(100, min(1000, stream_timeout_ms // 5))` ms.

The background thread **self-terminates** after `idle_exit_cycles` (default 3) consecutive empty cycles and is **respawned** by the next `touch()` that records a stamp. This keeps the sink at zero overhead while idle.

### Concurrency

`touch()` and `check_now()` can run on different threads. A single `threading.Lock` guards the per-key dict and timer-thread reference. Critical sections are tiny; user callbacks are invoked **outside** the lock, so a blocking callback cannot stall `touch()`.

## Public API

```python
from quixstreams.sinks.core.stream_timeout_tracker import StreamTimeoutTracker

tracker = StreamTimeoutTracker(
    stream_timeout_ms=60_000,            # positive int to enable, None to disable
    on_stream_timeout=lambda key: ...,   # Callable[[Any], None]
    check_interval_ms=None,              # override periodic cadence
    idle_exit_cycles=3,                  # empty cycles before self-terminate
    thread_name="StreamTimeoutTracker-check",
    logger=None,                         # defaults to module logger
)

tracker.enabled       # True iff both params are valid
tracker.touch(key)    # record a key (call from the host sink's `add()`)
tracker.check_now()   # synchronous check (call from the host sink's `flush()`)
tracker.start()       # start the background daemon thread (call from `setup()`)
tracker.stop()        # signal the thread to exit (call from `cleanup()`)
```

Both `stream_timeout_ms` and `on_stream_timeout` must be provided together. Mismatched pairs raise `ValueError`. Passing `None` for both silently disables the tracker — every public method becomes a no-op and the per-key dict is never allocated.

## Composing it into a custom sink

The recommended lifecycle wiring mirrors what `QuixTSDataLakeSink` does:

```python
from quixstreams.sinks.base import BatchingSink
from quixstreams.sinks.core.stream_timeout_tracker import StreamTimeoutTracker


class MyCustomSink(BatchingSink):
    def __init__(self, *, stream_timeout_ms=None, on_stream_timeout=None, **kwargs):
        super().__init__(**kwargs)
        self._timeout = StreamTimeoutTracker(
            stream_timeout_ms=stream_timeout_ms,
            on_stream_timeout=on_stream_timeout,
            thread_name="MyCustomSink-timeout-check",
        )

    def add(self, value, key, timestamp, headers, topic, partition, offset):
        super().add(value, key, timestamp, headers, topic, partition, offset)
        self._timeout.touch(key, topic=topic, partition=partition, offset=offset)

    def flush(self):
        super().flush()
        self._timeout.check_now()

    def setup(self):
        super().setup()
        self._timeout.start()

    def cleanup(self):
        self._timeout.stop()
        super().cleanup()
```

The tracker treats `topic`/`partition`/`offset` as opaque kwargs; pass any per-record context you want — the tracker currently ignores them (reserved for future log enrichment).

## Callback contract

- **Signature**: `Callable[[Any], None]`. The argument is the raw key passed to `touch()`.
- **Runs on**: the flush thread (for `check_now()` triggers) or the background thread (for periodic triggers). Both are the same in practice from a throughput standpoint — the consumer is not polling while either runs.
- **Must not block**. A slow or blocking callback stalls the consumer heartbeat and can trigger a rebalance. Use fire-and-forget patterns (e.g. `producer.produce()` without a subsequent `flush()`).
- **May raise**. Exceptions are logged and swallowed; the entry is **not** re-inserted, so the key won't fire again on retry. A later `touch()` re-arms it.

## Fire latency

Worst-case fire latency is `stream_timeout_ms + check_interval_ms`. With a 60-second threshold and the default 1-second check interval, expect a fire 60–61 seconds after the last message for a given key.

With a flush-only cadence, fire latency would be unbounded on a fully silent topic — `BatchingSink` stops calling `flush()` with no batches to process. The background thread closes that gap.

## Restart / rebalance behaviour

Tracking state is in-memory only. On process restart or Kafka partition rebalance, the per-key dict for affected keys is lost. Keys that remain dormant after a restart or rebalance will **not** fire for their current silence cycle; they resume normal tracking the next time they publish.

If you need fire-once-across-restarts semantics, persist "already fired" state outside the tracker (e.g. in the application's state store or a downstream dedupe layer).
