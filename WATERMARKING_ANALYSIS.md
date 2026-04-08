# Watermarking Implementation Analysis

## Branch: `feature/watermarking`

### Authors & Timeline
- **Daniil Gusev** (Oct 2025) — Original "Watermarks v0.2" implementation. Massive commit: +2641/-3664 lines. Built the entire watermarking subsystem, integrated it with all time windows, refactored window expiration from per-key to per-partition.
- **Remy Gwaramadze** (Oct 2025) — Integration with triggers, sinks, backpressure. 8 commits.
- **Steve Rosam** (Mar 2026) — Comprehensive test suite (`test_watermarking.py`, 23 tests).
- **Ludvík** (Mar 18, 2026) — 2 commits fixing the "stuck at beginning" bug.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                            KAFKA CLUSTER                                     │
│                                                                              │
│  ┌──────────────┐   ┌──────────────┐   ┌───────────────────────────────────┐ │
│  │ input-topic   │   │ input-topic   │   │ watermarks__<group>--watermarks  │ │
│  │ partition 0   │   │ partition 1   │   │ (single partition, compacted)    │ │
│  └──────┬───────┘   └──────┬───────┘   └──────────────┬────────────────────┘ │
└─────────┼──────────────────┼──────────────────────────┼──────────────────────┘
          │                  │                          │
          ▼                  ▼                          │
┌─────────────────────────────────────────┐             │
│         APPLICATION INSTANCE            │             │
│                                         │             │
│  ┌───────────────────────────────────┐  │             │
│  │        MAIN POLL LOOP             │  │             │
│  │                                   │  │             │
│  │  1. consumer.poll()               │  │             │
│  │       │                           │  │             │
│  │       ├── data message?           │  │             │
│  │       │   ├── extract timestamp   │  │             │
│  │       │   ├── WM.store(ts,        │  │             │
│  │       │   │     default=True) ──────────────┐      │
│  │       │   └── execute dataframe   │  │      │      │
│  │       │       (is_watermark=False)│  │      │      │
│  │       │                           │  │      │      │
│  │       └── watermark message? ◄────────────────────┘
│  │           ├── WM.receive(msg)     │  │      │
│  │           ├── calc global WM      │  │      │
│  │           └── for each partition:  │  │      │
│  │               execute dataframe   │  │      │
│  │               (is_watermark=True) │  │      │
│  │                                   │  │      │
│  │  2. commit_checkpoint()           │  │      │
│  │  3. WM.produce() ────────────────────────── ┘
│  │       │  (rate-limited, 1s)       │  │  produces to
│  │       └── mark checkpoint if      │  │  watermarks topic
│  │           watermarks produced     │  │
│  └───────────────────────────────────┘  │
│                                         │
│  ┌───────────────────────────────────┐  │
│  │     WATERMARK MANAGER STATE       │  │
│  │                                   │  │
│  │  _watermarks = {                  │  │
│  │    (topic, 0): 1712000000,        │  │
│  │    (topic, 1): 1711999500,  ◄── min = global watermark
│  │    (topic, 2): -1,          ◄── STUCK! never got data
│  │  }                                │  │
│  │                                   │  │
│  │  _to_produce = {                  │  │
│  │    (topic, 0): (1712000100, True),│  │
│  │    (topic, 1): (1711999800, True),│  │
│  │  }                                │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

---

## How Watermarks Flow Through the Pipeline

### Step 1: Watermark Extraction (Producer Side)

For every consumed data message (`app.py:1078-1086`):

```
message.timestamp → WatermarkManager.store(topic, partition, ts, default=True)
```

If the user calls `sdf.set_timestamp()` (`dataframe.py:758-809`):

```
custom_timestamp → WatermarkManager.store(topic, partition, ts, default=False)
```

**Priority rule**: non-default always overrides default; defaults never override non-defaults.

### Step 2: Watermark Production (Rate-Limited)

`WatermarkManager.produce()` (`watermarking.py:100-129`) — called every poll iteration but rate-limited to 1x/second:

```
_to_produce entries → JSON serialize → produce to watermarks topic
Key: "{topic}[{partition}]"
Value: {"topic": str, "partition": int, "timestamp": int}
```

### Step 3: Watermark Consumption (All Instances)

All instances subscribe to the single-partition watermarks topic. On receive (`watermarking.py:131-158`):

```
1. Update per-TP watermark:  _watermarks[tp] = max(current, received)
2. Calculate new global WM:  global = min(all _watermarks values)
3. If global advanced → return new watermark
4. If not → return None (skip processing)
```

### Step 4: Pipeline Propagation

When global watermark advances (`app.py:1051-1074`):

```
For each assigned data partition:
  → Create MessageContext(topic, partition, offset=None)
  → Execute StreamingDataFrame(value=None, key=None, timestamp=watermark, is_watermark=True)
```

All stream operators (filter, apply, update) **pass through** watermarks without executing user callbacks. Only `TransformFunction` with `on_watermark` callback executes — used by window operations.

### Step 5: Window Expiration

`TimeWindow.final().on_watermark` (`time_based.py:90-113`):

```
watermark_timestamp
    → expire_by_partition(transaction, watermark_ts)
    → max_expired_window_end = max(watermark_ts, latest_expired) - grace_ms
    → expire_all_windows(max_end_time=max_expired_window_end)
    → yield final window results
```

A window is expired when: `window.end ≤ watermark_timestamp - grace_ms`

---

## What Ludvík Changed (commit `610be41`)

### Problem: "Stuck at Beginning"

Before Ludvík's fix, the main loop was:

```python
# BEFORE (Daniil/Remy code)
watermark_manager.produce()      # produce() returned None
# checkpoint.empty() → True (no data offsets, no store txns)
# In EOS mode: empty checkpoint → ABORTED
# Watermark messages in the transaction → ROLLED BACK
# → Global watermark never advances → windows never expire
```

### Fix: 3 Changes

**1. `produce()` returns `bool`** (`watermarking.py:100-129`):
```python
def produce(self) -> bool:   # was: def produce(self):
    if monotonic() >= self._last_produced + self._interval:
        produced = bool(self._to_produce)  # NEW
        ...
        return produced  # NEW
    return False  # NEW
```

**2. Checkpoint tracks watermark production** (`checkpoint.py:58-70`):
```python
self._watermarks_produced = False  # NEW field

def mark_watermarks_produced(self):  # NEW method
    self._watermarks_produced = True
```

**3. Empty checkpoint considers watermarks** (`checkpoint.py:82-92`):
```python
def empty(self) -> bool:
    return (
        not bool(self._tp_offsets)
        and not bool(self._store_transactions)
        and not self._watermarks_produced  # NEW condition
    )
```

**4. Main loop marks checkpoint** (`app.py:982-983`):
```python
# AFTER (Ludvík's fix)
if watermark_manager.produce():
    processing_context.checkpoint.mark_watermarks_produced()
```

**Effect**: Checkpoints containing watermark messages are no longer treated as empty → not aborted in EOS mode → watermarks actually persist → global watermark advances → windows expire.

---

## Potential Causes of Missing Data

### 1. CRITICAL: Empty Partitions Block Global Watermark

`set_topics()` (`watermarking.py:37-53`) primes ALL partitions with `-1`:

```python
self._watermarks = {
    (topic.name, partition): -1
    for topic in topics
    for partition in range(topic.broker_config.num_partitions or 1)
}
```

Global watermark = `min(all TP watermarks)`. If ANY partition never receives data, its watermark stays at `-1` and the **entire global watermark is stuck at -1 forever**.

```
Partition 0: watermark = 1712000000  ✓ data flowing
Partition 1: watermark = 1711999500  ✓ data flowing
Partition 2: watermark = -1          ✗ NO DATA → blocks everything
                                     ↓
Global watermark = min(...) = -1     → windows NEVER expire
                                     → .final() NEVER emits
```

**This is the most likely cause of "missing data" — it's not lost, it's buffered in windows that never close.**

### 2. Revocation Doesn't Clean `_watermarks`

`on_revoke()` (`watermarking.py:64-69`) only removes from `_to_produce`:

```python
def on_revoke(self, topic: str, partition: int):
    tp = (topic, partition)
    self._to_produce.pop(tp, None)
    # _watermarks[tp] is NOT removed!
```

After revocation, the stale watermark entry stays in `_watermarks` and participates in `min()` calculation. If the partition is reassigned to another instance, its watermark is updated via the shared watermarks topic — but if it's never reassigned (e.g., scale-down), it stays stale.

### 3. Watermarks Topic Read From Beginning on Restart

Noted as a TODO (`app.py:1146-1148`):

```python
# TODO: Also, how to avoid reading the whole WM topic on each restart?
#  We really need only the most recent data
#  Is it fine to read it from the end? The active partitions must still publish something.
#  Or should we commit it?
```

On restart, the consumer reads the **entire** watermarks topic (compacted, but still). This can:
- Contain stale watermarks for partitions that no longer exist
- Cause slow startup if the topic has accumulated many unique keys

### 4. Watermark Produce AFTER Checkpoint Commit

In the main loop (`app.py:963-984`):

```
process_message()             ← stores watermarks via store()
commit_checkpoint()           ← may commit (flushes producer, commits offsets)
...
watermark_manager.produce()   ← produces watermarks to Kafka buffer
```

Watermarks are produced AFTER the checkpoint commit for the current batch. They'll be flushed in the NEXT checkpoint commit. If the app crashes between `produce()` and the next commit, those watermarks are lost. In at-least-once mode this is fine (messages replay). In EOS mode the watermark messages are part of the next transaction.

### 5. Rate Limiting Delays Window Closure

Default interval is 1.0 second. During burst processing, many messages can be processed between watermark publications. The effective watermark lags behind actual event time by up to 1 second plus consumer poll latency.

### 6. `set_timestamp()` Priority Can Mask Default Watermarks

If `set_timestamp()` is used on SOME messages but not all, the non-default watermark for a partition may prevent default watermarks from advancing it further (`watermarking.py:89-91`):

```python
if default and not stored_default:
    return  # Default cannot override non-default
```

This is by design but could cause confusion if the custom extractor returns lower timestamps than message timestamps.

### 7. Watermark as Min Across ALL Topics

If the application consumes from multiple topics with different throughputs, the slow topic holds back the global watermark for all windows — even those operating on the fast topic.

---

## Data Flow Diagram: Window Lifecycle

```
                 message arrives (ts=1000)
                         │
                         ▼
              ┌──────────────────────┐
              │  WM.store(tp, 1000)  │
              │  _to_produce[tp]=1000│
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  Window.process()    │
              │  update window       │
              │  [0, 60000)          │
              │  (if not expired)    │
              └──────────┬───────────┘
                         │
        ═══ 1 second passes ═══
                         │
                         ▼
              ┌──────────────────────┐
              │  WM.produce()        │──────► Kafka watermarks topic
              │  → returns True      │        {"topic":"x","partition":0,
              │  mark checkpoint     │         "timestamp":1000}
              └──────────────────────┘
                         │
        ═══ consumed by all instances ═══
                         │
                         ▼
              ┌──────────────────────┐
              │  WM.receive()        │
              │  _watermarks[tp]=1000│
              │  global = min(...)   │
              │  if advanced → 1000  │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  SDF(is_watermark=T) │
              │  for each partition  │
              └──────────┬───────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  Window.on_watermark │
              │  expire_by_partition │
              │  threshold = 1000    │
              │    - grace_ms        │
              │                      │
              │  window [0, 60000)?  │
              │  60000 ≤ 1000-grace? │
              │  NO → not expired    │
              └──────────────────────┘
                         │
        ═══ much later, watermark reaches 60000+grace ═══
                         │
                         ▼
              ┌──────────────────────┐
              │  expire_by_partition │
              │  60000 ≤ WM-grace?   │
              │  YES → EMIT RESULT   │───► downstream (sink, topic, etc.)
              └──────────────────────┘
```

---

## Key Files Reference

| Component | File | Key Lines |
|---|---|---|
| WatermarkManager | `quixstreams/processing/watermarking.py` | entire file (165 lines) |
| Main loop integration | `quixstreams/app.py` | 940-984, 1040-1086 |
| Partition assignment | `quixstreams/app.py` | 1135-1150 |
| Partition revocation | `quixstreams/app.py` | 1165-1205 |
| Checkpoint integration | `quixstreams/checkpointing/checkpoint.py` | 58-92 |
| Window expiration | `quixstreams/dataframe/windows/time_based.py` | 90-119, 319-343 |
| Window base (apply) | `quixstreams/dataframe/windows/base.py` | 92-124 |
| Stream function passthrough | `quixstreams/core/stream/functions/base.py` | 33-77 |
| Transform with on_watermark | `quixstreams/core/stream/functions/transform.py` | 56-92 |
| set_timestamp() | `quixstreams/dataframe/dataframe.py` | 758-809 |
| Topic config | `quixstreams/models/topics/manager.py` | 288-310 |
| Tests | `tests/test_quixstreams/test_processing/test_watermarking.py` | entire file |
