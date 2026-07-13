# RocksDB Lock-Contention / Rebalance Handover Analysis

When multiple consumer replicas share a state volume and a Kafka rebalance occurs, the outgoing consumer must close its RocksDB instances before the incoming consumer can open them. If closing blocks on in-progress background work — which it does by default — the outgoing consumer can miss Kafka's `max.poll.interval.ms` deadline, be evicted from its group, yet still hold the OS file lock. The new owner then spins on a locked database, also misses its poll deadline, and the group enters a crash-loop. This document describes the three-stage fix and the subsequent hardening that together bound every blocking step in the revoke and assign paths.

## The livelock

RocksDB uses an OS-level file lock to enforce single-writer access per database directory. The sequence that triggers the crash-loop is:

1. A rebalance assigns a partition from consumer A to consumer B.
2. Consumer A's revoke callback calls `db.close()`. By default, `db.close()` joins any in-progress background flush or compaction, which can run for seconds on a large or write-heavy state store.
3. Consumer A's poll deadline (`max.poll.interval.ms`) passes while it is still inside `db.close()`. Kafka evicts consumer A from the group.
4. The eviction triggers another rebalance. Consumer B is assigned the same partition and attempts to open the DB.
5. Consumer A is still holding the OS file lock (still inside `db.close()`). Consumer B's open fails or retries.
6. If consumer B's retries outlast its own poll deadline, it is also evicted. The group cycles back to step 1.

The lock is not held maliciously. The outgoing consumer is doing the right thing — flushing data to disk — but there is no bound on how long that takes.

## The fix, in stages

### Stage 1 — Fast shutdown: cancel background flush/compaction before close()

The fix interrupts in-progress background compaction and flush jobs before calling `db.close()`, using `cancel_all_background(True)`. The OS lock is released in milliseconds rather than seconds. Unflushed memtable data is preserved by the Write-Ahead Log; cancelled compaction debt resumes under the new owner.

`cancel_all_background` is not present in every rocksdict release within the supported `>=0.3,<0.4` range. The call is guarded with `getattr`; when the symbol is absent, `close()` falls back to the plain (slower) close rather than raising.

If `cancel_all_background` itself raises, the failure is logged at `WARNING` and `close()` is still attempted. Skipping `close()` on a cancel failure would leak the OS lock — the exact livelock this fix addresses.

After a successful cancel, RocksDB may surface a "Shutdown in progress" error from `close()`. This is benign: the database has closed and the lock has been released. The condition is logged at `DEBUG` and not re-raised. On the plain-close fallback (no cancel), the same error is genuine and propagates.

### Stage 2 — Fast revoke: skip the local state flush for changelog-backed stores

A normal checkpoint commit writes pending state transactions to disk at step 5. During a revoke, this write holds the RocksDB lock for the duration of the flush, which can be substantial.

`Checkpoint.commit(revoking=True)` skips step 5 for stores that have a changelog topic. The changelog already holds the delta produced in step 2 of the same commit; the new owner replays it during recovery and arrives at the same state without needing an on-disk flush from the old owner. This releases the RocksDB lock earlier in the revoke sequence.

Stores without a changelog topic are always flushed because there is no other durable record of the pending writes.

### Stage 3 — Stop-flag-aware open-retry loop

When the outgoing consumer is slow to close its DB, the incoming consumer's open-retry loop waits by sleeping between open attempts. If the application is simultaneously stopping (for example, because it was evicted), a sleeping lock-waiter delays the shutdown for the full retry budget.

A `stop_event` (`threading.Event`) is threaded from `Application` through `StateStoreManager` down to every `RocksDBStorePartition`. `Application.stop()` sets the event; `Application.run()` clears it at startup. Inside the open-retry backoff, instead of `time.sleep(backoff)`, the loop calls `stop_event.wait(backoff)`. When the event is set, `wait` returns immediately and the loop raises `RocksDBOpenAborted` — a dedicated exception that signals intentional abort rather than lock exhaustion. The abort is fast: no remaining backoff sleep is taken.

## Hardening (review follow-ups)

### Bounded revoke flushes

To prevent any single blocking operation in the revoke path from consuming the entire `max.poll.interval.ms` budget, both the sink flush (step 1) and the producer flush (step 3) are bounded by a wall-clock timeout during `Checkpoint.commit(revoking=True)`.

The timeout for each is `max.poll.interval.ms × 0.2`. At the default `max.poll.interval.ms` of 300,000 ms (300 s), each bounded flush has a 60 s budget.

Sink flushes are bounded by running `sink.flush()` in a daemon thread and joining it with the timeout. If the join times out or the thread raises any exception other than `SinkBackpressureError`, the checkpoint aborts without committing offsets. The new owner reprocesses the batch and re-flushes (at-least-once). `SinkBackpressureError` still propagates through to the existing backpressure handler in `commit()`. An orphaned flush thread that times out but continues running may complete its write after reprocessing has landed, which can produce a duplicate in the sink; this is acceptable under at-least-once semantics.

The producer flush is bounded by passing the same timeout to `producer.flush(timeout=...)`. If messages remain undelivered after the timeout, `changelog_confirmed` is set to `False`. A `False` value disables the fast-revoke skip in step 5: every store flushes locally regardless of whether it has a changelog, avoiding state loss when changelog delivery is unconfirmed.

### Shared per-assign open deadline

`OpenDeadline` is a shared wall-clock budget for the total RocksDB-open time across all partitions opened during a single `_on_assign` callback. It is armed at the start of `_on_assign` with a budget of `max.poll.interval.ms × 0.5` (150 s at the 300 s default) and is disarmed in the callback's `finally` block.

Every `RocksDBStorePartition` opened during that callback receives the same `OpenDeadline` instance. Before sleeping between open retries, the partition checks whether the remaining budget covers the next backoff. If the budget is exhausted or would be exceeded by the next sleep, the loop stops and re-raises the underlying lock error. It does not raise `RocksDBOpenAborted` — that exception is reserved for the graceful-stop path (stage 3) — so a deadline overrun keeps today's restart semantics but triggers the restart sooner instead of waiting out the full retry budget.

Outside a rebalance callback, `OpenDeadline` is disarmed and never expires, so opens in other contexts (sources, for example) are unbounded as before.

### Orphaned recovery-pause resume on reassignment

During recovery, `RecoveryManager` pauses data partitions to prevent processing before state is rebuilt. If those partitions are subsequently revoked and reassigned in a rebalance that brings no stateful partitions to the consumer, the normal resume path does not run and the partitions remain paused indefinitely.

`RecoveryManager.resume_reassigned_data_partitions` is called on every assignment. It resumes any data partitions still in the recovery-paused set that are no longer undergoing active recovery. The call is a no-op while a recovery is pending or active. A resume is logged at `INFO`:

```
Resuming data partitions paused for recovery: [("<topic>", <partition>), ...]
```

### Close-error handling

If `cancel_all_background` raises, the failure is logged at `WARNING` and `close()` still proceeds — never skip `close()` on a cancel failure. Genuine errors from `close()` (anything other than "Shutdown in progress" following a successful cancel) propagate to the caller as before.

## Operator guidance

### Tuning max.poll.interval.ms

The bounded flush timeouts and the open deadline all scale proportionally with `max.poll.interval.ms`. There is no separate config knob for any of them; raising `max.poll.interval.ms` automatically lengthens every per-step budget.

Pipelines with several slow sinks or large state stores should raise `max.poll.interval.ms` beyond its 300,000 ms default. Pass it in the extra consumer config:

```python
app = Application(
    broker_address="localhost:9092",
    consumer_extra_config={"max.poll.interval.ms": 600000},  # 600 s
)
```

### Log reference

| Level | Message fragment | What it means |
|-------|-----------------|---------------|
| `WARNING` | `Failed to cancel background work before closing rocksdb partition on "..."` | `cancel_all_background` raised; `close()` will still proceed. |
| `DEBUG` | `Benign "shutdown in progress" closing rocksdb partition on "..."` | Expected after a successful cancel; lock is released. |
| `WARNING` | `Failed to open rocksdb partition on "...", cannot acquire a lock (attempt N/M)` | Lock still held by the previous owner; retrying. |
| `WARNING` | `Open budget exhausted for rocksdb partition on "..."` or `Open budget for rocksdb partition on "..." would be exceeded by the next retry` | Per-assign open deadline expired (or the next backoff would overrun it); the consumer will restart. |
| `WARNING` | `Revoke: sink "..." flush timed out after ...s` | Sink flush overran its budget; checkpoint aborted, offsets not committed. |
| `WARNING` | `Revoke: producer flush timed out with '...' undelivered changelog message(s)` | Changelog delivery unconfirmed; falling back to full local state flush. |
| `INFO` | `Resuming data partitions paused for recovery: [...]` | Orphaned recovery-paused partitions resumed on reassignment. |
