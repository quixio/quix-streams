# State TTL — Cookbook

Practical patterns for per-write expiry in Quix Streams state stores.

For the full reference — API semantics, how TTL activates on a store, upgrade
guide, configuration knobs, and troubleshooting — see
[Stateful Processing — State TTL](stateful-processing.md#state-ttl).


## Quick-start patterns

Any stateful callback receives a `state` handle. Pass `stateful=True` (and
`metadata=True` if you need the message key, timestamp, or headers) so the
callback signature is `(value, key, timestamp, headers, state)`.

### Dedup filter — expire by field value

```python
from datetime import timedelta

def dedup(value, key, timestamp, headers, state):
    if state.get("status") == value["status"]:
        return False  # same status, still within TTL — drop
    state.set("status", value["status"], ttl=timedelta(minutes=5))
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
```

### Short-lived lookup cache (enrich)

```python
from datetime import timedelta

def enrich(value, key, timestamp, headers, state):
    cached = state.get("profile")
    if cached is None:
        cached = fetch_profile(value["user_id"])
        state.set("profile", cached, ttl=timedelta(hours=1))
    return {**value, "profile": cached}

sdf = sdf.apply(enrich, stateful=True, metadata=True)
```

### Rolling counter with expiry

```python
from datetime import timedelta

def bump(value, key, timestamp, headers, state):
    count = (state.get("count") or 0) + 1
    state.set("count", count, ttl=timedelta(minutes=10))
    value["count"] = count

sdf = sdf.update(bump, stateful=True, metadata=True)
```


## Same TTL for every entry

Hoist the TTL into a single constant. Every entry written by the callback
expires after the same duration.

```python
from datetime import timedelta

DEFAULT_TTL = timedelta(minutes=5)

def dedup(value, key, timestamp, headers, state):
    if state.get("last_status") == value["status"]:
        return False
    state.set("last_status", value["status"], ttl=DEFAULT_TTL)
    state.set("last_seen_at", value["timestamp"], ttl=DEFAULT_TTL)
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
```

Both `last_status` and `last_seen_at` expire five minutes after the most
recent write.

You can reuse the same constant across multiple callbacks:

```python
from datetime import timedelta

DEFAULT_TTL = timedelta(minutes=5)

def dedup(value, key, timestamp, headers, state):
    if state.get("status") == value["status"]:
        return False
    state.set("status", value["status"], ttl=DEFAULT_TTL)
    return True

def bump(value, key, timestamp, headers, state):
    count = (state.get("count") or 0) + 1
    state.set("count", count, ttl=DEFAULT_TTL)
    value["count"] = count

sdf = sdf.filter(dedup, stateful=True, metadata=True)
sdf = sdf.update(bump, stateful=True, metadata=True)
```

To tune the window without a code change, read it from an environment variable:

```python
import os
from datetime import timedelta

DEFAULT_TTL = timedelta(seconds=int(os.environ.get("STATE_TTL_SECONDS", "300")))
```


## Refreshing a key (keep-alive)

Re-writing a key resets its TTL clock — each `state.set(key, value, ttl=...)`
starts a fresh countdown from that write, replacing whatever expiry the entry
had before:

```python
state.set("session", data, ttl=timedelta(minutes=5))
# ... 4 minutes later, another message arrives for the same key:
state.set("session", data, ttl=timedelta(minutes=5))  # clock restarts at 5:00
```

This is safe regardless of timing: a refresh always wins, even when it lands in
the exact moment the previous, already-expired entry is being cleaned up. The
refreshed value is never lost and keeps expiring normally on its new schedule.
You never need to `get` first, delete, or otherwise guard a refresh.

A refresh can also change the duration (`ttl=timedelta(hours=1)` on a key
originally written with minutes), or make the key permanent by writing it
without `ttl` — a plain `state.set(key, value)` never expires.


## Emit only on toggle or after TTL expiry

A window sensor emits readings like `window_open_ON` and `window_open_OFF`. You
want the output topic to receive an event only when:

1. **The value toggles** — e.g. `ON` → `OFF`, or `OFF` → `ON`.
2. **The TTL has expired** — no event was seen for five minutes, so the next
   reading (even if identical to the previous one) is treated as fresh and
   forwarded.

A single stateful key with a TTL handles both cases:

```python
from datetime import timedelta

TTL = timedelta(minutes=5)

def emit_on_change_or_expiry(value, key, timestamp, headers, state):
    # value["event"] is e.g. "window_open_ON" or "window_open_OFF"
    cached = state.get("last_window_event")

    if cached == value["event"]:
        # Same value, still within TTL — drop
        return False

    # Either the value toggled, or the cached entry expired (cached is None)
    state.set("last_window_event", value["event"], ttl=TTL)
    return True

sdf = sdf.filter(emit_on_change_or_expiry, stateful=True, metadata=True)
```

Behavior summary:

| Incoming event | Cached state         | Result            |
|----------------|----------------------|-------------------|
| `ON`           | `None` (expired/new) | **emit**          |
| `ON`           | `ON`                 | drop              |
| `OFF`          | `ON`                 | **emit** (toggle) |
| `OFF`          | `None` (expired)     | **emit** (TTL)    |


## Status encoded in the Kafka message key

If the status is encoded in the Kafka key itself (e.g. `window_open_ON` /
`window_open_OFF`) rather than in the value, the previous pattern breaks. Quix
Streams partitions state by message key, so `window_open_ON` and
`window_open_OFF` each get their **own** state scope and never see each other's
`last_status` — the filter would always emit.

Fix: parse the key into `entity` + `status`, then use `group_by` on the entity
so both variants share one state scope.

```python
import os
from datetime import timedelta
from quixstreams import Application

TTL = timedelta(seconds=int(os.environ.get("STATE_TTL_SECONDS", "300")))

app = Application(consumer_group="window-dedup", auto_offset_reset="earliest")
input_topic  = app.topic(os.environ["input"])   # keyed as window_open_ON / window_open_OFF
output_topic = app.topic(os.environ["output"])

sdf = app.dataframe(input_topic)

# 1. Split the key into entity + status, stash both on the value.
def parse_key(value, key, timestamp, headers):
    key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
    entity, _, status = key_str.rpartition("_")   # "window_open_ON" → ("window_open", "_", "ON")
    return {**(value or {}), "_entity": entity, "_status": status}

sdf = sdf.apply(parse_key, metadata=True)

# 2. Repartition by entity so ON and OFF share one state scope.
sdf = sdf.group_by(lambda v: v["_entity"], name="by_entity")

# 3. Emit only on toggle or after TTL expiry.
def emit_on_change_or_expiry(value, key, timestamp, headers, state):
    status = value["_status"]
    if state.get("last_status") == status:
        return False                              # same status, still within TTL — drop
    state.set("last_status", status, ttl=TTL)     # toggled OR expired → emit + reset clock
    return True

sdf = sdf.filter(emit_on_change_or_expiry, stateful=True, metadata=True)

sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()
```

`state.get("last_status")` returns `None` once the TTL expires, so the next
event (even with identical status) is treated as fresh and forwarded.


## Changelog storage and sweep tombstones

When a key expires and is swept from the local store, the framework also writes a tombstone (a delete marker) to the store's changelog topic. Under the default `compact` policy, Kafka removes the superseded record and, after `delete.retention.ms`, the tombstone itself — so the changelog physically shrinks in step with the local store. You do not need to configure `cleanup.policy=compact,delete` or set a retention window for this to work.

The sweep budget (`max_evictions_per_flush`, default 10,000) governs tombstone production at the same rate as local evictions. A higher budget evicts and tombstones more keys per flush.

To disable tombstone production and revert to local-only eviction:

```python
from quixstreams.state.rocksdb.options import RocksDBOptions

app = Application(
    rocksdb_options=RocksDBOptions(ttl_changelog_tombstones=False),
)
```

With `ttl_changelog_tombstones=False`, the changelog retains the last record of each expired key until compaction reclaims it. This is identical to the behavior before sweep tombstones were introduced, and may be useful if you manage changelog retention manually or want to defer to a `cleanup.policy=compact,delete` retention window. This flag has no effect on windowed or timestamped stores.

### Changelog topic sizing for short TTLs

Every expiry produces a tombstone. A 2-minute TTL replaces the entire live keyspace roughly every 2 minutes, so tombstone ingest roughly doubles the normal write rate. Two Kafka defaults work against you here:

- **`segment.bytes` (default 1 GiB).** The log cleaner only compacts closed segments. On a low-throughput partition a 1 GiB segment may never close, which freezes compaction regardless of policy. Setting `segment.bytes` to `67108864` (64 MiB) or `134217728` (128 MiB) closes segments quickly and lets compaction keep pace with expiry.
- **`delete.retention.ms` (default 24 hours).** Kafka retains tombstones for this long. For a 2-minute TTL, 24 hours is 720 TTL windows of tombstones. Setting it to a few TTL windows — for example `600000` ms (10 minutes) for a 2-minute TTL — reclaims topic space far sooner while still giving rebuilding consumers enough time to complete a cold restore.

Apply these settings to the changelog topic. The application prints the topic name at startup; it follows the pattern `changelog__v4--<source-topic>--<store-name>`.

If you apply a `retention.bytes` cap to the changelog, keep in mind that a cold restore can only rebuild as much state as the changelog currently retains. A cap tight enough to evict live records limits how complete a cold restore can be.


## Upgrading an existing store

If your pipeline ran before TTL existed (no `state.set(..., ttl=...)` calls anywhere in your code), see [Upgrading an existing (legacy) store](stateful-processing.md#upgrading-an-existing-legacy-store-legacy_records_ttl) for the full guide, including `legacy_records_ttl` configuration and the before/after worked example.

If your pipeline ran on v3.24.0 (the TTL preview), read on.

### Upgrading from v3.24.0

The scenarios below cover what happens to each store shape when you deploy the current version over a v3.24.0 deployment. No manual steps are required; all migration paths are automatic.

#### Warm upgrade — state volume intact

On a normal upgrade that preserves the state directory (Quix Cloud rolling update, Kubernetes PVC, Docker named volume), the store already carries the TTL flag that v3.24.0 wrote. The new build opens it, resumes TTL mode from the first second, and the sweep starts reclaiming expired keys. No migration log lines appear.

#### Cold restore — state volume absent or empty

When the state volume is gone and the store rebuilds from its changelog, the v3.24.0 records arrive without the `__ttl_stamped__` Kafka header used by the current release. Recovery identifies the stamp pattern embedded in the value bytes, adopts the store provisionally, and logs a single `[WARNING]`:

```
[WARNING] Auto-adopted <N> v3.24.0-shaped record(s) from the changelog at path=<...> (REVERSIBLE): values kept verbatim, __ttl_index__ rebuilt, originals backed up to __ttl_adopt_backup__, sweep-deletion suppressed until corroborated by a live ttl= write. If this is actually a pre-TTL legacy store, set QUIXSTREAMS_STATE_TTL_ROLLBACK=1 and restart to roll back (originals restored byte-identical).
```

This `[WARNING]` is expected and does not indicate a problem. Sweep-deletion stays suppressed until the adoption is confirmed. On the first live `state.set(..., ttl=...)` write that reaches a flush, corroboration fires and the adoption becomes permanent:

```
[INFO] Corroborated v3.24.0 adoption at path=<...> on a live ttl= write; produced the durable migration-done marker, cleared the pending marker, lifted sweep suppression (backup dropped after the commit barrier).
```

Subsequent cold restores find the durable marker and skip the provisional path entirely.

#### Interrupted migration — process crashed mid-backfill

If the application crashed while a `legacy_records_ttl` backfill was in progress, the store holds a mix of stamped and un-stamped records with no `__ttl_enabled__` flag. The new build detects the migration bookkeeping and resumes automatically:

```
[INFO] Repaired an interrupted legacy-TTL migration at path=<...>: found migration bookkeeping (<artifacts>), no __ttl_enabled__ flag, a default CF that samples as uniformly TTL-stamped (<M> of <N> sampled value(s), <K> of them still live), and no __ttl_backfill_in_progress__ marker. Resuming TTL mode and persisting the flip; the leftover census is completed by the recovery pass before live processing resumes.
```

Already-stamped records keep their original expiry. The remaining un-stamped records are completed before live processing starts. The `legacy_records_ttl` backfill also now skips any value that already carries a stamp, so a partial migration can never produce double-wrapped records.

An INFO line logged at startup is normal. If the sample contains stamps that are all already in the past (a store whose keys have fully drained), recovery still adopts it and logs an additional note — the presence of valid stamp bytes, not their liveness, is the evidence used.

#### Self-heal for double-stamped records

An earlier build contained a bug that could re-wrap already-stamped v3.24.0 records during a `legacy_records_ttl` backfill, producing `outer_stamp||inner_stamp||payload`. The current build repairs these automatically during changelog replay and at index rebuild:

```
[WARNING] Recovery at path=<...> repaired <N> DOUBLE-STAMPED record(s) during changelog replay: their payload was itself a TTL stamp (outer_stamp||inner_stamp||payload), the signature of an earlier build's live backfill re-stamping already-stamped v3.24.0 values and producing them to the changelog. The outer stamp was stripped once and the record landed with its ORIGINAL expiry, so reads return the real payload instead of raising a serialization error. The changelog records themselves are unchanged (nothing is produced back); every rebuild of this consumer group repairs them ...
```

Each record is unwrapped to its original expiry. Because the changelog itself still carries the double-stamped bytes, each cold restore repairs them on ingest — this is expected and not a sign of data loss.

### What if something looks wrong — operator levers

Two operational levers cover rare misidentification cases. Both default to off and are mutually exclusive; setting both raises a `ValueError` at startup.

**Roll back a wrong cold adoption**

If the cold adoption fired on a store that actually holds pre-TTL application data (the 8-byte values look like stamps but are real payload), set `QUIXSTREAMS_STATE_TTL_ROLLBACK=1` in the deployment environment or `RocksDBOptions(ttl_rollback=True)` in code, then restart. On a warm restart the pre-adoption originals are restored byte-identical and the store reverts to legacy mode. On a fresh volume the provisional adoption is suppressed entirely. This lever only touches provisional (uncorroborated) adoptions — a corroborated store is immune. Unset it and restart once the rollback has taken effect.

**Force-flip a store whose TTL flag went missing**

If the log shows a line containing `NOT flipping it` and recommends setting `QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1`, set that environment variable or `RocksDBOptions(ttl_force_flip=True)` in code, then restart. The lever persists the `__ttl_enabled__` flag and lets the recovery pass finish the migration. It is a no-op on a store already in TTL mode, so it is safe to leave set for one restart and then clear.

Prefer the `RocksDBOptions` form in Quix Cloud. A deployment environment variable not declared in `app.yaml` is silently dropped on redeploy, which can make a lever disappear between runs.
