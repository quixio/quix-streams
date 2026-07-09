# Stateful Applications

Quix Streams provides a local, persistent state store backed by RocksDB. Data stored in state survives application restarts and is backed up to Kafka using changelog topics for fault tolerance.

Here, we will outline how stateful processing works.


## How State Relates to Kafka Message Keys

The most important concept to understand with state is that it depends on the message 
key due to how Kafka topic partitioning works.

**Every Kafka message key's state is independent and _inaccessible_ from all others.**
State for key `A` is only accessible while the application is processing a message with key `A`. You cannot read or write state for key `B` while handling a message with key `A`.  

Each key may belong to different Kafka topic partitions, and partitions are automatically 
assigned and re-assigned by Kafka broker to consumer apps in the same consumer group.

Be sure to consider this when making decisions around what your 
Kafka message keys should be.

The good news? _The library manages this aspect for you_, so you don't need to 
handle that complexity yourself: 

- The state store in Quix Streams keeps data per topic partition and automatically reacts to the changes in partition assignment.  
Each partition has its own RocksDB instance, therefore data from different partitions is stored separately, which
enables parallel processing of partitions.

- The state data is also stored per key, so the updates for the messages with key `A` are visible only for the messages with the same key.


### Example: 

There are two messages with two new message keys, `KEY_A` and `KEY_B`. 

A consumer app processes `KEY_A`, storing a value for it, `{"important_value": 5}`.

When another consumer reads the message with `KEY_B`, it will not be able to read or update the data for the key `KEY_A`.



## Using State in Custom Functions

The state is available in functions passed to `StreamingDataFrame.apply()`, `StreamingDataFrame.update()`, and `StreamingDataFrame.filter()` with parameter `stateful=True`:

```python
from quixstreams import Application, State
app = Application(
    broker_address='localhost:9092', 
    consumer_group='consumer', 
)
topic = app.topic('topic')

sdf = app.dataframe(topic)

def count_messages(value: dict, state: State):
    total = state.get('total', default=0)
    total += 1
    state.set('total', total)
    return {**value, 'total': total}
    
    
# Apply a custom function and inform StreamingDataFrame 
# to provide a State instance to it using "stateful=True"
sdf = sdf.apply(count_messages, stateful=True)

```

Currently, only functions passed to `StreamingDataFrame.apply()`, `StreamingDataFrame.update()`, and `StreamingDataFrame.filter()` may use `State`.


## Fault Tolerance & Recovery

To prevent state data loss in case of failures, all local state stores in Quix Streams are backed by the changelog topics in Kafka.  

**Changelog topics** are internal (hidden) topics that Quix Streams uses to keep the record of the state changes for the given state store.
Changelog topics use the same Kafka replication mechanisms which makes the state data highly available and durable.  
Changelog topics are enabled by default.  
The `Application` class will automatically create and manage them for each used state store.

Changelog topics have the same number of partitions as the source topic to ensure that partitions can be reassigned between consumers.   
They are also compacted to prevent them from growing indefinitely.


### How Changelog Topics Work
- When the application starts, it automatically checks which state stores need to be created.  
It will ensure the changelog topics exist for these stores.
- When the key is updated in the state store during processing, the update is staged in the current state transaction. During checkpoint commit, staged updates are produced to the changelog topic and then flushed to the local state store.
- When the application restarts or a new consumer joins the group, it will check whether the state stores are up-to-date with their changelog topics.    
If they are not, the application will first update the local stores, and only then will it continue processing the messages. 

### Source Offset Retention During Recovery

For stateful applications, Quix Streams uses the consumer group's committed source topic offsets as the recovery boundary for changelog replay.

If a committed source offset is lower than the broker's current low watermark for that source topic partition, the source records needed to validate the recovery boundary are no longer retained.

By default, Quix Streams treats this as a destructive recovery case: it deletes the local state for the affected partition, replays changelog records up to the source topic low watermark, seeks the source consumer to that low watermark, and logs a critical warning. This avoids getting stuck, but it necessarily loses state/source history before the retained source offset.

Use `state_recovery_offset_reset` to choose where automatic recovery resumes source consumption after deleting local state. The default `"earliest"` uses the broker low watermark as the changelog recovery boundary, then resumes source consumption from that same low watermark. This rebuilds the most complete state possible from Kafka's retained source data. `"latest"` resumes source consumption from the broker high watermark and skips changelog records that carry processed source-offset metadata, so retained source records are not reprocessed. Older changelog records without processed-offset metadata may still be applied during recovery. `"match"` follows `auto_offset_reset`; if `auto_offset_reset="error"`, Quix Streams raises `StateRecoveryOffsetOutOfRange`.

To fail instead of deleting local state automatically, set `auto_recover_from_source_offset_out_of_range=False` on `Application`. In that mode, Quix Streams raises `StateRecoveryOffsetOutOfRange` during partition assignment. To recover manually, reset the consumer group offset to a retained offset and clear or rebuild the local state directory, or start the application with a new consumer group and state directory.


### Creating Changelog Topics manually

Should you need to manage these changelog topics yourself (e.g. due to lack of permissions to create topics), you can find out what you need by running your `Application`.  
It prints what topics it expects to exist during initialization:

```
[2024-02-14 16:46:15,567] [INFO] : Initializing processing of StreamingDataFrame
[2024-02-14 16:46:15,567] [INFO] : Topics required for this application: "test-topic", "changelog__v4--test-topic--tumbling_window_60000_sum", "changelog__v4--test-topic--default"
[2024-02-14 16:46:15,573] [INFO] : Creating a new topic "changelog__v4--test-topic--tumbling_window_60000_sum" with config: "{'num_partitions': 1, 'replication_factor': 1, 'extra_config': {'cleanup.policy': 'compact'}}"
[2024-02-14 16:46:15,573] [INFO] : Creating a new topic "changelog__v4--test-topic--default" with config: "{'num_partitions': 1, 'replication_factor': 1, 'extra_config': {'cleanup.policy': 'compact'}}"
```

Be sure that the partition counts and `cleanup.policy` match what is printed.

### Disabling Changelog Topics

Should you need it, you can disable changelog topics by passing `use_changelog_topics=False` to the `Application()` object. 

> ***WARNING***: you will lose all stateful data should something happen to the local state stores, 
> so this is not recommended.
> 
> Also, re-enabling changelog topics will not "backfill" them from the state.  
> It will simply send new state updates from that point forward.


## Changing the State File Path

By default, an `Application` keeps the state in the `state` directory relative to the current working directory.  
To change it, pass `state_dir="your-path"` when initializing an `Application`:

```python
from quixstreams import Application
app = Application(
    broker_address='localhost:9092', 
    state_dir="folder/path/here",
)
```

## Clearing the State

To clear all the state data, use the `Application.clear_state()` command. 

This will delete all data stored in the state stores for the given consumer group, 
allowing you to start from a clean slate:

```python
from quixstreams import Application

app = Application(broker_address='localhost:9092')

# Delete state for the app with consumer group "consumer"
app.clear_state()
```

>***NOTE:*** Calling `Application.clear_state()` is only possible when the `Application.run()` is not running.  
> The state can be cleared either before calling `Application.run()` or after.  
> This ensures that state clearing does not interfere with the ongoing stateful processing.



## State Guarantees

Because Quix Streams currently handles messages with "At Least Once" delivery guarantees, it is possible
for the state to become slightly out of sync with a topic in between shutdowns and rebalances. 

While the impact of this is generally minimal and only for a small amount of messages, be aware this could cause side effects where the same message may be reprocessed differently, if it depended on certain state conditionals.

"Exactly Once" delivery guarantees avoid this. You can learn more about delivery/processing guarantees [here](https://quix.io/docs/quix-streams/configuration.html?h=#processing-guarantees).

## Serialization

By default, the keys and values are serialized to JSON for storage. If you need to change the serialization format, you can do so using the `rocksdb_options` parameter when creating the `Application` object. This change will apply to all state stores created by the application and existing state will be un-readable. Before changing the serialization format, call `app.clear_state()` before `app.run()` to delete the existing state stores, otherwise the application will fail to read them.

For example, you can use [python `pickle` module](https://docs.python.org/3/library/pickle.html) to serialize and deserialize all stores data.

```python
import pickle

from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

app = Application(
    broker_address='localhost:9092', 
    rocksdb_options=RocksDBOptions(dumps=pickle.dumps, loads=pickle.loads) 
)
```

You can also handle the serialization and deserialization yourself by using the [`State.get_bytes`](../api-reference/state.md#stateget_bytes) and [`State.set_bytes`](../api-reference/state.md#stateset_bytes) methods. This allows you to store any type of values in the state store, as long as you can convert it to bytes and back.

```python
import pickle

from quixstreams import Application, State
app = Application(
    broker_address='localhost:9092', 
    consumer_group='consumer', 
)
topic = app.topic('topic')

sdf = app.dataframe(topic)

def apply(value, state):
    old = state.get_bytes('key', default=None)
    if old is not None:
        old = pickle.loads(old)
    state.set_bytes('key', pickle.dumps(value))
    return {"old": old, "new": value}
    
sdf = sdf.apply(apply, stateful=True)
```


## State TTL

State TTL lets you attach an expiry duration to individual writes in a state store. You opt in per write by passing `ttl=timedelta(...)` to `state.set()`. Writes without a `ttl` argument never expire.

**If your pipeline never calls `state.set(..., ttl=...)`, nothing changes.** The on-disk layout, changelog bytes, and recovery path are identical to Quix Streams v3.23.6. TTL machinery activates only on the stores that actually use it.

For practical patterns and complete working examples (dedup filter, short-lived cache, toggle/expiry filter, status-in-key with `group_by`, env-var TTL), see the [State TTL — Cookbook](state-ttl.md).


### The API

```python
state.set(key, value, ttl=timedelta(days=7))   # expires after 7 days of event time
state.set(key, value)                           # never expires (default)
```

`ttl` accepts a `datetime.timedelta`. It must be greater than zero. Bare numbers and floats are not accepted. `ttl=None` is equivalent to omitting the argument — the entry never expires.

Expiry is based on **event time** — the timestamp of the Kafka record being processed — not the system clock. Replaying historical data works as expected: entries expire relative to the timestamps in the data, not relative to when you run the job.


### When to use it

**Deduplication over a fixed window.** You want to drop duplicate events if you have already seen the same message ID in the last seven days. You write the ID to state only on the first encounter. The expiry clock starts when the entry is created and never resets.

**Last-seen / heartbeat tracking.** You want to know when a device was last active and treat devices as "offline" after five minutes of silence. You write the device's last timestamp to state on every event, passing `ttl=` each time. Because the entry is re-stamped on every write, it lives for `ttl` from the *most recent* event — a sliding window.


### Fixed-window dedup

Write the TTL only on the first encounter. Subsequent events with the same ID find the key already present and are suppressed without touching it.

```python
from datetime import timedelta
from quixstreams import Application, State

app = Application(broker_address="localhost:9092")
topic = app.topic("events")
sdf = app.dataframe(topic)

def is_new(value: dict, state: State) -> bool:
    msg_id = value["id"]
    if state.get(msg_id) is not None:
        return False                              # already seen — drop it
    state.set(msg_id, 1, ttl=timedelta(days=7))  # first time — record it
    return True

sdf = sdf.filter(is_new, stateful=True)
```

After seven days of event time the entry expires, `state.get(msg_id)` returns `None`, and the same ID is accepted as new again.

The `ttl=` argument goes on the `state.set()` call inside your callback. There is no `ttl=` on `.filter()`, `.apply()`, or `.update()`.


### Sliding-window heartbeat

Write the TTL on every encounter. Each new event re-stamps the entry, so it lives for `ttl` from the last time the key was written.

```python
def touch_device(value: dict, state: State) -> dict:
    state.set(value["device_id"], value["ts"], ttl=timedelta(minutes=5))
    return value

sdf = sdf.update(touch_device, stateful=True)
```

Once a device stops sending events for longer than the TTL, its entry expires and `state.get(device_id)` returns `None`.


### Mixing expiring and permanent entries in one store

You can write some keys with a `ttl` and others without in the same callback and the same store. Writes without `ttl` never expire. Both kinds coexist with no extra configuration.

```python
from datetime import timedelta
from quixstreams import Application, State

app = Application(broker_address="localhost:9092")
topic = app.topic("events")
sdf = app.dataframe(topic)

def handle(value: dict, state: State) -> dict:
    # Expires 7 days after the event that last wrote it.
    state.set(f"seen:{value['id']}", 1, ttl=timedelta(days=7))

    # Lives forever — no ttl argument.
    state.set(f"config:{value['source']}", value["config"])

    return value

sdf = sdf.apply(handle, stateful=True)
```

This is the normal pattern for pipelines that track both transient events and stable configuration in a single callback.


### How TTL activates on a store

You do not need to declare that a store uses TTL. The framework detects it automatically on the first `state.set(..., ttl=...)` call that reaches a flush.

- **New store (empty).** On the first flush that contains a TTL write, the store switches into TTL mode. The switch is free and transparent.
- **Store with existing data.** If a store already contains data written without TTL and you deploy a code change that adds `state.set(..., ttl=...)`, the framework auto-migrates at the next flush:
  - **`RocksDBOptions(legacy_records_ttl=...)` is set** — the framework backfills every pre-existing record with a uniform expiry of `high_water + legacy_records_ttl` and then flips the store into TTL mode in place. No data is deleted. See [Upgrading an existing store](#upgrading-an-existing-legacy-store-legacy_records_ttl) below.
  - **`legacy_records_ttl` is not set** — the framework still auto-migrates, using the triggering write's own `ttl=` value as the implicit window (`high_water + max(ttl=)` in that batch). A one-time `[WARNING]` names the derived window and how to override it. See [Upgrading an existing store](#upgrading-an-existing-legacy-store-legacy_records_ttl) for details.

Once a store has switched into TTL mode, it stays in TTL mode for the life of that store. Plain `state.set(k, v)` calls on a TTL-enabled store still work and write a "never expires" sentinel — they do not turn off TTL for that key.


### Upgrading an existing (legacy) store - `legacy_records_ttl`

If your pipeline was already running before TTL existed, its state store holds records that were written without any expiry stamp. Deploying a new version that calls `state.set(..., ttl=...)` triggers an automatic in-place migration on the first flush: the framework re-stamps every pre-existing un-stamped record with a uniform expiry and flips the store into TTL mode with no state deletion.

`RocksDBOptions(legacy_records_ttl=timedelta(...))` sets the expiry window for those legacy records explicitly. Without it, the framework derives the window from the triggering write's own `ttl=` value (`high_water + max(ttl=)` in that batch) and emits a one-time `[WARNING]`.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

app = Application(
    consumer_group="my-dedup",
    rocksdb_options=RocksDBOptions(
        legacy_records_ttl=timedelta(days=7),   # opt-in; default None
    ),
)
```

#### What the option does and does not do

- **Optional, default `None`.** The migration runs automatically on the first `ttl=` write whether or not this option is set. Without it, the framework derives the expiry window from the triggering write's own `ttl=` and emits a one-time `[WARNING]` naming the value. Set this option to choose a different uniform expiry window for the legacy cohort.
- **Activation gate.** Setting the option alone does nothing. The backfill only runs when your code actually calls `state.set(..., ttl=...)`. If your code has no TTL writes, the option is inert and the store stays legacy.
- **One-time and durable.** The backfill runs exactly once. The re-stamped values and a migration-done marker are written to the changelog, so the migration survives any restart or full cold restore (complete rebuild from the changelog). Once the store has flipped into TTL mode it will not backfill again, and you can remove `legacy_records_ttl` from your config on the next deploy.
- **Bounded memory.** The backfill runs in chunks of `RocksDBOptions(legacy_backfill_chunk_size=10_000)` (default 10,000 records). A multi-million-record store migrates without running out of memory. Processing pauses for the duration of the one-time backfill.
- **Migration only.** `legacy_records_ttl` only affects pre-existing un-stamped records. New writes still get their expiry from the `ttl=` argument on each `state.set()` call. A write with no `ttl=` argument is still never-expires, regardless of this option.

#### Decision table at the first TTL write

| State of the store | `legacy_records_ttl` | Outcome |
|--------------------|----------------------|---------|
| Already migrated (done-flag present in changelog) | any | No-op — backfill never re-runs |
| Empty store | any | Clean flip into TTL mode, nothing to backfill |
| Populated with legacy records | set | Backfill all records at `high_water + legacy_records_ttl`, then flip into TTL mode |
| Populated with legacy records | not set (`None`) | Auto-migrate using `high_water + max(ttl=)` from the triggering batch; emits one `[WARNING]` naming the derived window |

#### Worked example — upgrading a dedup app

**Before (the old version, no TTL).** The dedup filter stores a key forever. Over time the store grows unbounded.

```python
from quixstreams import Application

app = Application(consumer_group="my-dedup", auto_offset_reset="earliest")
sdf = app.dataframe(app.topic("input"))

def dedup(value, key, timestamp, headers, state):
    if state.get("seen"):
        return False          # already seen — drop
    state.set("seen", True)   # no ttl= → remembered forever
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
sdf.to_topic(app.topic("output"))
app.run()
```

**After (same deployment, same state, add `legacy_records_ttl` and `ttl=`).** The pre-existing forever-keys are backfilled to expire in seven days. New keys also expire seven days after they are first seen.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

app = Application(
    consumer_group="my-dedup",        # same consumer group → same state
    auto_offset_reset="earliest",
    rocksdb_options=RocksDBOptions(
        legacy_records_ttl=timedelta(days=7),
    ),
)
sdf = app.dataframe(app.topic("input"))

def dedup(value, key, timestamp, headers, state):
    if state.get("seen"):
        return False
    state.set("seen", True, ttl=timedelta(days=7))   # now expires
    return True

sdf = sdf.filter(dedup, stateful=True, metadata=True)
sdf.to_topic(app.topic("output"))
app.run()
```

On startup, the log shows the one-time migration:

```
[INFO] [quixstreams.state] Backfilled 1234567 legacy records and flipped state store partition into TTL mode
```

After the migration has run once, you can remove `legacy_records_ttl` from `RocksDBOptions` on the next deploy. The store remains in TTL mode permanently.

#### Reference clock - when do migrated records expire?

Legacy records carry no original timestamp, so their true age is unrecoverable. The backfill stamps them to expire `legacy_records_ttl` (or the implicit derived window) after the **event-time high-water mark at the moment of migration** — not after each record's original age. All migrated records therefore drain together once the stream's event time advances by that window past the upgrade point.

Because expiry is event-time based, the expiry clock advances only while new messages arrive. An idle stream freezes the clock; migrated records do not expire until traffic resumes.

On a **cold restore** (state volume lost, rebuilt from the changelog), the **drop filter during replay** judges records against the **wall clock at rebuild time**: records whose TTL window has already passed are discarded; those still within their window are kept. After the rebuild, the live event-time clock starts from the first message processed — it is **not** seeded from wall-clock. A reprocessing workload replaying historical timestamps is not affected by when the rebuild ran; TTL expiry is measured from each write's own event-time, as in normal processing.

If a rebuild encounters an interrupted migration (some records already stamped, some not — a "MIXED" changelog), recovery auto-completes the remaining un-stamped records. With `legacy_records_ttl` set, they expire at `wallclock-at-rebuild + legacy_records_ttl`; without it, they inherit the expiry of the already-stamped cohort (or `SENTINEL_NEVER` with a `[WARNING]` in the unlikely case that no stamped record with a future expiry survives). No data is deleted.

A store created on the earlier TTL preview (stock v3.24.0) has stamped values in its changelog but no `__ttl_stamped__` headers (those were introduced later). Recovery detects this by checking whether every replayed key decodes as a valid 8-byte stamp. If all keys pass **and** you set `RocksDBOptions(adopt_v3240_stamps=True)`, recovery adopts the stamps verbatim: it flips the store into TTL mode, keeps every value as-is (original v3.24.0 stamps are preserved), and rebuilds the expiry index. No data is re-stamped or deleted. The store emerges from recovery fully TTL-enabled and continues normal expiry from each record's original stamp. Leaving `adopt_v3240_stamps=True` set after adoption is safe and has no effect on subsequent restarts.

If `adopt_v3240_stamps` is **not** set (the default), recovery logs a `CRITICAL` naming the flag and does nothing — the store stays in legacy mode and every value reads back unchanged. The default is conservative because a legacy store whose values happen to begin with 8 plausible numeric bytes is on-disk identical to a v3.24.0 store; auto-adopting the wrong store would silently corrupt it.

The memory backend mirrors the same opt-in contract. Pass `adopt_v3240_stamps=True` directly to the `MemoryStorePartition` constructor to enable adoption there. Production memory deployments are detection-only by default — `MemoryStore.create_new_partition` does not forward this flag, matching how `legacy_records_ttl` and `ttl_changelog_tombstones` work for the memory backend. A memory partition that sees the all-stamped census without the flag logs the same `CRITICAL` and leaves every value byte-identical.


### Troubleshooting

#### Enabling TTL on a store that already has data

**What happens.** You deployed a code change that adds `state.set(..., ttl=...)` to a callback, and the state store for that partition already contains records written before TTL was enabled. The framework does **not** error and does **not** silently drop the TTL: it **auto-finishes the migration**, re-stamping every pre-existing record with a uniform expiry and flipping the store into TTL mode in place. No state is deleted, and no changelog reset is needed. This works in environments where you cannot access the state directory directly (such as Quix Cloud).

**Which expiry the old records get.**

- If you set `legacy_records_ttl` on `RocksDBOptions`, old records expire at `high-water + legacy_records_ttl` (event-time at enable). Use this to choose the window explicitly.
- If you do **not** set it, old records inherit an *implicit* window equal to the triggering `state.set(..., ttl=...)` write's own duration (`high-water + that ttl`). A one-time `[WARNING]` names the count, the derived window, and how to override it:

```
[WARNING] [quixstreams.state] Enabled TTL on a populated legacy store WITHOUT legacy_records_ttl configured: auto-backfilled <N> pre-existing record(s) with an implicit expiry of high_water + <ttl_ms> ms (= <expiry>), derived from the triggering state.set(..., ttl=...) write. To choose a different uniform window for legacy records, set RocksDBOptions(legacy_records_ttl=timedelta(...)) and redeploy.
```

For the flagship deduplication workload — where every write uses one constant window — the implicit window equals that window, so the old dedup keys behave exactly as if freshly written at the enable moment. Set `legacy_records_ttl` explicitly only when you want a *different* window for the legacy cohort:

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

app = Application(
    broker_address="localhost:9092",
    rocksdb_options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
)
```

On the next flush the framework backfills every pre-existing record with the chosen (or implicit) expiry and flips the store into TTL mode. A one-time `[INFO]` line confirms completion. See [Upgrading an existing store](#upgrading-an-existing-legacy-store-legacy_records_ttl) for the full explanation and a worked example.

**Cold-restore completion.** If a rebuilt node replays a changelog whose migration was interrupted mid-backfill (some records already stamped, some not), it auto-completes the leftovers at end of recovery. With `legacy_records_ttl` set, they expire at `wallclock-at-rebuild + legacy_records_ttl`; without it, they inherit the expiry of the surviving stamped cohort (or never-expire, with a `[WARNING]`, in the rare case where no surviving stamp remains to derive from). No data is deleted.

> **Note.** The one hard error left on this path is a framework guard: if a `ttl=` write reaches flush with no event-time timestamp, the backfill raises `IncompatibleStateStoreError` rather than inventing a wall-clock expiry. That signals a bug in how `state.set(..., ttl=...)` was called (the framework injects the timestamp inside stateful callbacks), not a store-migration problem. Your existing state is intact after such an error.


### Semantics and limits

- **Expiry is event-time only in live processing.** A pipeline replaying old Kafka offsets sees entries expire relative to the timestamps in the data, not relative to today's date. During a **rebuild from the changelog** (cold restore), entries whose expiry has already passed by real-world clock at the moment the rebuild runs are not restored — even if the pipeline's event-time clock has not yet advanced past them. This is intentional: a purely event-time recovery filter would restore all uniformly-expiry-stamped records regardless of how much real time has elapsed since the store was written. After the rebuild, live processing is event-time only as normal. See [Reference clock — when do migrated records expire?](#reference-clock-when-do-migrated-records-expire) above for details on how the drop filter works during replay.
- **Reads are always consistent.** Once an entry's event-time expiry has passed relative to the current record's timestamp, `state.get()` returns `None` — even if the entry has not yet been physically deleted from disk. You will never read a stale expired value.
- **Physical eviction is approximate.** Expired entries are swept inside each `flush()`, capped at 10,000 evictions per flush by default. Each eviction also writes a tombstone to the store's changelog topic, so under the default `compact` policy the changelog physically shrinks in step with the local store. On a high-throughput partition, physical deletion can lag behind logical expiry — both local disk and changelog storage are bounded by the sweep budget. Space is eventually reclaimed; reads remain correct in the meantime.
- **No TTL on windowed stores.** Windowed aggregations manage their own retention via `grace_ms`. Passing `ttl=` to `state.set()` inside a windowed aggregation has no effect.
- **No per-store or per-callback default TTL.** There is no `ttl=` argument on `.apply()`, `.update()`, `.filter()`, or `register_store()`. Pass `ttl=` on each `state.set()` call where you want expiry.
- **Overwriting a key without `ttl=` on a TTL-enabled store makes it permanent.** Calling `state.set(k, v)` (no `ttl`) after a prior `state.set(k, v, ttl=...)` removes the expiry from that key. This is intentional: it gives you three primitives — expire, make permanent, delete.


### Debugging state and backfill

Set the `QUIXSTREAMS_STATE_LOG_LEVEL` environment variable to turn on verbose diagnostics for the `quixstreams.state` namespace without affecting the rest of the application.

```
QUIXSTREAMS_STATE_LOG_LEVEL=DEBUG
```

Accepted values: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. The variable is read at application start; redeploy to apply a change. When the variable is unset, the state namespace inherits the application log level — the default behavior is unchanged.

At `DEBUG`, the state logger emits per-flush and per-chunk backfill progress lines. This is useful for monitoring a large `legacy_records_ttl` migration (which can take several minutes on a store with millions of records) or for diagnosing unexpected recovery behavior.

The variable scopes its effect to `quixstreams.state` and its children. Non-state subsystems (the Kafka client, the application event loop, etc.) stay at the application log level.

#### What the logs show during a backfill

The migration emits a small set of one-time `INFO` lines you can watch (or grep) to follow its lifecycle. Everything per-record or per-chunk stays at `DEBUG`.

**Live migration** — first `state.set(..., ttl=...)` flush against a populated legacy store:

```
[INFO] TTL legacy backfill STARTED: <N> records to re-stamp path=<...>
[DEBUG] TTL legacy backfill progress: <n> / <N> records re-stamped path=<...>   (per chunk)
[INFO] TTL legacy backfill FINISHED: <N> records re-stamped path=<...>
[INFO] Backfilled <N> legacy records and flipped state store partition into TTL mode path=<...>
```

The backfill runs once, before new messages are processed, in chunks of `legacy_backfill_chunk_size` (default 10,000) — memory stays flat regardless of store size. Every chunk is confirmed on the changelog before the local commit, so a crash at any point is safe to resume.

**Cold restore of an already-migrated store** — the first stamped record replayed from the changelog flips the partition:

```
[INFO] Recovery: __ttl_stamped__ header on default-CF replay; flipping partition path=<...> into TTL mode for the rest of recovery.
```

**Cold restore of an interrupted migration** — if the process died mid-backfill, the changelog holds a mix of stamped and un-stamped records. Recovery detects the mix, replays everything, then finishes the migration by stamping only the genuine leftovers (records whose latest changelog entry is still un-stamped) — already-stamped records keep their original expiry:

```
[INFO] Recovery: completing interrupted legacy-TTL migration at path=<...>; <N> leftover legacy record(s) will be stamped with expiry=<...> (wallclock-at-rebuild + legacy_records_ttl).
[DEBUG] Recovery: legacy-TTL migration completion progress: <n> / <N> leftover record(s) stamped path=<...>   (per chunk)
[INFO] Recovery: completed legacy-TTL migration at path=<...>; stamped <N> leftover record(s); __ttl_backfill_pending__ is now empty.
```

The `<N> leftover` count is normally much smaller than the store — it is only the tail the interrupted run never reached. A durable done-flag is written after the last leftover, so subsequent restores skip completion entirely.

**Warm-restart resume** — if the process crashed after committing some backfill chunks but before the flag-last flip, the changelog already holds the re-stamped records. On any subsequent start (warm or cold), replay flips the partition via the `__ttl_stamped__` headers, and recovery detects the non-empty resume ledger and finishes the backfill:

```
[INFO] TTL legacy backfill RESUME STARTED: interrupted live migration detected at path=<...> (flipped, ledger non-empty, no done-marker); resuming over the un-stamped complement with expiry=<...>.
[INFO] TTL legacy backfill RESUME COMPLETED: stamped <N> leftover record(s) at path=<...>; done-marker produced, backfill bookkeeping cleaned.
```

The `RESUME STARTED` line names the path and the expiry derived from the surviving stamped cohort. The `RESUME COMPLETED` line confirms how many leftover records were stamped. After both lines, the done-marker is present and subsequent restarts skip the resume entirely.

**v3.24.0 preview-store adoption** — a store created on the earlier TTL preview can be adopted verbatim during rebuild when `RocksDBOptions(adopt_v3240_stamps=True)` is set:

```
[INFO] Recovery: adopted <N> v3.24.0-stamped record(s) at path=<...> (flipped into TTL mode; values kept verbatim, __ttl_index__ rebuilt from the adopted stamps).
```

Without the flag, recovery logs a `CRITICAL` naming `adopt_v3240_stamps` instead of this `INFO` line, and the store stays in legacy mode. See [Reference clock — when do migrated records expire?](#reference-clock-when-do-migrated-records-expire) for the adoption rationale.

**Wallclock-expired record drops** — whenever the wallclock filter discards at least one stamped record during replay, recovery logs a single aggregate `INFO` at the end of replay (never per-record). RocksDB:

```
[INFO] Recovery at path=<...> dropped <N> already-expired stamped record(s) during changelog replay (expired against recovery wallclock=<ms> ms; latest-record-wins).
```

Memory backend (no path):

```
[INFO] Recovery dropped <N> already-expired stamped record(s) from the in-memory changelog replay (expired against recovery wallclock=<ms> ms; latest-record-wins).
```

**Done-marker census discard** (RocksDB) — when a store's done-marker is present and there is an orphan pending-census to clean up, recovery logs one `INFO` before discarding (count 0 is logged too, as a "nothing to discard" signal):

```
[INFO] Recovery at path=<...>: durable migration done-marker present; discarding <N> orphan pending-census entry(ies) (store fully migrated, no completion needed).
```

If a migration appears stalled, raise `QUIXSTREAMS_STATE_LOG_LEVEL=DEBUG` and watch the per-chunk progress lines; a large store's backfill legitimately takes minutes (roughly 1–2k records/second including changelog confirmation).


### Tuning sweep budget

On partitions with a very high sustained expiration rate, the default 10,000 evictions per flush may not keep up with expiring entries. Local disk space and changelog storage will both grow even though reads remain correct — the same budget governs tombstone production to the changelog as governs local evictions. Every index-entry visit — including ghost entries left by TTL key re-stamps (a re-stamp mints a new index entry at the later expiry while the old entry becomes a ghost) — counts against this budget, bounding main-CF point-gets per flush to at most `max_evictions_per_flush`; ghosts are GC'd on each visit and converge to zero over successive sweeps. To increase the budget:

```python
from quixstreams import Application
from quixstreams.state.rocksdb.options import RocksDBOptions

app = Application(
    broker_address="localhost:9092",
    rocksdb_options=RocksDBOptions(max_evictions_per_flush=50_000),
)
```

Larger values increase the time spent inside each `flush()` call. Start with the default and increase only if you observe the state directory growing despite active expiry.

At the default `commit_interval` of 5 seconds, 10,000 evictions per flush translates to a sustained eviction capacity of roughly 2,000 keys per second per partition. If your expiration rate exceeds that, increase the budget.


### Changelog topic configuration

The changelog topic for a TTL-enabled store uses `cleanup.policy=compact` by default. With sweep tombstones enabled (the default), expired keys are written to the changelog as tombstones when they are evicted locally. Log compaction removes the superseded record and, after `delete.retention.ms`, the tombstone — so the changelog physically shrinks in step with the local store. No extra retention configuration is required.

`cleanup.policy=compact,delete` with an explicit `retention.ms` is optional. It acts as a secondary safeguard for keys on partitions that stopped receiving writes before those keys expired. Such keys are never revisited by the sweep, so their last changelog record persists until a future write triggers eviction or until you manually trigger topic compaction.

```
cleanup.policy = compact,delete
retention.ms   = <max_expected_ttl_ms + buffer>
```

To disable sweep tombstones and restore the previous local-only eviction behavior, set `RocksDBOptions(ttl_changelog_tombstones=False)`. With this option, the changelog retains the last record of each expired key until compaction reclaims it — identical to the behavior before sweep tombstones were introduced. In that case, `cleanup.policy=compact,delete` with a retention window is the recommended way to reclaim changelog space predictably. The `ttl_changelog_tombstones` flag has no effect on windowed or timestamped stores.


### API reference

- [`State.set()`](../api-reference/state.md#stateset) — accepts `ttl: Optional[timedelta] = None`
- [`State.set_bytes()`](../api-reference/state.md#stateset_bytes) — accepts `ttl: Optional[timedelta] = None`
- [`StreamingDataFrame.apply()`](../api-reference/dataframe.md#streamingdataframeapply)
- [`StreamingDataFrame.filter()`](../api-reference/dataframe.md#streamingdataframefilter)
- [`StreamingDataFrame.update()`](../api-reference/dataframe.md#streamingdataframeupdate)
