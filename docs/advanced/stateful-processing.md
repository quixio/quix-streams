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
- **Store with existing data.** If a store already contains data written without TTL and you deploy a code change that adds `state.set(..., ttl=...)`, there are two outcomes at the next flush:
  - **`RocksDBOptions(legacy_records_ttl=...)` is set** — the framework backfills every pre-existing record with a uniform expiry and then flips the store into TTL mode in place. No data is deleted. See [Upgrading an existing store](#upgrading-an-existing-legacy-store--legacy_records_ttl) below.
  - **`legacy_records_ttl` is not set** — the framework raises `IncompatibleStateStoreError`. See [Troubleshooting](#troubleshooting) for the fix.

Once a store has switched into TTL mode, it stays in TTL mode for the life of that store. Plain `state.set(k, v)` calls on a TTL-enabled store still work and write a "never expires" sentinel — they do not turn off TTL for that key.


### Upgrading an existing (legacy) store — `legacy_records_ttl`

> **Preview feature.** Available from Quix Streams 3.24.1a2.

If your pipeline was already running before TTL existed, its state store holds records that were written without any expiry stamp. Deploying a new version that calls `state.set(..., ttl=...)` will trigger `IncompatibleStateStoreError` on the first flush — unless you opt in to the in-place migration.

`RocksDBOptions(legacy_records_ttl=timedelta(...))` opts in. When set, the framework backfills every pre-existing un-stamped record with a uniform expiry on the first TTL-enabled flush, then flips the store into TTL mode — all in place, with no state deletion.

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

- **Opt-in, default `None`.** Leaving the option unset preserves the original behavior: a populated legacy store still raises `IncompatibleStateStoreError` on the first `ttl=` write.
- **Activation gate.** Setting the option alone does nothing. The backfill only runs when your code actually calls `state.set(..., ttl=...)`. If your code has no TTL writes, the option is inert and the store stays legacy.
- **One-time and durable.** The backfill runs exactly once. The re-stamped values are written to the changelog, so the migration survives a restart. Once the store has flipped into TTL mode it will not backfill again, and you can remove `legacy_records_ttl` from your config on the next deploy.
- **Bounded memory.** The backfill runs in chunks of `RocksDBOptions(legacy_backfill_chunk_size=10_000)` (default 10,000 records). A multi-million-record store migrates without running out of memory. Processing pauses for the duration of the one-time backfill.
- **Migration only.** `legacy_records_ttl` only affects pre-existing un-stamped records. New writes still get their expiry from the `ttl=` argument on each `state.set()` call. A write with no `ttl=` argument is still never-expires, regardless of this option.

#### Decision table at the first TTL write

| State of the store | `legacy_records_ttl` | Outcome |
|--------------------|----------------------|---------|
| Already migrated (`__ttl_enabled__` present) | any | No-op — backfill never re-runs |
| Empty store | any | Clean flip into TTL mode, nothing to backfill |
| Populated with legacy records | set | Backfill all records, then flip into TTL mode |
| Populated with legacy records | not set (`None`) | Raises `IncompatibleStateStoreError` |

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

#### Reference clock — when do migrated records expire?

Legacy records carry no original timestamp, so their true age is unrecoverable. The backfill stamps them to expire `legacy_records_ttl` after the **event-time high-water mark at the moment of migration** — not after each record's original age. All migrated records therefore drain together once the stream's event time advances by `legacy_records_ttl` past the upgrade point.

Because expiry is event-time based, the expiry clock advances only while new messages arrive. An idle stream freezes the clock; migrated records do not expire until traffic resumes.

On a **cold restore** (state volume lost, rebuilt from the changelog), expiry for migrated records is judged against the **wall clock at rebuild time** rather than against the event-time stamp written during backfill. This prevents all migrated records from appearing expired simultaneously on replay.


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

On the next flush the framework backfills every pre-existing record with the chosen (or implicit) expiry and flips the store into TTL mode. A one-time `[INFO]` line confirms completion. See [Upgrading an existing store](#upgrading-an-existing-legacy-store--legacy_records_ttl) for the full explanation and a worked example.

**Cold-restore completion.** If a rebuilt node replays a changelog whose migration was interrupted mid-backfill (some records already stamped, some not), it auto-completes the leftovers at end of recovery. With `legacy_records_ttl` set, they expire at `wallclock-at-rebuild + legacy_records_ttl`; without it, they inherit the expiry of the surviving stamped cohort (or never-expire, with a `[WARNING]`, in the rare case where no surviving stamp remains to derive from). No data is deleted.

> **Note.** The one hard error left on this path is a framework guard: if a `ttl=` write reaches flush with no event-time timestamp, the backfill raises `IncompatibleStateStoreError` rather than inventing a wall-clock expiry. That signals a bug in how `state.set(..., ttl=...)` was called (the framework injects the timestamp inside stateful callbacks), not a store-migration problem. Your existing state is intact after such an error.


### Semantics and limits

- **Expiry is event-time only.** There is no wall-clock fallback. A pipeline replaying old Kafka offsets sees entries expire relative to the timestamps in the data, not relative to today's date.
- **Reads are always consistent.** Once an entry's event-time expiry has passed relative to the current record's timestamp, `state.get()` returns `None` — even if the entry has not yet been physically deleted from disk. You will never read a stale expired value.
- **Physical eviction is approximate.** Expired entries are swept from disk inside each `flush()`, capped at 10,000 evictions per flush by default. On a high-throughput partition, physical deletion can lag behind logical expiry. Disk space is eventually reclaimed; reads remain correct in the meantime.
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


### Tuning sweep budget

On partitions with a very high sustained expiration rate, the default 10,000 evictions per flush may not keep up with expiring entries. Disk space will grow even though reads remain correct. To increase the budget:

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

For long-running pipelines with TTL-enabled stores, set the changelog topic's retention to at least `ttl + a buffer`:

```
cleanup.policy = compact,delete
retention.ms   = <max_expected_ttl_ms + buffer>
```

Without this, the changelog retains expired entries indefinitely. This is harmless for correctness (recovery filters them out) but wastes storage. The `compact` policy alone keeps the latest value per key forever; you need `delete` as well to enforce time-based purging.


### API reference

- [`State.set()`](../api-reference/state.md#stateset) — accepts `ttl: Optional[timedelta] = None`
- [`State.set_bytes()`](../api-reference/state.md#stateset_bytes) — accepts `ttl: Optional[timedelta] = None`
- [`StreamingDataFrame.apply()`](../api-reference/dataframe.md#streamingdataframeapply)
- [`StreamingDataFrame.filter()`](../api-reference/dataframe.md#streamingdataframefilter)
- [`StreamingDataFrame.update()`](../api-reference/dataframe.md#streamingdataframeupdate)
