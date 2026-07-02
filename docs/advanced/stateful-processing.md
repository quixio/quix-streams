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
- **Store with existing data.** If a store already contains data written without TTL and you deploy a code change that adds `state.set(..., ttl=...)`, the pipeline will raise `IncompatibleStateStoreError` on the next flush. See [Troubleshooting](#troubleshooting) below.

Once a store has switched into TTL mode, it stays in TTL mode for the life of that store. Plain `state.set(k, v)` calls on a TTL-enabled store still work and write a "never expires" sentinel — they do not turn off TTL for that key.


### Troubleshooting

#### `IncompatibleStateStoreError` on startup or first flush

**What it means.** You deployed a code change that adds `state.set(..., ttl=...)` to a callback, but the state store for that partition already contains data written before TTL was enabled. The framework refuses to mix the two layouts silently, because silent mixing would defeat the purpose of TTL on deduplication workloads.

The error log names the state directory path, the approximate number of existing entries, and the action to take:

```
ERROR  IncompatibleStateStoreError: state store at <path> has <N>
       un-stamped existing entries; cannot enable TTL on a populated
       store. To enable TTL: stop the application, delete the state
       directory at <path>, restart — recovery will rebuild from the
       changelog with TTL enabled.
```

**How to fix it.**

1. Stop the application.
2. Delete the local state directory (default: `./state`, or whatever `state_dir` is set to in your `Application`). You can also use `app.clear_state()` before calling `app.run()`.
3. Restart the application. The state store rebuilds automatically from the changelog topic.

If the changelog topic also contains data written before TTL was enabled (for example, the pipeline ran for weeks before you added TTL), recovery will rebuild a non-TTL store and the next TTL write will fail again. In that case you also need to delete (or reset the offsets of) the changelog topic before restarting.

> **Note.** The pipeline does not partially commit the failed flush. Your existing state is intact after the error — only the new TTL writes were rejected.


### Semantics and limits

- **Expiry is event-time only.** There is no wall-clock fallback. A pipeline replaying old Kafka offsets sees entries expire relative to the timestamps in the data, not relative to today's date.
- **Reads are always consistent.** Once an entry's event-time expiry has passed relative to the current record's timestamp, `state.get()` returns `None` — even if the entry has not yet been physically deleted from disk. You will never read a stale expired value.
- **Physical eviction is approximate.** Expired entries are swept from disk inside each `flush()`, capped at 10,000 evictions per flush by default. On a high-throughput partition, physical deletion can lag behind logical expiry. Disk space is eventually reclaimed; reads remain correct in the meantime.
- **No TTL on windowed stores.** Windowed aggregations manage their own retention via `grace_ms`. Passing `ttl=` to `state.set()` inside a windowed aggregation has no effect.
- **No per-store or per-callback default TTL.** There is no `ttl=` argument on `.apply()`, `.update()`, `.filter()`, or `register_store()`. Pass `ttl=` on each `state.set()` call where you want expiry.
- **Overwriting a key without `ttl=` on a TTL-enabled store makes it permanent.** Calling `state.set(k, v)` (no `ttl`) after a prior `state.set(k, v, ttl=...)` removes the expiry from that key. This is intentional: it gives you three primitives — expire, make permanent, delete.


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
