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
- When the key is updated in the state store during processing, the update will be sent both to the changelog topic and the local state store.
- When the application restarts or a new consumer joins the group, it will check whether the state stores are up-to-date with their changelog topics.    
If they are not, the application will first update the local stores, and only then will it continue processing the messages. 


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

State TTL lets you attach an expiry duration to individual writes in a state store. You opt in per write by passing `ttl=timedelta(...)` as the third argument to `state.set()`. A write with no `ttl` argument never expires. Expiry is driven by the **event time** of each incoming record (the Kafka message timestamp), not by the wall clock. Eviction runs as a bounded sweep inside each `flush()` call — no new threads are created and no external scheduler is required.


### When to use it

- **Deduplication over a fixed window.** You want semantic idempotency: if you have already seen a message ID in the last seven days, drop the duplicate. You write the ID to state only on the first encounter, with a `ttl=timedelta(days=7)`. After seven days the entry expires, and the same ID is treated as new again.

- **Last-seen / heartbeat tracking.** You want to know when a device was most recently active. You write the device's timestamp to state on every encounter, passing `ttl=` each time. The TTL acts as a sliding window: as long as events keep arriving, the stamp is refreshed and the entry never expires. Once the device goes silent for longer than the TTL, the entry is evicted and the device is effectively forgotten.


### Quick example — deduplication

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

The `ttl=` argument goes on the `state.set()` call inside your callback, not on `.filter()` or `.apply()`.


### Fixed-window vs sliding-window semantics

The same store produces two different retention behaviors depending on whether your callback calls `state.set(..., ttl=...)` only on the first encounter or on every encounter.

**Fixed window** — set on first encounter only. The expiry clock starts when the entry is created and does not reset on subsequent events.

```python
def dedup(value: dict, state: State) -> bool:
    if state.get(value["id"]) is not None:
        return False                                    # seen before — suppress
    state.set(value["id"], 1, ttl=timedelta(days=7))   # first time — stamp it
    return True
```

**Sliding window** — set on every encounter. Each new event re-stamps the entry, so it lives for `ttl` from the *last* time it was seen.

```python
def touch_session(value: dict, state: State) -> dict:
    state.set(value["session_id"], value["ts"], ttl=timedelta(minutes=5))
    return value
```


### Mixing expiring and permanent entries in one store

Any callback can write some keys with a `ttl` and others without, all to the same store. Writes without `ttl` carry a "never expires" sentinel and live forever. Writes with `ttl` expire relative to the record's event time. Both coexist in the same RocksDB column family with no extra configuration.

```python
def handle(value: dict, state: State) -> dict:
    # This key expires 7 days after each event that writes it.
    state.set(f"seen:{value['id']}", 1, ttl=timedelta(days=7))

    # This key lives forever — no ttl argument.
    state.set(f"config:{value['k']}", value["v"])

    return value

sdf = sdf.apply(handle, stateful=True)
```

Mixing TTL behavior in the same callback is the normal pattern. Writes without a `ttl` argument live forever; writes with a `ttl` argument expire `ttl` of event-time after the record's timestamp. The same key can flip between expiring and permanent by overwriting it with or without `ttl=`.


### Semantics and gotchas

- **Expiry is event-time-based.** The expiry of an entry is `record.timestamp + ttl`. Replaying historical data does not instantly evict everything; entries expire relative to the timestamps in the data being processed, not relative to the system clock.

- **Eviction is approximate.** The sweep runs inside `flush()` and is capped at 10,000 evictions per flush by default. You can tune this with `RocksDBOptions(max_evictions_per_flush=N)`. On a low-traffic partition, physical eviction can lag by up to one `commit_interval` after logical expiry. Logically-expired entries are already invisible to reads — they are just not yet reclaimed from disk. This lag is by design.

- **Cold start.** Until at least one record has been processed, the partition has no event-time reference and no sweep runs. Reads return whatever is in the store.

- **8-byte overhead per value.** Every stored value carries an 8-byte expiry stamp on disk, including entries written without `ttl=`. The stamp is transparent to your code and negligible in practice.

- **Upgrading from v3.23.6 or earlier.** On-disk state stores from v3.23.6 and earlier use an incompatible layout. The application will raise `IncompatibleStateStoreError` on startup if it detects an old store. Delete the local state directory before starting; the store rebuilds from the changelog topic automatically. If the changelog topic also predates the new format, clear it too — all state is lost, but this is a one-time cost on upgrade.

- **Changelog topic retention.** For long-lived deployments with TTL, set the changelog topic's `retention.ms` to at least `ttl + a buffer` and use `cleanup.policy=compact,delete`. Without this, the changelog may retain data longer than the TTL — harmless but wastes storage. See [Changelog Topics](#how-changelog-topics-work) above.


### Current limitations

- **No per-store default TTL.** There is no `ttl=` argument on `.apply()`, `.update()`, `.filter()`, or `register_store()`. If you want every entry in a callback to expire, pass `ttl=` on each `state.set()` call.
- **No wall-clock TTL.** Expiry is always event-time. There is no wall-clock fallback.
- **No TTL on windowed stores.** Windowed state stores manage their own retention via `grace_ms`; `ttl=` on `state.set()` has no effect inside windowed aggregations.


### API reference

- [`State.set()`](../api-reference/state.md#stateset)
- [`State.set_bytes()`](../api-reference/state.md#stateset_bytes)
- [`StreamingDataFrame.apply()`](../api-reference/dataframe.md#streamingdataframeapply)
- [`StreamingDataFrame.filter()`](../api-reference/dataframe.md#streamingdataframefilter)
- [`StreamingDataFrame.update()`](../api-reference/dataframe.md#streamingdataframeupdate)
