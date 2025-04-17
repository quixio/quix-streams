# Stateful Applications

Quix Streams provides a RocksDB-based state store that enables the storage of data in a persistent state for use during stream processing.

Here, we will outline how stateful processing works.


## How State Relates to Kafka Message Keys

The most important concept to understand with state is that it depends on the message 
key due to how Kafka topic partitioning works.

**Every Kafka message key's state is independent and _inaccessible_ from all others; it is
accessible only while it is the currently active message key**.  

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

By default, the keys and values are serialized to JSON for storage. If you need to change the serialization format, you can do so using the `rocksdb_options` parameter when creating the `Application` object. This change will apply to all state stores created by the application and existing state will be un-readable.

For example, you can use [python `pickle` module](https://docs.python.org/3/library/pickle.html) to serialize and deserialize all stores data.

```python
import pickle

from quixstreams import Application
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
