# Stateful Applications

Quix Streams v2 provides a RocksDB-based state store that allows to store 
data in the persistent state and use it during stream processing.

This allows you to do things like compare a record to a previous version of it, or
do some aggregate calculations.  
Here, we will outline how stateful processing works.


## How State Relates to Kafka Keys

The most important concept to understand with state is that it depends on the message 
key due to how Kafka topic partitioning works.

**Every Kafka key's state is independent and _inaccessible_ from all others; it is
accessible only while it is the currently active message key**.  

Each key may belong to different Kafka topic partitions, and partitions are automatically 
assigned and re-assigned by Kafka broker to consumer apps in the same consumer group.

Be sure to take this into consideration when making decisions around what your 
Kafka message keys should be.

The good news? _The library manages this aspect for you_, so you don't need to 
handle that complexity yourself: 

- State store in Quix Streams keeps data per each topic partition and automatically reacts to the changes in partition assignment.  
Each partition has its own RocksDB instance, therefore data from different partitions is stored separately, which
allows to processing of partitions in parallel.

- The state data is also stored per key, so the updates for the messages with key `A` are visible only for the messages with the same key.


### Example: 

There are two messages with two new message keys, `KEY_A` and `KEY_B`. 

A consumer app processes `KEY_A`, storing a value for it, `{"important_value": 5}`.`

When another consumer reads the message with `KEY_B`, it will not be able to read or update the data for the key `KEY_A`.


<br>

## Using State

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
    
    
# Apply a custom function and inform StreamingDataFrame to provide a State instance to it via passing "stateful=True"
sdf = sdf.apply(count_messages, stateful=True)

```

Currently, only functions passed to `StreamingDataFrame.apply()`, `StreamingDataFrame.update()`, and `StreamingDataFrame.filter()` may use State.


## Fault Tolerance & Recovery

Quix Streams stores data in the local state stores, and these stores are not fault-tolerant out of the box.  
For example, a certain hard disk may get corrupted, and the data on this disk will be lost.

To prevent that, Quix Streams uses a mechanism based on Changelog topics to keep the state data in Kafka, which is already highly available and replicated.  

**Changelog topic** is an internal kind of topic that Quix Streams uses to keep the record of the state changes for the given state store.  
Changelog topics are "on" by default, and the `Application` class will create and manage them for each used state store.

Changelog topics have the same number of partitions as the source topic to ensure that partitions can be reassigned between consumers.   
They are also compacted to prevent them from growing indefinitely.


### How They Work
- When the application starts, it automatically checks which state stores need to be created, and it will also ensure the changelog topics for these stores.
- When the key is updated in the state store during processing, the update will be sent both to the changelog topic and to the local database.
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

Should you need it, you can disable changelog topics via 
`Application(use_changelog_topics=False)`. 

> ***WARNING***: you will lose all stateful data should something happen to the local state stores, 
> so this is not recommended.
> 
> Also, re-enabling changelog topics will not "backfill" them from the state.  
> It will simply send new state updates from that point forward.


## Changing the State File Path

By default, an `Application` keeps the state in the `state` directory relative to the current working directory.  
To change it, pass `state_dir="your-path"` to `Application` or `Application.Quix` calls:

```python
from quixstreams import Application
app = Application(
    broker_address='localhost:9092', 
    consumer_group='consumer', 
    state_dir="folder/path/here",
)

# or

app = Application.Quix(
    consumer_group='consumer', 
    state_dir="folder/path/here",
)
```

## Clearing the State

To clear all the state data, use the `Application.clear_state()` command. 

This will delete all data stored in the state stores for the given consumer group, 
allowing you to start from a clean slate:

```python
from quixstreams import Application

app = Application(broker_address='localhost:9092', consumer_group='consumer')

# Delete state for the app with consumer group "consumer"
app.clear_state()
```

Note that clearing the app state using `Application.clear_state()` 
is only possible when the `Application.run()` is not running. 
Meaning that the state can be cleared either before calling `Application.run()` or after.
This ensures that state clearing does not interfere with the ongoing stateful processing.


## State Guarantees

Because we currently handle messages with "At Least Once" guarantees, it is possible
for the state to become slightly out of sync with a topic in between shutdowns and
rebalances. 

While the impact of this is generally minimal and only for a small amount of messages,
be aware this could cause side effects where the same message may be re-processed 
differently, if it depended on certain state conditionals.

Exactly Once Semantics avoids this, and it is currently on our roadmap.
