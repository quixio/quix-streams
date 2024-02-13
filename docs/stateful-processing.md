# Stateful Applications

Currently, Quix Streams 2.0alpha provides a RocksDB-based state store that allows to store 
data in persistent state and use them during stream proicessing.

This allows you to do things like compare a record to a previous version of it, or
do some aggregate calculations. Here, we will outline how stateful processing works.

<br>

## How State Relates to Kafka Keys

The most important concept to understand with state is that it depends on the message 
key due to how Kafka topic partitioning works.

What does this mean for you?

**Every Kafka key's state is independent and _inaccessible_ from all others; it is
accessible only while it is the currently active message key**.<br>
Each key may belong to different Kafka topic partitions, and partitions are automatically 
assigned and re-assigned by Kafka broker to consumer apps in the same consumer group.

Be sure to take this into consideration when making decisions around what your 
Kafka message keys should be.

The good news? _The library manages this aspect for you_, so you don't need to 
handle that complexity yourself: 

- State store in Quix Streams keeps data per each topic partition and automatically reacts to the changes in partition assignment.<br>
Each partition has its own RocksDB instance, therefore data from different partitions is stored separately, which
allows to process partitions in parallel.
- The state data is also stored per-key, so the updates for the messages with key `A` are visible only for the messages with the same key.


### Example: 

There are two messages with two new message keys, `KEY_A` and `KEY_B`. 

A consumer app processes `KEY_A`, storing a value for it, `{"important_value": 5}`.`

When another consumer reads the message with `KEY_B`, it will not be able to read or update the data for the key `KEY_A`.


<br>

## Using State

The state is available in functions passed to `StreamingDataFrame.apply()`, `StreamingDataFrame.update()` and `StreamingDataFrame.filter()` with parameter `stateful=True`:

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

Currently, only functions passed to `StreamingDataFrame.apply()`, `StreamingDataFrame.update()` and `StreamingDataFrame.filter()` may use State.

<br>

## Changing the State FilePath

By default, an `Application` keeps the state in `state` directory relative to the current working directory.
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
<br>
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

<br>


## State Guarantees

Because we currently handle messages with "At Least Once" guarantees, it is possible
for the state to become slightly out of sync with a topic in-between shutdowns and
rebalances. 

While the impact of this is generally minimal and only for a small amount of messages,
be aware this could cause side effects where the same message may be re-processed 
differently if it depended on certain state conditionals.

Exactly Once Semantics avoids this, and it is currently on our roadmap.

<br>

## Recovering State

Should something happen to an `Application`s stateful database (corruption, new 
application host, etc.), it can rebuild its state from changelog topics (enabled by 
default when using stateful processing).

### Changelog Topics

Changelog topics are "internal topics", AKA managed entirely by the `Application`.
This includes creating, producing, and consuming from them. The user should not have
to manage them.

They function as the "source of truth" for an Application's respective state stores. 
Depending on the stateful operations, there can be multiple changelog topics per 
Application.

They function differently from other topics, primarily due to how they retain data. 
They use "compaction", meaning they indefinitely store the latest value of a Kafka key,
enabling state rebuilds.

#### Manual Managing of Changelog Topics

Should you need to manage these changelog topics yourself (due to things like 
permission issues), you can find out what you need by running your `Application`, 
which prints what topics it expects to exist during initialization. 

Be sure that the partition counts and `cleanup.policy` match what is printed.

#### Disabling Changelog Topics

Should you need it, you can disable changelog topics via 
`Application(use_changelog_topics=False)`. 

> ***WARNING***: you will lose all stateful data should something happen to the databases, 
> so this is not recommended.

Re-enabling them will not "backfill" the changelog topics from state; 
it will simply store state updates from that point forward.

<br>

### Limitations 

#### Shared state directory 
In the current version, it's assumed that the state directory is shared between consumers (e.g. using Kubernetes PVC)
If consumers live on different nodes and don't have access to the same state directory, they will not be able to pick up state on rebalancing.
