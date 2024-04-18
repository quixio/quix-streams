# Grouping (re-keying) Data with GroupBy

## What is a "GroupBy" operation?

`GroupBy` is a common operation in things like SQL, Pandas, or even
Excel (commonly done with pivot tables).

It is basically a way of aggregating or organizing your data for performing certain 
operations on particular subsets of it.

## A SQL "GroupBy" Example

Suppose you had an `Orders` table like so:

`SELECT * FROM Orders`

| store_id | item | quantity |
|:--------:|:----:|:--------:|
|    4     |  A   |    5     |
|    4     |  C   |    8     |
|    4     |  B   |    2     |
|    2     |  A   |    2     |
|    2     |  B   |    1     |

<br>

Suppose you want a summary of the quantities ordered. 

Of course, you could get a simple total (`18`) using `SUM(quantity)`. 

However, using `GroupBy`, you could additionally calculate `quantity` per `item`:

`SELECT item, SUM(quantity) FROM Orders GROUPBY item` 

| item | quantity |
|:----:|:--------:|
|  A   |    7     |
|  C   |    8     |
|  B   |    3     |

This is why GroupBy is so useful! So, how do we accomplish this using Kafka?

## How Does a "GroupBy" Translate to Kafka?

### Constraints

With Kafka, we of course don't have static tables of data; we have messages on topics, which
we cannot realistically access on demand. So how is `GroupBy` useful in the context of Kafka?


### How Data is Organized in Kafka

To understand `GroupBy`, it's important to understand Kafka message keys and topic partitioning.

As a lightning quick summary, messages are sorted on topics based on their message keys: if
a message has key `X`, then all messages that also have key `X` will always end up on the same topic partition. This means
you have guarantees around message ordering for that key with respect to itself (but NOT to other keys!).

> NOTE: Understanding keys is also very important for [stateful operations with Quix Streams](./advanced/stateful-processing.md)

### Repartitioning By Changing Keys

A `GroupBy` for Kafka is simply reorganizing your messages by changing
their message keys so that the respective data we wish to operate on shows up on the 
same partition.

This allows you to perform things like stateful aggregations similar to our `SUM()`
example above.

In this case, it's easier to understand with an example, like the one below:


## A Kafka "GroupBy" Example

### The Situation

Imagine you have the same data from the [SQL example above](#a-groupby-example-using-sql), only now each row of the table is 
instead a message.

The message key is `store_id`, and the `item` and `quantity` columns are in the message value:

```python
message_1 = {"key": "store_4", "value": {"item": "A", "quantity": 5}}
# ...etc...
message_5 = {"key": "store_2", "value": {"item": "B", "quantity": 1}}
```

### Want: Sum per Item

Now imagine you have a `Quix Streams Application` totaling each item's quantities (regardless of store) 
just like our previous `SUM(quantity)`, ultimately sending each new updated total downstream.

> NOTE: storing/performing aggregations like this requires using [a state store](./processing.md#using-state-store) or [windowing](./windowing.md).

### Problematic Message Keys

Unfortunately, the results won't be what you expect with the current message format.

If you had two consumers, one consumer might process all messages related to `store_4`, while the other processes `store_2`, due to the 
way Kafka divides partitions across consumers (matching keys end up on 
the same partition). 

Since these consumers don't share any state between each other, we have a problem.

Basically, your the totals per consumer are **actually**:

- _total quantity per store_ (which ignores `item` name)

which is NOT the desired:

-  _total quantity per item_ (which ignores `store_id`).

### Re-Keying the Messages

If we repartition or messages where the key is instead the `item` name:

```python
message_1 = {"key": "A", "value": {"item": "A", "quantity": 5}}
# ...etc...
message_5 = {"key": "B", "value": {"item": "B", "quantity": 1}}
```

then the _per item_ totals will now be accurately generated since each message relating to
a given `item` will handled by the same consumer instance.

This re-keying can be easily accomplished with the `StreamingDataFrame.group_by()` operation.

## StreamingDataFrame.group_by()

### What it Does

`StreamingDataFrame.group_by()` allows you to seamlessly change message keys while also
including further processing, all with the same `StreamingDataFrame` instance.

> Note: State-based operations will also use the updated keys!

Re-keying usually requires an additional application, but `.group_by()` eliminates that need.

### Using .group_by()

Assume you want to re-key messages to be the value from `column_X`.

With `StreamingDataFrame.group_by()`, there are two options:

1. provide a column name
    - `StreamingDataFrame.group_by("column_X")`
    - you can optionally provide a name for it

2. provide a custom grouping function with a unique name
    - `StreamingDataFrame.group_by(lambda row: row["column_X"], name="col-X")`
    - the `name` field is required for this and must be unique per `.group_by()`

> NOTE: Your message value must be valid json (like a dict) for `.group_by()`. See 
> "Advanced Usage" for using other message value formats.

In either case, the result will be processed onward with the updated key, including
having it updated in `message.context()`.


### Example (with column name)

```python
sdf = StreamingDataFrame()
sdf["new_column"] = sdf["a_column"] + "append_me"
sdf = sdf.group_by("column_x")  # uses "column_x" as new message key
sdf = sdf["new_column"] + "another_append"
sdf = sdf.to_topic(topic_out)
```

## Advanced Usage 

Most users will likely not need this, but here are more details around how 
`StreamingDataFrame.group_by()` works, and how to additionally configure it.

### How GroupBy works
Each `GroupBy` operation is facilitated by a unique "internal" topic. By 
default, its settings are inherited from its origin topic. 

It also uses `JSON` serialization and deserialization by default for keys AND values, 
but you can configure them as shown below.

### Configuring the Internal Topic

Though you cannot provide alter the `GroupBy` topic name that will be used, 
you _can_ alter its config like serialization, partition count, and any other topic 
settings (like retention).

Here is an example of doing so (noting the topic name will be ignored):

```python
from quixstreams import Application
from quixstreams.models.topics import TopicConfig

app = Application(broker_address='localhost:9092')
input_topic = app.topic("input_topic")
groupby_config = app.topic(
   "my_groupby", 
   value_serializer="string",
   value_deserializer="string",
   config=TopicConfig(num_partitions=2, replication_factor=1),
   add_to_cache=False
)

sdf = app.dataframe(input_topic)
sdf = sdf.group_by('my_col_name', config_topic=groupby_config)
```

> NOTE: you'll likely need to define both serializers and deserializers for your own
> non-json data types

Note the `add_to_cache` option on the `app.topic()`, which means the topic will be
ignored during topic creation.

We simply pass the `my_groupby_settings` to the `.groupby()` as a `config_topic` and 
those will be the settings used by the `GroupBy` topic.