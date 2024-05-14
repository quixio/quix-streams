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

With Kafka, rather than compiled tables of data, we have messages on topics that are 
individually read and independently processed. 

So how is `GroupBy` useful in the context of Kafka?


### How Data is Organized in Kafka

To understand `GroupBy`, it's important to understand Kafka message keys and topic partitioning.

As a lightning quick summary, messages are sorted on topics based on their message keys: if
a message has key `X`, then all messages that also have key `X` will always end up on the same topic partition. This means
you have guarantees around message ordering for that key with respect to itself (but NOT to other keys!).

> NOTE: Understanding keys is also very important for [stateful operations with Quix Streams](./advanced/stateful-processing.md)

### Regrouping By Changing Keys

A `GroupBy` for Kafka simply repartitions messages by changing their message keys based
on some aspect of their message value.

This enables stateful aggregations similar to our `SUM()` example above.

In this case, it's easier to understand with an example, as seen below.


## Revisiting SQL "GroupBy" Example, with Kafka

### Data as Kafka Messages

Imagine you have the same data from the [SQL example above](#a-sql-groupby-example), only now each row of the table is 
instead a Kafka message.

The message key is `store_id`, and the `item` and `quantity` columns are in the message value:

```python
message_1 = {"key": "store_4", "value": {"item": "A", "quantity": 5}}
# ...etc...
message_5 = {"key": "store_2", "value": {"item": "B", "quantity": 1}}
```

### Problematic Message Keys

Like before, we want the sum of each quantity per item. Unfortunately, the results won't be what you expect with the current message format.

If you had two consumers, one consumer might process all messages related to `store_4`, while the other processes `store_2` (due to the 
way Kafka distributes partitions across consumers).

Basically, the totals are **actually**:

- _total quantity per store_

which is NOT the desired:

-  _total quantity per item_

### Regrouping the Messages

To fix this, simply repartition the messages so that the `item` name is the message key:

```python
message_1 = {"key": "A", "value": {"item": "A", "quantity": 5}}
# ...etc...
message_5 = {"key": "B", "value": {"item": "B", "quantity": 1}}
```

This part can be easily accomplished using `StreamingDataFrame.group_by()`.

## StreamingDataFrame.group_by()

### What it Does

`StreamingDataFrame.group_by()` allows you to seamlessly regroup messages during 
processing. Basically, ***message keys can be changed within the same application!***

> Note: All downstream `StreamingDataFrame` operations will use the updated keys!

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

### Typical Patterns with .group_by()

While it can be used just to re-key a topic, `.group_by()` is most often used for aggregation, and thus often 
followed by some sort of [stateful operation](advanced/stateful-processing.md) or [windowing](windowing.md).

It is recommended to learn about those operations; you can also see how they are commonly 
used with the following examples below.

## GroupBy Examples

Assume we are a retail store chain, and our Kafka messages are orders from our various store locations.

Our keys are the `store_id`s, and (the same) `store_id`, `item`, and `quantity` columns are in the message value:

```python
# Kafka Messages
{"key": "store_4", "value": {"store_id": "store_4", "item": "A", "quantity": 5}}
# ...etc...
{"key": "store_2", "value": {"store_id": "store_2", "item": "B", "quantity": 1}}
```

Assume we are in charge of generating some real-time statistics for our company.


### Single Column GroupBy with Aggregation

Imagine we are tasked with getting the **total** `quantity` of each `item` ordered (regardless of
what `store_id` it came from).

In this case, we need to get totals based on a single column identifier: `item`.

This can be done by simply passing the `item` column name to `.groupby()`, followed by
a [stateful aggregation](advanced/stateful-processing.md):

```python
def calculate_total(message, state):
    current_total = state.get("item_total", 0)
    current_total += int(message["quantity"])
    state.set("item_total", current_total)
    return current_total

sdf = StreamingDataFrame()
sdf = sdf.group_by("item")
sdf["total_quantity"] = sdf.apply(calculate_total, stateful=True)
sdf = sdf[["total_quantity"]]
```

which generates data like:

```python
{"key": "A", "value": {"total_quantity": 32}}
# ...etc...
{"key": "B", "value": {"total_quantity": 17}}
{"key": "A", "value": {"total_quantity": 35}}
# ...etc...
```

### Custom GroupBy (multi-column) with Aggregation

Imagine we are tasked with getting the **total** `quantity` of each `item` ordered ***per*** `store_id`.

Here, we need to be careful: right now our data is only "grouped" by the `store_id`. We need
to do a multi-column groupby to achieve this.

This can be done by simply passing a custom key generating function to `.group_by()` that 
concatenates the two field values, creating a unique key combination for them:

```python
def calculate_total(message, state):
    current_total = state.get("item_total", 0)
    current_total += int(message["quantity"])
    state.set("item_total", current_total)
    return current_total

def groupby_store_and_item(message):
    return message["store_id"] + "--" + message["item"]

sdf = StreamingDataFrame()
sdf = sdf.group_by(key=groupby_store_and_item, name="store_item_gb")
sdf["total_quantity"] = sdf.apply(calculate_total, stateful=True)
sdf = sdf[["total_quantity"]]
```

Of course, we follow the `.groupby()` with a [stateful aggregation](advanced/stateful-processing.md).

>***NOTE:*** a `name` is required for a custom `.groupby()` function, as seen here!

Together, this generates data like:

```python
{"key": "store_2--A", "value": {"total_quantity": 11}}
{"key": "store_4--A", "value": {"total_quantity": 13}}
# ...etc...
{"key": "store_4--B", "value": {"total_quantity": 9}}
{"key": "store_2--A", "value": {"total_quantity": 20}}
# ...etc...
```

### Single Column GroupBy with Windowing

Imagine we are tasked with getting the **total** `quantity` of each `item` ordered (regardless of
what `store_id` it came from) ***over the past hour***.

In this case, we need to get a windowed sum based on a single column identifier: `item`.

This can be done by simply passing the `item` column name to `.groupby()`, followed by 
a [`tumbling_window()`](windowing.md#tumbling-windows) [`.sum()`](windowing.md#min-max-mean-and-sum) over the past `3600` seconds:

```python
sdf = StreamingDataFrame()
sdf = sdf.group_by("item")
sdf = sdf.tumbling_window(duration_ms=3600).sum().final()
sdf = sdf.apply(lambda window_result: {"total_quantity": window_result["value"]})
```
which generates data like:

```python
{"key": "A", "value": {"total_quantity": 9}}
# ...etc...
{"key": "B", "value": {"total_quantity": 4}}
# ...etc...
```

>***NOTE***: refer to the [windowing documentation](windowing.md) to learn more about window results, including 
> their output format and when they are generated.

## Advanced Usage and Details

Here is some supplemental information around how 
`StreamingDataFrame.group_by()` works and any advanced configuration details.

### GroupBy limitations

`GroupBy` is limited to one use per `StreamingDataFrame`.

### How GroupBy works
Each `GroupBy` operation is facilitated by a unique, internally managed 
"repartition" topic. By default, its settings are inherited from its origin topic and 
is created automatically.

The `Application` automatically subscribes to it and knows where in the `StreamingDataFrame` 
pipeline to start from based on a consumed message's topic origin.

### Message Processing Order

Because `GroupBy` uses an internal topic, there are three (related) side effects to be aware of: 

- message processing will likely begin on one application instance and finish on another
- messages will likely finish processing in a different order than they arrived
  >NOTE: remember in Kafka, message ordering is only guaranteed per key.
- The regrouped message may be processed much later than its pre-grouped counterpart

### Configuring the Internal Topic

Though you cannot configure the internal `GroupBy` Kafka configuration, 
you _can_ provide your own serializers in case the `JSON` defaults are inappropriate 
(should be rare if using typical `StreamingDataFrame` features).

Here is an example of doing so:

```python
from quixstreams import Application

sdf = app.dataframe(input_topic)
sdf = sdf.group_by(
    "my_col_name",
    value_deserializer="int",
    key_deserializer="string",
    value_serializer="int",
    key_serializer="string"
)
```
