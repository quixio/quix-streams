
# `StreamingDataFrame`: Detailed Overview

`StreamingDataFrame` and `Column` are the primary interface to define the stream processing pipelines.
Changes to instances of `StreamingDataFrame` and `Column` update the processing pipeline, but the actual
data changes happen only when it's executed via `Application.run()`

Example:
```python
from quixstreams import Application, MessageContext, State

# Define an application
app = Application(
   broker_address="localhost:9092",  # Kafka broker address
   consumer_group="consumer-group-name",  # Kafka consumer group
)

# Define the input and output topics. By default, the "json" serialization will be used
input_topic = app.topic("my_input_topic")
output_topic = app.topic("my_output_topic")


def add_one(data: dict, ctx: MessageContext):
    for field, value in data.items():
        if isinstance(value, int):
            data[field] += 1

            
def count(data: dict, ctx: MessageContext, state: State):
    # Get a value from state for the current Kafka message key
    total = state.get('total', default=0)
    total += 1
    # Set a value back to the state
    state.set('total')
    # Update your message data with a value from the state
    data['total'] = total

# Create a StreamingDataFrame instance
# StreamingDataFrame is a primary interface to define the message processing pipeline
sdf = app.dataframe(topic=input_topic)

# Print the incoming messages
sdf = sdf.apply(lambda value, ctx: print('Received a message:', value))

# Select fields from incoming message
sdf = sdf[["field_0", "field_2", "field_8"]]

# Filter only messages with "field_0" > 10 and "field_2" != "test"
sdf = sdf[(sdf["field_0"] > 10) & (sdf["field_2"] != "test")]

# Apply custom function to transform the message
sdf = sdf.apply(add_one)

# Apply a stateful function in persist data into the state store
sdf = sdf.apply(count, stateful=True)

# Print the result before producing it
sdf = sdf.apply(lambda value, ctx: print('Producing a message:', value))

# Produce the result to the output topic 
sdf = sdf.to_topic(output_topic)
```


## Interacting with `Rows`

Under the hood, `StreamingDataFrame` is manipulating Kafka messages via `Row` objects.

Simplified, a `Row` is effectively a dictionary of the Kafka message 
value, with each key equivalent to a dataframe column name. 

`StreamingDataFrame` interacts with `Row` objects via the Pandas-like
interface and user-defined functions passed to `StreamingDataFrame.apply()`

<br>

## Accessing Fields/Columns

In typical Pandas dataframe fashion, you can access a column:

```python
sdf["field_a"]  # "my_str"
```

Typically, this is done in combination with other operations.

You can also access nested objects (dicts, lists, etc):

```python
sdf["field_c"][2]  # 3
```

<br>

## Performing Operations with Columns

You can do almost any basic operations or 
comparisons with columns, assuming validity:

```python
sdf["field_a"] + sdf["field_b"]
sdf["field_a"] / sdf["field_b"]
sdf["field_a"] | sdf["field_b"]
sdf["field_a"] & sdf["field_b"]
sdf["field_a"].isnull()
sdf["field_a"].contains('string')
sdf["field_a"] != "woo"
```


<br>

## Assigning New Columns

You may add new columns from the results of numerous other
operations:

```python
sdf["a_new_int_field"] = 5 
sdf["a_new_str_field"] = sdf["field_a"] + sdf["field_b"]
sdf["another_new_field"] = sdf["a_new_str_field"].apply(lambda value, ctx: value + "another")
```

See [the `.apply()` section](#user-defined-functions-apply) for more information on how that works.


<br>

## Selecting only certain Columns

In typical Pandas fashion, you can take a subset of columns:

```python
# remove "field_d"
sdf = sdf[["field_a", "field_b", "field_c"]]
```

<br>

## Filtering Rows (messages)

"Filtering" is a very specific concept and operation with `StreamingDataFrames`.

In practice, it functions similarly to how you might filter rows with Pandas DataFrames
with conditionals.

When a "column" reference is actually another operation, it will be treated 
as a "filter". If that result is empty or None, the row is now "filtered".

When filtered, ALL downstream functions for that row are now skipped,
_including Kafka-related operations like producing_.

```python
# This would continue onward
sdf = sdf[sdf["field_a"] == "my_str"]

# This would filter the row, skipping further functions
sdf = sdf[(sdf["field_a"] != "woo") & (sdf["field_c"][0] > 100)]
```

<br>

## User Defined Functions: `.apply()`

Should you need more advanced transformations, `.apply()` allows you
to use any python function to operate on your row.

When used on a `StreamingDataFrame`, your function must accept 2 ordered arguments:
- a current Row value (as a dictionary)
- an instance of `MessageContext` that allows you to access other message metadata (key, partition, timestamap, etc).


Consequently, your function **MUST either** _alter this dict in-place_ 
**OR** _return a dictionary_ to directly replace the current data with.

For example:

```python
# in place example
def in_place(value: dict, ctx: MessageContext):
    value['key'] = 'value'     
            
sdf = sdf.apply(in_place)

# replacement example
def new_data(value: dict, ctx: MessageContext):
    new_value = {'key': value['key']}
    return new_value

sdf = sdf.apply(new_data)
```

<br>

The `.apply()` function is also valid for columns, but rather than providing a 
dictionary, it instead uses the column value, and the function must return a value.

```python
sdf["new_field"] = sdf["field_a"].apply(lambda value, ctx: value + "-add_me")
```

NOTE: Every `.apply()` is a _temporary_ state change, but the result can be assigned. 
So, in the above example, `field_a` remains `my_str`, but `new_field == my_str-add_me` 
as desired.

<br>

### Stateful Processing with `.apply()`

If you want to use persistent state during processing, you can access the state for a given row via
a keyword argument `stateful=True`, and your function should accept a third `State` object as
an argument (you can just call it something like `state`).

When your function has access to state, it will receive a `State` object, which can do:
- `.get(key, default=None)`
- `.set(key, value)`
- `.delete(key)`
- `.exists(key)`

`Key` and value can be anything, and you can have any number of keys.

NOTE: `key` is unrelated to the Kafka message key, which is handled behind the scenes.

```python
from quixstreams import MessageContext, State


def edit_data(row, ctx: MessageContext, state: State):
    msg_max = len(row["field_c"])
    current_max = state.get("current_len_max")
    if current_max < msg_max:
        state.set("current_len_max", msg_max)
        current_max = msg_max
    row["len_max"] = current_max
    return row


sdf = sdf.apply(edit_data, stateful=True)
```

For more information about stateful processing in general, see 
[**Stateful Applications**](./stateful_processing.md).


<br>

## Producing to Topics: `.to_topic()`

To send the current state of the `StreamingDataFrame` to a topic, simply call 
`to_topic` with a `Topic` instance generated from `Application.topic()` 
as an argument.

To change the outgoing message key (which defaults to the current consumed key), 
you can optionally provide a key function, which operates similarly to the `.apply()` 
function with a `row` (dict) and `ctx` argument, and returns a desired 
(serializable) key.

```python
from quixstreams import Application

app = Application(...)
output_topic = app.topic("my_output_topic")

# Producing a new message to a topic with the same key
sdf = sdf.to_topic(other_output_topic)

# Producing a new message to a topic with a new key
sdf = sdf.to_topic(output_topic, key=lambda value, ctx: ctx.key + value['field'])
```
