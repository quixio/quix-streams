
# `StreamingDataFrame`: Detailed Overview


> ***NOTE***: If you just need to see more general/full examples, [check out our various examples](example.md).

`StreamingDataFrame` and `StreamingSeries` are the primary objects to define the stream processing pipelines.

Changes to instances of `StreamingDataFrame` and `StreamingSeries` update the processing pipeline, but the actual
data changes happen only when it's executed via `Application.run()`

If you just need to see more general/full examples, [check out our various examples](example.md).

## Data Types

`StreamingDataFrame` is agnostic of data types passed to it during processing.

All functions passed to `StreamingDataFrame` will receive data in the same format as it's deserialized
by the `Topic` object.

It can also produce any types back to Kafka as long as the value can be serialized
to bytes by `value_serializer` passed to the output `Topic` object.

The column access like `dataframe["column"]` is supported only for dictionaries.

<br>

## Accessing Fields via StreamingSeries

In typical Pandas dataframe fashion, you can access a column:

```python
sdf["field_a"]  # returns a StreamingSeries with value from field "field_a"
```

Typically, this is done in combination with other operations.

You can also access nested objects (dicts, lists, etc.):

```python
sdf["field_c"][2]  # returns a StreamingSeries with value of "field_c[2]" if "field_c" is a collection
```

<br>

## Performing Operations with StreamingSeries

You can do almost any basic operations or 
comparisons with columns, assuming typical validity of the given operations:

> ***NOTE***: No type-checking is conducted here; it is expected you know your data's field types!

```python
sdf["field_a"] + sdf["field_b"]
sdf["field_a"] / sdf["field_b"]
sdf["field_a"] | sdf["field_b"]
sdf["field_a"] & sdf["field_b"]
sdf["field_a"].isnull()
sdf["field_a"].contains("string")
sdf["field_a"] != "woo"
```


<br>

## Assigning New Fields

You may add new fields from the results of numerous other operations:

```python
# Set dictionary key "a_new_int_field" to 5
sdf["a_new_int_field"] = 5  

# Set key "a_new_str_field" to a sum of "field_a" and "field_b"
sdf["a_new_str_field"] = sdf["field_a"] + sdf["field_b"]

# Do the same but with a custom function applied to a whole message value
sdf["another_new_field"] = sdf.apply(lambda value: value['field_a'] + value['field_b'])

# Use a custom function on StreamingSeries to update key "another_new_field" 
sdf["another_new_field"] = sdf["a_new_str_field"].apply(lambda value: value + "another")
```

<br>

## Selecting Columns

In typical `pandas` fashion, you can take a subset of columns:

```python
#  Select only fields "field_a", "field_b", "field_c"
sdf = sdf[["field_a", "field_b", "field_c"]]
```

<br>

## Filtering

`StreamingDataFrame` provides a similar `pandas`-like API to filter data. 

To filter data you may use:
- Conditional expressions with `StreamingSeries` (if underlying message value is deserialized as a dictionary)
- Custom functions like `sdf[sdf.apply(lambda v: v['field'] < 0)]`
- Custom functions like `sdf = sdf.filter(lambda v: v['field'] < 0)`

When the value is filtered from the stream, ALL downstream functions for that value are now skipped,
_including Kafka-related operations like producing_.

Example:

```python
# Filter values using `StreamingSeries` expressions
sdf = sdf[(sdf["field_a"] == 'my_string') | (sdf['field_b'] > 0)]

# Filter values using `StreamingDataFrame.apply()`
sdf = sdf[sdf.apply(lambda value: value > 0)]

# Filter values using `StreamingDataFrame.filter()`
sdf = sdf.filter(lambda value: value >0)
```


## Using Custom Functions: `.apply()`, `.update()` and `.filter()`

`StreamingDataFrame` provides a flexible mechanism to transform and filter data using
simple python functions via `.apply()`, `.update()` and `.filter()` methods.

All three methods accept 2 arguments:
- A function to apply. 
A stateless function should accept only one argument - value.
A stateful function should accept only two argument - value and `State`.

- A `stateful` flag which can be `True` or `False` (default - `False`).
<br>
By passing `stateful=True`, you inform a `StreamingDataFrame` to pass an extra argument of type `State` to your function
to perform stateful operations.

Read on for more details about each method.


### `StreamingDataFrame.apply(<function>)`
Use `.apply()` when you need to generate a new value based on the input.
<br>
When using `.apply()`, the result of the function will always be propagated downstream and will become an input for the next functions.
<br>
Although `.apply()` can mutate the input, it's discouraged, and `.update()` method should be used instead.

Example:
```python
# Return a new value based on input
sdf = sdf.apply(lambda value: value + 1)
```

There are 2 other use cases for `.apply()`:
1. `StreamingDataFrame.apply()` can be used to assign new keys to the value if the value is a dictionary:
```python
# Set a key "field_a" to a sum of "field_b" and "field_c"
sdf['field_a'] = sdf.apply(lambda value: value['field_b'] + value['field_c'])
```

2. `StreamingDataFrame.apply()` can be used to filter values.
<br>
In this case, the result of the passed function is interpreted as `bool`: 
```python
# Filter values where sum of "field_b" and "field_c" is greater than 0
sdf = sdf[sdf.apply(lambda value: (value['field_b'] + value['field_c']) > 0)]
```


### `StreamingDataFrame.update(<function>)`
Use `.update()` when you need to mutate the input value in place or to perform a side effect without generating a new value.
For example, use to print data to the console or to simply update the counter in the State.

The result of a function passed to `.update()` is always ignored, and its input will be propagated downstream instead.

Examples:
```python
# Mutate a list by appending a new item to it
# The updated list will be passed downstream
sdf = sdf.update(lambda value: value.append(1))

# Use .update() to print a value to the console
sdf = sdf.update(lambda value: print("Received value: ", value))
```


### `StreamingDataFrame.filter(<function>)`
Use `.filter()` to filter values based on entire message content.
<br>
The result of a function passed to `.filter()` is interpreted as boolean.
```python
# Filter out values with "field_a" <= 0
sdf = sdf.filter(lambda value: value['field_a'] > 0)

# Filter out values where "field_a" is False  
sdf = sdf.filter(lambda value: value['field_a'])
```

You may also achieve the same result with `sdf[sdf.apply()]` syntax:
```python
# Filter out values with "field_a" <= 0 using .apply() syntax
sdf = sdf[sdf.apply(lambda value: value['field_a'] > 0)]
```

<br>

### Expanding a list of values into individual items with `StreamingDataFrame.apply(expand=True)`
`StreamingDataFrame.apply()` with `expand=True` will expand the returned iterable (e.g. list or tuple) into individual values downstream, so the 
next steps in `StreamingDataFrame` will work with individual items from this list instead of the whole list.

For example, you get a sentence, and you need to apply transformations to individual words and produce them:
```python
# Split imaginary sentences into words
sdf = sdf.apply(lambda sentence: sentence.split(' '), expand=True)
# Get the length of each word
sdf = sdf.apply(lambda word: len(word))
# Send these lengths to the output topic as separate messages
sdf = sdf.to_topic(words_topic)
```

After using `StreamingDataFrame.apply(expand=True)`, each downstream function will be applied
to the item of the returned iterable.
<br/>
The items will be processed in the same order as they are returned.

There are certain limitations coming with this API:
- `StreamingDataFrame.apply(expand=True)` cannot be used to filter values via `sdf[sdf.apply(func, expand=True)]`
- `StreamingDataFrame.apply(expand=True)` cannot be set back to the `StreamingDataFrame` via `sdf['column'] = sdf[sdf.apply(func, expand=True)]`


### Using custom functions with StreamingSeries
The `.apply()` function is also valid for `StreamingSeries`.
But instead of receiving an entire message value, it will receive only a value of the particular key:

```python
# Generate a new value based on "field_b" and assign it back to "field_a"
sdf['field_a'] = sdf['field_b'].apply(lambda field_b: field_b.strip())
```

It follows the same rules as `StreamingDataFrame.apply()`, and the result of the function
will be returned as is.

`StreamingSeries` supports only `.apply()` method.

<br>

## Stateful Processing with Custom Functions

If you want to use persistent state during processing, you can access the state for a given _message key_ via
passing `stateful=True` to `StreamingDataFrame.apply()`, `StreamingDataFrame.update()` or `StreamingDataFrame.filter()`.

In this case, your custom function should accept a second argument of type `State`.

The `State` object provides a minimal API to worked with persistent state sore:
- `.get(key, default=None)`
- `.set(key, value)`
- `.delete(key)`
- `.exists(key)`


You may treat `State` as a dictionary-like structure.
<br>
`Key` and `value` can be of any type as long as they are serializable to JSON (a default serialization format for the State).
<br>
You may easily store strings, numbers, lists, tuples and dictionaries.



Under the hood, the `key` is always prefixed by the actual Kafka message key to ensure
that messages with different keys don't have access to the same state.


```python
from quixstreams import State


# Update current value using stateful operations 

def edit_data(value, state: State):
    msg_max = len(value["field_c"])
    current_max = state.get("current_len_max")
    if current_max < msg_max:
        state.set("current_len_max", msg_max)
        current_max = msg_max
    value["len_max"] = current_max


sdf = sdf.update(edit_data, stateful=True)
```

For more information about stateful processing in general, see 
[**Stateful Applications**](./stateful-processing.md).


## Accessing the Kafka Message Keys and Metadata
`quixstreams` provides access to the metadata of the current Kafka message via `quixstreams.context` module.

Information like message key, topic, partition, offset, timestamp and more is stored globally in `MessageContext` object, 
and it's updated on each incoming message.

To get the current message key, use `quixstreams.message_key` function:

```python
from quixstreams import message_key
sdf = sdf.apply(lambda value: 1 if message_key() == b'1' else 0)
```

To get the whole `MessageContext` object with all attributes including keys, use `quixstreams.message_context`  
```python
from quixstreams import message_context

# Get current message timestamp and set it to a "timestamp" key
sdf['timestamp'] = sdf.apply(lambda value: message_context().timestamp.milliseconds)
```

Both `quixstreams.message_key()` and `quixstreams.message_context()` should be called
only from the custom functions during processing.


## Producing to Topics: `StreamingDataFrame.to_topic()`

To send the current value of the `StreamingDataFrame` to a topic, simply call 
`.to_topic(<Topic>)` with a `Topic` instance generated from `Application.topic()` 
as an argument.

To change the outgoing message key (which defaults to the current consumed key), 
you can optionally provide a key function, which operates similarly to the `.apply()`. 
<br>
It should accept a message value and return a new key.

The returned key must be compatible with `key_serializer` provided to the `Topic` object.

```python
from quixstreams import Application

app = Application(broker_address='localhost:9092', consumer_group='consumer')

# Incoming key is deserialized to string
input_topic = app.topic("input", key_deserializer='str')
# Outgoing key will be serialized as a string too
output_topic = app.topic("my_output_topic", key_serializer='str')

sdf = app.dataframe(input_topic)

# Producing a new message to a topic with the same key
sdf = sdf.to_topic(output_topic)

# Generate a new message key based on "value['field']" assuming it is a string
sdf = sdf.to_topic(output_topic, key=lambda value: str(value["field"]))
```
