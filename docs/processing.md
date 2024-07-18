# Processing & Transforming Data

## Introduction to StreamingDataFrame

`StreamingDataFrame` is the primary interface to define the stream processing pipelines
in Quix Streams.

With `StreamingDataFrame`, you can:

- Transform data from one format to another.
- Update data in-place.
- Filter data.
- Access and modify data similarly to pandas DataFrames.
- Apply custom functions (and access State store inside them).
- Perform windowed aggregations.
- Produce data to the output topic.

### How It Works

`StreamingDataFrame` is a lazy object.  
You may think of it as a pipeline containing all transformations for incoming messages.

It doesn't store the actual consumed data in memory, but only declares how the data
should be transformed.

All changes to `StreamingDataFrame` objects update the processing pipeline, and each
operation on it only adds a new step to the pipeline.

Most StreamingDataFrame API methods return new `StreamingDataFrame` objects with updated
pipeline, so you can chain them together.

To run it, pass the `StreamingDataFrame` object to the `Application.run()` method.

**Example:**

Let's assume you have a temperature sensor producing readings in
Fahrenheit to the `temperature` topic , and you need to convert them to Celsius, and
publish results to the output topic.

Message format:

```json
{
  "tempF": 68
}
```

Here is how it can be done with `StreamingDataFrame`:

```python
from quixstreams import Application

# Define an application and input topic with JSON deserialization
app = Application(broker_address='localhost:9092')
input_topic = app.topic('temperature', value_deserializer='json')
output_topic = app.topic('temperature-celsius', value_deserializer='json')

# Create StreamingDataFrame and connect it to the input topic 
sdf = app.dataframe(topic=input_topic)

sdf = (
    # Convert the temperature value from °F to °C
    # E.g. {"tempF": 68} will become {"tempC": 20}
    sdf.apply(lambda value: {'tempC': (value['tempF'] - 32) * 5 / 9})

    # Print the result to the console
    .update(print)
)

# Publish data to the output topic
sdf = sdf.to_topic(output_topic)

# Run the pipeline
app.run(sdf)
```

### Data Types

`StreamingDataFrame` itself is agnostic of data types passed to it during processing,
and it can work with any Python type.

The serialization and deserialization of the messages
is handled by the `Topic` objects according to
the settings passed to `app.topic()`.

**Example:**

```python
from quixstreams import Application

app = Application(broker_address='localhost:9092')

# Input topic will deserialize incoming messages from JSON bytes
input_topic = app.topic('input', value_deserializer='json')

# Output topic will serialize outgoing message to JSON bytes
output_topic = app.topic('input', value_serializer='json')
```

## Selecting Columns

Quix Streams provides a pandas-like API to select columns from incoming messages (AKA "to make a projection").  

You may use it to extract only the necessary columns and reduce the size of the message before sending it downstream.


**Example**:

In this example, assume you receive temperature readings in the following format:

```json
{
  "temperature": 35.5,
  "timestamp": 1710865771.3750699,
  "metadata": {
    "sensor_id": "sensor-1"
  }
} 
```

and you only need "temperature" and "timestamp" columns for processing:

```json
{
  "temperature": 35.5,
  "timestamp": 1710865771.3750699
}
```

Here is how to do that with `StreamingDataFrame`:

```python
sdf = app.dataframe(...)
# Selecting only "temperature" and "timestamp" columns using pandas-like approach
sdf = sdf[["temperature", "timestamp"]]

# The same can be done using .apply() with a custom function
sdf = sdf.apply(lambda value: {'temperature': value['temperature'],
                               'timestamp': value['timestamp']})
```

> **_NOTE:_**  The pandas-like approach works only with mapping-like values like dictionaries.  
> To make projection on top of non-mapping values (like custom objects), use
> the `.apply()` approach.

## Dropping Columns
Similarly to projections, you may drop unnecessary columns from incoming records using a `StreamingDataFrame.drop()` method.

It accepts either one column name as a string or a list of names.

The `.drop()` method updates the existing `StreamingDataFrame` object and returns the same `StreamingDataFrame` instance so that you can chain other methods after the `drop()` call, too.

Internally, it mutates the record's value and deletes the keys in place.

**Example**:

In this example, assume you receive temperature readings in the following format:

```json
{
  "temperature": 35.5,
  "timestamp": 1710865771.3750699,
  "metadata": {
    "sensor_id": "sensor-1"
  }
} 
```

and you need to drop a "metadata" key from the record:

```json
{
  "temperature": 35.5,
  "timestamp": 1710865771.3750699
}
```

Here is how to do that with `StreamingDataFrame`:

```python
sdf = app.dataframe(...)
# Dropping the "metadata" key from the record's value assuming it's a dictionary
sdf.drop("metadata")

# You may also drop multiple keys by providing a list of names:
sdf.drop(["metadata", "timestamp"])
```

> **_NOTE:_**  The `StreamingDataFrame.drop()` method works only with mapping-like values like dictionaries.


## Transforming Data

### Generating New Data

To generate a new value based on the current one, use the `StreamingDataFrame.apply()`
method and provide it with a custom function defining the transformation.

`StreamingDataFrame.apply()` accepts a callback and provides the current value as a
first argument.

Note that functions passed to `.apply()` should always return the result, and it will be
sent to downstream transformations.

**Example**:

Imagine you receive data in tabular format, and you need to convert it to a plain
structure.

```python
# Input:
{"columns": ["temperature", "timestamp"], "values": [35.5, 1710865771.3750699]}

# Output:
{"temperature": 35.5, "timestamp": 1710865771.3750699}
```

Here is how to do it with `StreamingDataFrame.apply()`:

```python
sdf = app.dataframe(...)

# Convert tabular data to dictionaries
sdf = sdf.apply(
    lambda data: {column: value for column, value in
                  zip(data['columns'], data['values'])}
)
```

### Mutating Data In-Place

There are two primary ways to update data in-place with StreamingDataFrame:

1. Assigning and updating columns using DataFrame API.  
   Use it if the message value is decoded as dictionary.

2. Using `StreamingDataFrame.update()` with a custom function.
   Use this approach if the values are not dictionaries or when you need to perform a more
   complex function.

**Example**:

Assume you receive the same temperature readings in Celsius, but this time you need to add
a new column with values in Fahrenheit.

Here is how you can do that using columns and DataFrame API:
> **_NOTE:_**  This approach works only with dict-like values.

```python
sdf = app.dataframe(...)

# Add a new column with temperature in Fahrenheit
# Input: {'temperature': 9}
sdf['temperatureF'] = (sdf['temperature'] * 9 / 5) + 32
# Output: {'temperature': 9, 'temperatureF': 48.2}

# The same can be done by assigning columns and .apply()
# Note: here function passed to .apply() will only get "temperature" as an argument
sdf['temperatureF'] = sdf['temperature'].apply(lambda temp: (temp * 9 / 5) + 32)

# You can also assign the result of StreamingDataFrame.apply() to a column
sdf['temperatureF'] = sdf.apply(lambda value: (value['temperature'] * 9 / 5) + 32)
```

The similar can be done by using `StreamingDataFrame.update()`.

Similarly to `StreamingDataFrame.apply()`, `StreamingDataFrame.update()` accepts a
callback and provides the current value to it as a first argument.

But this callback is supposed to mutate data in-place, so its return will be ignored by
the downstream StreamingDataFrame.

```python

sdf = app.dataframe(...)


def add_fahrenheit(value):
    """
    Add a new column with temperature in Fahrenheit
    
    Note that this function doesn't return anything and only mutates the incoming value
    """
    value['temperatureF'] = (value['temperature'] * 9 / 5) + 32


# Input: {'temperature': 9}
sdf = sdf.update(add_fahrenheit)
# Output: {'temperature': 9, 'temperatureF': 48.2}
```

## Filtering Data

To filter data with `StreamingDataFrame`, you may use:

- DataFrame API with conditional expressions on columns.  
  Use it if the message value is decode to a dictionary.

- Custom functions using `sdf.filter(...)`
  Use this approach if the value is not a dictionary, or you need to perform a more
  complex function.

When the value is filtered from the stream, all the downstream operations for that value
are now skipped.

**Example**:

Imagine you process the temperature readings, and you are only interested in numbers
higher than a certain threshold.

Here is how to filter these values with DataFrame API:

```python
sdf = app.dataframe(...)

# Filter only values with temperature higher than 60 degrees.
sdf = sdf[sdf['temperature'] > 60]

# The same can be done with .apply() and a custom function
sdf = sdf[sdf['temperature'].apply(lambda temp: temp > 60)]
# Or
sdf = sdf[sdf.apply(lambda value: value['temperature'] > 60)]

# Multiple conditions can also be combined using binary operators 
sdf = sdf[(sdf['temperature'] > 60) & (sdf['country'] == 'US')]
# Or
sdf = sdf[
    (sdf['temperature'] > 60)
    & sdf['country'].apply(lambda country: country.lower() == 'US')
    ]
```

You can achieve the same result by using `StreamingDataFrame.filter()`.

`StreamingDataFrame.filter()` accepts a callback and provides the current value to it as
a first argument.
If the result of the callback is `False`, the value will be filtered out from the
stream.

```python
sdf = sdf.filter(lambda value: value['temperature'] > 60)
```

## Writing Data to Kafka Topics

To publish the current value of the `StreamingDataFrame` to a topic, simply call
`StreamingDataFrame.to_topic(<Topic>)` with a `Topic` instance generated
from `Application.topic()`
as an argument.

Similarly to other methods, `.to_topic()` creates a new `StreamingDataFrame` object.

### Changing Message Key Before Producing

To change the outgoing message key (which defaults to the current consumed key),
you can optionally provide a `key` callback generating a new key.

This callback can use the current value and must return a new message key.

The returned key must be compatible with `key_serializer` provided to the `Topic`object.

**Example:**

Imagine you get temperature readings from multiple sensors in multiple locations.   
Each message uses a sensor ID as a message key, but you want them to use location ID
instead to do aggregations downstream.

Message format:

```json
{
  "temperature": 35,
  "location_id": "location-1"
}
```

Here is how you can produce messages to the output topics:

```python
from quixstreams import Application

app = Application(broker_address='localhost:9092', consumer_group='consumer')

# Define an input topic and deserialize message keys to strings
input_topic = app.topic("input", key_deserializer='str')

# Define an output topic and serialize keys from strings to bytes
output_topic = app.topic("output", key_serializer='str')

sdf = app.dataframe(input_topic)

# Publish the consumed message to the output topic with the same key (sensor ID)
sdf = sdf.to_topic(output_topic)

# Publish the consumed message to the topic, but use the location ID as a new message key 
sdf = sdf.to_topic(output_topic, key=lambda value: str(value["location_id"]))
```

### Timestamps and Headers of Produced Messages

Since version 2.6, the `StreamingDataFrame.to_topic()` method always forwards the current timestamp and message headers to the output topics.

This way, **the outgoing messages will be produced with the identical timestamps and headers as they were received with** by default. 

To change the timestamp of the message, use the `StreamingDataFrame.set_timestamp()` API, described in [this section](#updating-timestamps).

## Using Columns and DataFrame API

`StreamingDataFrame` class provides rich API to access and combine individual columns,
similar to `pandas.DataFrame`.

With this API, you can:

- Update and set new columns
- Do boolean and math operations on columns
- Filter data based on column values
- Apply custom functions to individual columns

> ***NOTE***: DataFrame API works only with mapping-like values like dictionaries.  
> If the stream values are not dict-like, you may
> use [custom functions](#using-custom-functions) instead to transform and filter the
> data.

**Examples:**

Here are some examples of the column operations:

```python
sdf = app.dataframe(...)

# Input: {"total": 3, "count" 2}

# Use columns to filter data based on condition
sdf = sdf[(sdf['total'] > 0) | (sdf['count'] > 1)]

# Calculate a new value using keys "total" and "count" 
# and set it back to the value as "average"
sdf['average'] = sdf["total"] / sdf["count"]

# Apply a custom function to a column
# The function will receive "average" column value as an argument
sdf['average_as_string'] = sdf['average'].apply(lambda avg: f"Average: {avg}")

# Check if "average" is null
sdf['average_is_null'] = sdf["average"].isnull()
```

### How it works

Under the good, when you access a column on `StreamingDataFrame` it generates the new `StreamingSeries` instance that refers to the value of the passed key.

These objects are also lazy, and they are evaluated only when the `StreamingDataFrame`is
executed by `app.run(sdf)`.

When you set them back to the StreamingDataFrame or use them to filter data, it creates
a new step in the pipeline to be evaluated later.

We will go over the individual use cases in the next chapters.

## Using Custom Functions

`StreamingDataFrame` provides a flexible mechanism to transform and filter data using
custom functions and `.apply()`, `.update()` and `.filter()` methods.

These methods accept two arguments:

1. A function to apply.
   A stateless function should accept only one argument - the value.  
   A stateful function should accept two arguments - the value and `State`.

2. A `stateful` flag which can be `True` or `False` (default - `False`).  
   By passing `stateful=True`, you inform a `StreamingDataFrame` to pass an extra
   argument of type `State` to your function
   to perform stateful operations.

### StreamingDataFrame.apply()

Use `.apply()` when you need to generate a new value based on the input.  
When using `.apply()`, the result of the callback will be sent to downstream operations
as an input.

Although `.apply()` can mutate the input, it's discouraged, and `.update()` method
should be used for mutations instead.

**Example:**

```python
# Return a new value based on input
sdf = sdf.apply(lambda value: value + 1)
```

There are 2 other use cases for `.apply()`:

1. `StreamingDataFrame.apply()` can be used to assign new columns if the value is a
   dictionary:

    ```python
    # Calculate an average value of some metric using "sum" and "count" columns
    sdf['average'] = sdf.apply(lambda value: value['sum'] / value['count'])
    ```

2. `StreamingDataFrame.apply()` can be used to filter values.
   In this case, the result of the passed function is interpreted as boolean.   
   If it is `False`, the value will be filtered out from the stream:

    ```python
    # Filter values where sum of "field_b" and "field_c" is greater than 0
    sdf = sdf[sdf.apply(lambda value: (value['field_b'] + value['field_c']) > 0)]
    ```

#### Expanding collections into items

`StreamingDataFrame.apply()` with `expand=True` will expand the collection  (e.g. list
or tuple) into individual values, so the
next operations in `StreamingDataFrame` will work with individual items from this list
instead of the whole list.

For example, you get a sentence, and you need to apply transformations to individual
words and produce them:

```python
# Split a sentence into words
sdf = sdf.apply(lambda sentence: sentence.split(' '), expand=True)
# Get the length of each word
sdf = sdf.apply(lambda word: len(word))
```

After using `StreamingDataFrame.apply(expand=True)`, each downstream function will be
applied
to the item of the returned iterable.
<br/>
The items will be processed in the same order as they are returned.

There are certain limitations of this API:

- `StreamingDataFrame.apply(expand=True)` cannot be used to filter values
  using `sdf[sdf.apply(<function>, expand=True)]`
- `StreamingDataFrame.apply(expand=True)` cannot be set back to the `StreamingDataFrame`
  using `sdf['column'] = sdf[sdf.apply(<function>, expand=True)]`

### StreamingDataFrame.update()

Use `.update()` when you need to mutate the input value in-place or to perform a side
effect without changing the value.
For example, to log input data, or to update a counter in the State.

The return of the callback passed to `.update()` will be ignored, and the original input
will be sent to downstream operations instead.

This operation occurs in-place, meaning reassigning the operation to your `sdf` is 
entirely OPTIONAL; the original `StreamingDataFrame` is still returned to allow the 
chaining of commands like `sdf.update().print()`.

> Note: chains that include any non-inplace function will still require reassignment: 
> `sdf = sdf.update().filter().print()`

**Example:**

```python
# Mutate a list by appending a new item to it
# The updated list will be passed downstream
sdf = sdf.update(lambda some_list: some_list.append(1))

# OR instead (no reassignment):
sdf.update(lambda some_list: some_list.append(1))

```

### StreamingDataFrame.filter()

Use `.filter()` to filter values based on entire message content.

The result of the callback passed to `.filter()` is interpreted as boolean.
If it is `False`, the value will be filtered out from the stream.

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

## Debugging

To debug code in `StreamingDataFrame`, you can use the usual tools like prints, logging
and breakpoints.

**Example 1**:

Using `StreamingDataFrame.print()` to print the current record's value and metadata in the stream:

```python
sdf = app.dataframe(...)
# some SDF transformations happening here ...  

# Print the current record's value, key, timestamp and headers 
sdf.print(metadata=True)
# It will print the record's data wrapped into a dict for readability:
# { 'value': {'number': 12183},
#   'key': b'key',
#   'timestamp': 1721129697951,
#   'headers': [('header_name', b'header-value')]
#   }
```


**Example 2**:

Here is how to use `StreamingDataFrame.update()` to set a breakpoint and examine the
value between operations:

```python
import pdb

sdf = app.dataframe(...)
# some SDF transformations happening here ...  

# Set a breakpoint
sdf.update(lambda value: pdb.set_trace())
```

## Updating Kafka Timestamps

In Quix Streams, each processed item has a timestamp assigned.   
These timestamps are used in [windowed aggregations](./windowing.md) and when producing messages to the output topics. 

To update the current timestamp, use the `StreamingDataFrame.set_timestamp()` API. 

There are several things to note:

- The new timestamp will be used by the `StreamingDataFrame.to_topic()` method when the message is produced (see [here](#timestamps-of-produced-messages)).
- The timestamp will also determine the applicable window in the windowed aggregations.

**Example:**

```python
import time

sdf = app.dataframe(...)

# Update the timestamp to be the current epoch using "set_timestamp" 
# method with a callback.
# The callback receives four positional arguments: value, key, current timestamp, and headers. 
# It must return a new timestamp as integer in milliseconds as well

sdf = sdf.set_timestamp(lambda value, key, timestamp, headers: int(time.time() * 1000))

```

## Updating Kafka Headers

Kafka headers are key-value pairs that can provide additional information about a message without modifying its payload.  

The message headers are represented as lists of ("header", "value") tuples, where "header" is a string, and "value" is bytes.  
The same header can have multiple values.

Example of the headers:
```[('clientID', b'client-123'), ('source', b'source-123')]```


To set or update the Kafka message headers, use the `StreamingDataFrame.set_headers()` API. 

The updated headers will be attached when producing messages to the output topics.

**Example:**

```python
sdf = app.dataframe(...)

APP_VERSION = "v0.1.1"

# Add the value of APP_VERSION to the message headers for debugging purposes.  
# The callback receives four positional arguments: value, key, current timestamp, and headers. 
# It must return a new set of headers as a list of (header, value) tuples.

sdf = sdf.set_headers(
    lambda value, key, timestamp, headers: [('APP_VERSION', APP_VERSION.encode())]
)
```


## Accessing Kafka Keys, Timestamps and Headers
By leveraging the power of custom functions in `apply()`, `update()`, and `filter()` methods of `StreamingDataFrame`, you can conveniently access message keys, timestamps and headers of the records.

To get a key and a timestamp in your callback, you need to pass an additional keyword-only parameter, `metadata=True.`


> **_NOTE:_** You should never mutate keys during processing.  
> If you need to change a message key during processing, use the `StreamingDataFrame.group_by()` method described [here](./groupby.md). 

**Examples:**

```python
# Using a message key to filter data with invalid keys
sdf = sdf.filter(lambda value, key, timestamp, headers: key != b'INVALID', metadata=True)

# Assigning a message timestamp to the value as a new column
sdf['timestamp'] = sdf.apply(lambda value, key, timestamp, headers: timestamp, metadata=True)
```

## Accessing Topic Name, Partition and Offset


To access other metadata for the current Kafka message, like a topic name, partition, or offset, you may use the `message_context()` function
from the `quixstreams` module.  
This function returns an instance of `MessageContext` with message-specific fields.

The `MessageContext` is stored as a context variable via the [`contextvars`](https://docs.python.org/3/library/contextvars.html) module.
It's updated on each incoming message.

The `quixstreams.message_context()` function should be called
only from the custom functions during processing.

> **_NOTE:_** Before quixstreams==2.6.0, `MessageContext` also provided access to message keys and timestamps.  
> To access keys and timestamps in `quixstreams>=2.6`, use the approach described in this [section](#accessing-kafka-keys-anad-timestamps)


**Example:**

```python
from quixstreams import message_context

# Print the offset of the current Kafka message
sdf = sdf.update(lambda _: print('Current offset: ', message_context().offset))
```




## Using State Store

If you want to use persistent state during processing, you can access the state for a
given _message key_ by passing `stateful=True` to `StreamingDataFrame.apply()`, `StreamingDataFrame.update()`
or `StreamingDataFrame.filter()`.

In this case, your custom function should accept a second argument of type `State`.

The `State` object provides a minimal API to store key-value data:

- `.get(key, default=None)`
- `.set(key, value)`
- `.delete(key)`
- `.exists(key)`

Keys and values can be of any type as long as they are serializable to JSON (a default
serialization format for the State).  
You may easily store strings, numbers, lists, tuples and dictionaries.

Under the hood, keys are always prefixed by the actual Kafka message key to ensure
that messages with different keys don't have access to the same state.

**Example:**

Imagine you process the temperature readings, and you need to add the maximum observed
temperature to the current value:

```python
from quixstreams import State

sdf = app.dataframe(...)


def add_max_temperature(value: dict, state: State):
    """
    Calculate max observed temperature and add it to the current value
    """
    current_max = state.get('max_temperature')
    if current_max is None:
        max_temperature = value['temperature']
    else:
        max_temperature = max(value['temperature'], current_max)
    state.set('max_temperature', max_temperature)
    value['max_temperature'] = max_temperature


sdf = sdf.update(add_max_temperature, stateful=True)
```

For more information about stateful processing, see
[**Stateful Processing**](advanced/stateful-processing.md).
