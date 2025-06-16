# Inspecting Data and Debugging

Sometimes, there is a need for more direct insight around your data processing with
a `StreamingDataFrame`, especially with specific operations.

Here are some helpful tools or strategies for inspecting your `Application` and data, 
whether for debugging or confirming things are working as expected!

## Printing Data

This is generally the simplest approach for inspecting your `Application`.

You can print the current record's value at desired points in a `StreamingDataFrame`.

### Single Record Printing

The most log-friendly approach (especially if you do not have a live terminal 
session, such as when running in Kubernetes) is `StreamingDataFrame.print()`,
which prints the current record value wherever it's called.

It's a multi-line print by default, but it can be single line with kwarg `pretty=False`.

It can additionally include the record metadata with kwarg `metadata=True`:

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


### Table Printing

A more user-friendly way to monitor your data stream is using `StreamingDataFrame.print_table()`.
It creates a live-updating table that shows the most recent records:

```python
sdf = app.dataframe(...)
# some SDF transformations happening here ...

# Show last 5 records with metadata columns
sdf.print_table(size=5, title="My Stream")

# For wide datasets, limit columns to improve readability
sdf.print_table(
    size=5,
    title="My Stream",
    columns=["id", "name", "value"],
    column_widths={"name": 20}
)
```

This will produce a live table like:

```
My Stream
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ _key       ┃ _timestamp ┃ id     ┃ name                 ┃ value   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━┩
│ b'53fe8e4' │ 1738685136 │ 876    │ Charlie              │ 42.5    │
│ b'91bde51' │ 1738685137 │ 11     │ Alice                │ 18.3    │
│ b'6617dfe' │ 1738685138 │ 133    │ Bob                  │ 73.1    │
│ b'f47ac93' │ 1738685139 │ 244    │ David                │ 55.7    │
│ b'038e524' │ 1738685140 │ 567    │ Eve                  │ 31.9    │
└────────────┴────────────┴────────┴──────────────────────┴─────────┘
```

Note that the "name" column is resized to desired width of 20 characters.

You can monitor multiple points in your pipeline by adding multiple print_table calls:

```python
sdf = app.dataframe(topic)
sdf.print_table(title="Raw Input")

sdf = sdf.filter(lambda value: ...)
sdf.print_table(title="Filtered Values")

sdf = sdf.apply(lambda value: ...)
sdf.print_table(title="Final Output")
```

## Interacting with Data

If you'd like to store or manipulate data from `Application` processing directly, you can 
do an "interactive" `Application` by executing `Application.run()` with stop conditions within an 
iPython session (terminal/Pycharm/VS Code etc.) or Jupyter Notebook cell.


Basically:

1. The `Application` runs for the specified period in the interactive session. 
2. Once the Application stops, the `run` method can return the accumulated outputs of the application for all registered dataframes.

The details of this pattern are further explained below.

### Stopping the application early and collecting the outputs

In a production setting, `Application.run()` should be called only once with no
arguments, which means it will run indefinitely (until it encounters an error or 
is manually stopped by the user).

However, for debugging, the following kwargs can be passed to `Application.run()` to stop it 
when the applicable condition is met:

- `timeout`: maximum time to wait for a new message to arrive (default `0.0` == infinite)
- `count`: a number of outputs to process across all dataframes and input topics (default `0` == infinite)
- `collect_mode`: how to collect the processed outputs to return them as a result of the `Application.run()` call.  
  This setting is effective only when either `timeout` or `count` are passed.  
  Possible values:
  - `"values"` (default) - the output values are collected into a list of dictionaries.
  - `"values-and-metadata"` - collect values, keys, timestamps, offsets, topics and partitions.
  - `"off"` - don't collect anything.

If `timeout` and `count` are passed together (which is the recommended pattern for debugging), either condition 
will trigger the stop.

**Example**: 

```python
from quixstreams import Application

app = Application(broker_address="localhost:9092")

topic = app.topic("some-topic")
# Assume the topic has one partition and three JSON messages:
#  {"temperature": 30} 
#  {"temperature": 40}
#  {"temperature": 50}

sdf = app.dataframe(topic=topic)

# Process one output and collect the value (stops if no messages for 10s)
result_values_only = app.run(count=1, timeout=10, collect_mode="values")  
# >>> result_values_only = [
#   {"temperature": 30}
# ]


# Process one output and collect the value with metadata (stops if no messages for 10s)
result_values_and_metadata = app.run(count=1, timeout=10, collect_mode="values-and-metadata")
# >>> result_values_and_metadata = [
#   {"temperature": 40, "_key": "<message_key>", "_timestamp": 123, "_offset": 1, "_topic": "some-topic", "_partition": 1, "_headers": None},
# ]


# Process one output and without collecting (stops if no messages for 10s)
result_empty = app.run(count=1, timeout=10, collect_mode="off")
# >>> result_empty = []


```

#### Count Behavior

There are a few things to be aware of with `count`:

- It counts _outputs_ processed by **all** `StreamingDataFrames`.   
Under the hood, every message may generate from 0 to N outputs as it is passing through the topology generated by `StreamingDataFrames`:
  - Operations like [filtering](processing.md#filtering-data) (e.g., `StreamingDataFrame.filter()` or `dataframe[dataframe["<field>"] == "<value>"]`) and ["final" windowed aggregations](windowing.md#emitting-after-the-window-is-closed-) reduce the number of outputs.
  - Operations like [`StreamingDataFrame.apply(..., expand=True)`](processing.md#expanding-collections-into-items) and [branching](branching.md) may increase the total number of outputs

- The total number of outputs may be higher than the passed `count` parameter because every message is processed fully before stopping the app.
- After the count is reached, the `Application` stops and returns the accumulated outputs according to the `collection_mode` setting.


#### Timeout Behavior

A couple of things to note about `timeout`:

- Though it can be used standalone, it's recommended to be paired with a `count`.

- Tracking starts once the first partition assignment (or recovery, if needed) finishes.
  - There is a 60s wait buffer for the first assignment to trigger.

- Using only `timeout` when collecting data from high-volume topics may cause out-of-memory errors when `collect_mode` is either `"values"` (default) or `"values-and-metadata"`.

#### Multiple Application.run() calls

It is safe to do subsequent `Application.run()` calls with different arguments; it will simply pick up where it left off. 

There is no need to do any manual cleanup when finished; each run cleans up after itself.

> **NOTE:** You do not need to re-execute the entire Application setup, just `.run()`.

### ListSink

`ListSink` primarily exists to use alongside `Application.run()` stop conditions.

You may use it to collect and examine data after some specific operations in the DataFrame.  
It can be interacted with like a list once the `Application` stops.

#### Using ListSink

To use a `ListSink`, simply store one in a separate variable and pass it
like any other `Sink`:

```python
from quixstreams import Application
from quixstreams.sinks.core.list import ListSink

app = Application(broker_address="localhost:9092")
topic = app.topic("some-topic")
list_sink = ListSink()  # sink will be a list-like object
sdf = app.dataframe(topic=topic).sink(list_sink)
app.run(count=50, timeout=10)  # get up to 50 records (stops if no messages for 10s)
```

You can then interact with it once the `Application` stops:

```pycon
>>> print(list_sink) 
[{"thing": "x"}, {"thing": "y"}, {"thing": "z"}]

>>> list_sink[0]
{"thing": "x"}
```

> **NOTE:** Though the result is actually a `ListSink`, it behaves like a list...you can even pass it 
to `Pandas.DataFrame()` and it will work as expected!

#### Including Message Metadata

To include Kafka message data beyond fields contained in `value`, do `ListSink(metadata=True)`, 
which will include all the Kafka message parameters like `key`, `timestamp`, etc. 
as additional fields as `_{param}`, like so:

```shell
> list_sink[0]
{"thing": "x", "_key": "id-123", "_timestamp": 1234567890, "_topic": "some-topic", ... }
```

#### ListSink Limitations

- You can use any number of `ListSink` in an `Application`
  - each one must have its own variable.

- `ListLink` does not limit its own size 
  - Be sure to use it with `Application.run()` stopping conditions.

- `ListSink` does not "refresh" itself per `.run()`; it collects data indefinitely. 
  - You can remove the current data stored by doing `list_sink.clear()`.

### Interactive limitations

Currently, Quix Streams `Source` functionality is limited due 
to `multiprocessing` `if __name__ == '__main__':` limitations; specifically:

- You cannot run any source-based `Application` "interactively" 
(i.e. using ListSink to inspect values)
  - There is potentially a way around this for [Jupiter Notebooks](connectors/sources/custom-sources.md#custom-sources-and-jupyter-notebook)
- You can only run a plain source (no SDF) with the `timeout` argument.

## Using Breakpoints

For detailed examination, you can set breakpoints using `StreamingDataFrame.update()`:

```python
import pdb

sdf = app.dataframe(...)
# some SDF transformations happening here ...  

# Set a breakpoint
sdf.update(lambda value: pdb.set_trace())
```

## Application loglevel to DEBUG

Though likely not helpful for the average user, it is possible to change the
`Application` loglevel to `DEBUG` for more under-the-hood insight.

Just do `Application(loglevel="DEBUG")`.

> **NOTE**: This does NOT grant any insights into the message data directly.