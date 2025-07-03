<a id="quixstreams.dataframe.dataframe"></a>

## quixstreams.dataframe.dataframe

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame"></a>

### StreamingDataFrame

```python
class StreamingDataFrame()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L90)

`StreamingDataFrame` is the main object you will use for ETL work.

Typically created with an `app = quixstreams.app.Application()` instance,
via `sdf = app.dataframe()`.



<br>
***What it Does:***

- Builds a data processing pipeline, declaratively (not executed immediately)
- Executes this pipeline on inputs at runtime (Kafka message values)
- Provides functions/interface similar to Pandas Dataframes/Series
- Enables stateful processing (and manages everything related to it)



<br>
***How to Use:***

Define various operations while continuously reassigning to itself (or new fields).

These operations will generally transform your data, access/update state, or produce
to kafka topics.

We recommend your data structure to be "columnar" (aka a dict/JSON) in nature so
that it works with the entire interface, but simple types like `ints`, `str`, etc.
are also supported.

See the various methods and classes for more specifics, or for a deep dive into
usage, see `streamingdataframe.md` under the `docs/` folder.

>***NOTE:*** column referencing like `sdf["a_column"]` and various methods often
    create other object types (typically `quixstreams.dataframe.StreamingSeries`),
    which is expected; type hinting should alert you to any issues should you
    attempt invalid operations with said objects (however, we cannot infer whether
    an operation is valid with respect to your data!).



<br>
***Example Snippet:***

```python
sdf = StreamingDataFrame()
sdf = sdf.apply(a_func)
sdf = sdf.filter(another_func)
sdf = sdf.to_topic(topic_obj)
```

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.stream_id"></a>

<br><br>

#### StreamingDataFrame.stream\_id

```python
@property
def stream_id() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L175)

An identifier of the data stream this StreamingDataFrame
manipulates in the application.

It is used as a common prefix for state stores and group-by topics.
A new `stream_id` is set when StreamingDataFrames are merged via `.merge()`
or grouped via `.group_by()`.

StreamingDataFrames with different `stream_id` cannot access the same state stores.

By default, a topic name or a combination of topic names are used as `stream_id`.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.apply"></a>

<br><br>

#### StreamingDataFrame.apply

```python
def apply(func: Union[
    ApplyCallback,
    ApplyExpandedCallback,
    ApplyCallbackStateful,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
    ApplyWithMetadataCallbackStateful,
],
          *,
          stateful: bool = False,
          expand: bool = False,
          metadata: bool = False) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L234)

Apply a function to transform the value and return a new value.

The result will be passed downstream as an input value.



<br>
***Example Snippet:***

```python
# This stores a string in state and capitalizes every column with a string value.
# A second apply then keeps only the string value columns (shows non-stateful).
def func(d: dict, state: State):
    value = d["store_field"]
    if value != state.get("my_store_key"):
        state.set("my_store_key") = value
    return {k: v.upper() if isinstance(v, str) else v for k, v in d.items()}

sdf = StreamingDataFrame()
sdf = sdf.apply(func, stateful=True)
sdf = sdf.apply(lambda d: {k: v for k,v in d.items() if isinstance(v, str)})

```


<br>
***Arguments:***

- `func`: a function to apply
- `stateful`: if `True`, the function will be provided with a second argument
of type `State` to perform stateful operations.
- `expand`: if True, expand the returned iterable into individual values
downstream. If returned value is not iterable, `TypeError` will be raised.
Default - `False`.
- `metadata`: if True, the callback will receive key, timestamp and headers
along with the value.
Default - `False`.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.update"></a>

<br><br>

#### StreamingDataFrame.update

```python
def update(func: Union[
    UpdateCallback,
    UpdateCallbackStateful,
    UpdateWithMetadataCallback,
    UpdateWithMetadataCallbackStateful,
],
           *,
           stateful: bool = False,
           metadata: bool = False) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L338)

Apply a function to mutate value in-place or to perform a side effect

(e.g., printing a value to the console).

The result of the function will be ignored, and the original value will be
passed downstream.

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).



<br>
***Example Snippet:***

```python
# Stores a value and mutates a list by appending a new item to it.
# Also prints to console.

def func(values: list, state: State):
    value = values[0]
    if value != state.get("my_store_key"):
        state.set("my_store_key") = value
    values.append("new_item")

sdf = StreamingDataFrame()
sdf = sdf.update(func, stateful=True)
# does not require reassigning
sdf.update(lambda v: v.append(1))
```


<br>
***Arguments:***

- `func`: function to update value
- `stateful`: if `True`, the function will be provided with a second argument
of type `State` to perform stateful operations.
- `metadata`: if True, the callback will receive key, timestamp and headers
along with the value.
Default - `False`.


<br>
***Returns:***

the updated StreamingDataFrame instance (reassignment NOT required).

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.filter"></a>

<br><br>

#### StreamingDataFrame.filter

```python
def filter(func: Union[
    FilterCallback,
    FilterCallbackStateful,
    FilterWithMetadataCallback,
    FilterWithMetadataCallbackStateful,
],
           *,
           stateful: bool = False,
           metadata: bool = False) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L441)

Filter value using provided function.

If the function returns True-like value, the original value will be
passed downstream.


<br>
***Example Snippet:***

```python
# Stores a value and allows further processing only if the value is greater than
# what was previously stored.

def func(d: dict, state: State):
    value = d["my_value"]
    if value > state.get("my_store_key"):
        state.set("my_store_key") = value
        return True
    return False

sdf = StreamingDataFrame()
sdf = sdf.filter(func, stateful=True)
```


<br>
***Arguments:***

- `func`: function to filter value
- `stateful`: if `True`, the function will be provided with second argument
of type `State` to perform stateful operations.
- `metadata`: if True, the callback will receive key, timestamp and headers
along with the value.
Default - `False`.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.group_by"></a>

<br><br>

#### StreamingDataFrame.group\_by

```python
def group_by(key: Union[str, Callable[[Any], Any]],
             name: Optional[str] = None,
             value_deserializer: DeserializerType = "json",
             key_deserializer: DeserializerType = "json",
             value_serializer: SerializerType = "json",
             key_serializer: SerializerType = "json") -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L526)

"Groups" messages by re-keying them via the provided group_by operation

on their message values.

This enables things like aggregations on messages with non-matching keys.

You can provide a column name (uses the column's value) or a custom function
to generate this new key.

`.groupby()` can only be performed once per `StreamingDataFrame` instance.

>**NOTE:** group_by generates a new topic with the `"repartition__"` prefix
    that copies the settings of original topics.


<br>
***Example Snippet:***

```python
# We have customer purchase events where the message key is the "store_id",
# but we want to calculate sales per customer (by "customer_account_id").

def func(d: dict, state: State):
    current_total = state.get("customer_sum", 0)
    new_total = current_total + d["customer_spent"]
    state.set("customer_sum", new_total)
    d["customer_total"] = new_total
    return d

sdf = StreamingDataFrame()
sdf = sdf.group_by("customer_account_id")
sdf = sdf.apply(func, stateful=True)
```


<br>
***Arguments:***

- `key`: how the new key should be generated from the message value;
requires a column name (string) or a callable that takes the message value.
- `name`: a name for the op (must be unique per group-by), required if `key`
is a custom callable.
- `value_deserializer`: a deserializer type for values; default - JSON
- `key_deserializer`: a deserializer type for keys; default - JSON
- `value_serializer`: a serializer type for values; default - JSON
- `key_serializer`: a serializer type for keys; default - JSON


<br>
***Returns:***

a clone with this operation added (assign to keep its effect).

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.contains"></a>

<br><br>

#### StreamingDataFrame.contains

```python
def contains(keys: Union[str, list[str]]) -> StreamingSeries
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L640)

Check if keys are present in the Row value.


<br>
***Example Snippet:***

```python
# Add new column 'has_column' which contains a boolean indicating
# the presence of 'column_x' and `column_y`

sdf = StreamingDataFrame()
sdf['has_column_A'] = sdf.contains('column_a')
sdf['has_column_X_Y'] = sdf.contains(['column_x', 'column_y'])
```


<br>
***Arguments:***

- `keys`: column names to check.


<br>
***Returns:***

a Column object that evaluates to True if the keys are present
or False otherwise.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.to_topic"></a>

<br><br>

#### StreamingDataFrame.to\_topic

```python
def to_topic(
        topic: Topic,
        key: Optional[Callable[[Any], Any]] = None) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L670)

Produce current value to a topic. You can optionally specify a new key.

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).


<br>
***Example Snippet:***

```python
from quixstreams import Application

# Produce to two different topics, changing the key for one of them.

app = Application()
input_topic = app.topic("input_x")
output_topic_0 = app.topic("output_a")
output_topic_1 = app.topic("output_b")

sdf = app.dataframe(input_topic)
sdf = sdf.to_topic(output_topic_0)
# does not require reassigning
sdf.to_topic(output_topic_1, key=lambda data: data["a_field"])
```


<br>
***Arguments:***

- `topic`: instance of `Topic`
- `key`: a callable to generate a new message key, optional.
If passed, the return type of this callable must be serializable
by `key_serializer` defined for this Topic object.
By default, the current message key will be used.


<br>
***Returns:***

the updated StreamingDataFrame instance (reassignment NOT required).

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.set_timestamp"></a>

<br><br>

#### StreamingDataFrame.set\_timestamp

```python
def set_timestamp(
        func: Callable[[Any, Any, int, Any], int]) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L715)

Set a new timestamp based on the current message value and its metadata.

The new timestamp will be used in windowed aggregations and when producing
messages to the output topics.

The new timestamp must be in milliseconds to conform Kafka requirements.


<br>
***Example Snippet:***

```python
from quixstreams import Application


app = Application()
input_topic = app.topic("data")

sdf = app.dataframe(input_topic)
# Updating the record's timestamp based on the value
sdf = sdf.set_timestamp(lambda value, key, timestamp, headers: value['new_timestamp'])
```


<br>
***Arguments:***

- `func`: callable accepting the current value, key, timestamp, and headers.
It's expected to return a new timestamp as integer in milliseconds.


<br>
***Returns:***

a new StreamingDataFrame instance

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.set_headers"></a>

<br><br>

#### StreamingDataFrame.set\_headers

```python
def set_headers(
    func: Callable[
        [Any, Any, int, HeadersTuples],
        HeadersTuples,
    ]
) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L758)

Set new message headers based on the current message value and metadata.

The new headers will be used when producing messages to the output topics.

The provided callback must accept value, key, timestamp, and headers,
and return a new collection of (header, value) tuples.


<br>
***Example Snippet:***

```python
from quixstreams import Application


app = Application()
input_topic = app.topic("data")

sdf = app.dataframe(input_topic)
# Updating the record's headers based on the value and metadata
sdf = sdf.set_headers(lambda value, key, timestamp, headers: [('id', value['id'])])
```


<br>
***Arguments:***

- `func`: callable accepting the current value, key, timestamp, and headers.
It's expected to return a new set of headers
as a collection of (header, value) tuples.


<br>
***Returns:***

a new StreamingDataFrame instance

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.print"></a>

<br><br>

#### StreamingDataFrame.print

```python
def print(pretty: bool = True, metadata: bool = False) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L809)

Print out the current message value (and optionally, the message metadata) to

stdout (console) (like the built-in `print` function).

Can also output a more dict-friendly format with `pretty=True`.

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

> NOTE: prints the current (edited) values, not the original values.


<br>
***Example Snippet:***

```python
from quixstreams import Application


app = Application()
input_topic = app.topic("data")

sdf = app.dataframe(input_topic)
sdf["edited_col"] = sdf["orig_col"] + "edited"
# print the updated message value with the newly added column
sdf.print()
```


<br>
***Arguments:***

- `pretty`: Whether to use "pprint" formatting, which uses new-lines and
indents for easier console reading (but might be worse for log parsing).
- `metadata`: Whether to additionally print the key, timestamp, and headers


<br>
***Returns:***

the updated StreamingDataFrame instance (reassignment NOT required).

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.print_table"></a>

<br><br>

#### StreamingDataFrame.print\_table

```python
def print_table(
        size: int = 5,
        title: Optional[str] = None,
        metadata: bool = True,
        timeout: float = 5.0,
        live: bool = DEFAULT_LIVE,
        live_slowdown: float = DEFAULT_LIVE_SLOWDOWN,
        columns: Optional[List[str]] = None,
        column_widths: Optional[dict[str,
                                     int]] = None) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L855)

Print a table with the most recent records.

This feature is experimental and subject to change in future releases.

Creates a live table view that updates in real-time as new records are processed,
showing the most recent N records in a formatted table. When metadata is enabled,
the table includes message metadata columns (_key, _timestamp) along with the
record values.

The table automatically adjusts to show all available columns unless specific
columns are requested. Missing values in any column are displayed as empty cells.
Column widths adjust automatically to fit content unless explicitly specified.

Note: Column overflow is not handled gracefully. If your data has many columns,
the table may become unreadable. Use the `columns` parameter to specify which
columns to display and/or `column_widths` to control column sizes for better
visibility.

Printing Behavior:
- Interactive mode (terminal/console): The table refreshes in-place, with new
rows appearing at the bottom and old rows being removed from the top when
the table is full.
- Non-interactive mode (output redirected to file): Collects records until
either the table is full or the timeout is reached, then prints the complete
table and starts collecting new records.

Note: This works best in terminal environments. For Jupyter notebooks,
consider using `print()` instead.

Note: The last provided live value will be used for all print_table calls
in the pipeline.

Note: The last provided live_slowdown value will be used for all print_table calls
in the pipeline.


<br>
***Example Snippet:***


This will produce a live-updating table like this:

Live Records
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━┓
┃ _key       ┃ _timestamp ┃ active ┃ id  ┃ name    ┃ score ┃ status   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━┩
│ b'53fe8e4' │ 1738685136 │ True   │ 876 │ Charlie │ 27.74 │ pending  │
│ b'91bde51' │ 1738685137 │ True   │ 11  │         │       │ approved │
│ b'6617dfe' │ 1738685138 │        │     │ David   │       │          │
│ b'f47ac93' │ 1738685139 │        │ 133 │         │       │          │
│ b'038e524' │ 1738685140 │ False  │     │         │       │          │
└────────────┴────────────┴────────┴─────┴─────────┴───────┴──────────┘


<br>
***Arguments:***

- `size`: Maximum number of records to display in the table. Default: 5
- `title`: Optional title for the table
- `metadata`: Whether to include message metadata (_key, _timestamp) columns.
Default: True
- `timeout`: Time in seconds to wait for table to fill up before printing
an incomplete table. Only relevant for non-interactive environments
(e.g. output redirected to a file). Default: 5.0
- `live`: Whether to print the table in real-time if possible.
If real-time printing is not possible, the table will be printed
in non-interactive mode. Default: True
- `live_slowdown`: Time in seconds to wait between live table updates.
Increase this value if the table updates too quickly.
Default: 0.5 seconds.
- `columns`: Optional list of columns to display. If not provided,
all columns will be displayed. Pass empty list to display only metadata.
- `column_widths`: Optional dictionary mapping column names to their desired
widths in characters. If not provided, column widths will be determined
automatically based on content. Example: {"name": 20, "id": 10}
```python
sdf = app.dataframe(topic)
# Show last 5 records, update at most every 1 second
sdf.print_table(size=5, title="Live Records", slowdown=1)
```

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.compose"></a>

<br><br>

#### StreamingDataFrame.compose

```python
def compose(sink: Optional[VoidExecutor] = None) -> dict[str, VoidExecutor]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L971)

Compose all functions of this StreamingDataFrame into one big closure.

Closures are more performant than calling all the functions in the
`StreamingDataFrame` one-by-one.

Generally not required by users; the `quixstreams.app.Application` class will
do this automatically.



<br>
***Example Snippet:***

```python
from quixstreams import Application
sdf = app.dataframe()
sdf = sdf.apply(apply_func)
sdf = sdf.filter(filter_func)
sdf = sdf.compose()

result_0 = sdf({"my": "record"})
result_1 = sdf({"other": "record"})
```


<br>
***Arguments:***

- `sink`: callable to accumulate the results of the execution, optional.


<br>
***Returns:***

a function that accepts "value"
and returns a result of StreamingDataFrame

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.test"></a>

<br><br>

#### StreamingDataFrame.test

```python
def test(value: Any,
         key: Any = b"key",
         timestamp: int = 0,
         headers: Optional[Any] = None,
         ctx: Optional[MessageContext] = None,
         topic: Optional[Topic] = None) -> List[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1005)

A shorthand to test `StreamingDataFrame` with provided value

and `MessageContext`.


<br>
***Arguments:***

- `value`: value to pass through `StreamingDataFrame`
- `key`: key to pass through `StreamingDataFrame`
- `timestamp`: timestamp to pass through `StreamingDataFrame`
- `ctx`: instance of `MessageContext`, optional.
Provide it if the StreamingDataFrame instance calls `to_topic()`,
has stateful functions or windows.
Default - `None`.
- `topic`: optionally, a topic branch to test with


<br>
***Returns:***

result of `StreamingDataFrame`

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.tumbling_window"></a>

<br><br>

#### StreamingDataFrame.tumbling\_window

```python
def tumbling_window(
    duration_ms: Union[int, timedelta],
    grace_ms: Union[int, timedelta] = 0,
    name: Optional[str] = None,
    on_late: Optional[WindowOnLateCallback] = None
) -> TumblingTimeWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1044)

Create a time-based tumbling window transformation on this StreamingDataFrame.

Tumbling windows divide time into fixed-sized, non-overlapping windows.

They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

**Notes**:

  
  - The timestamp of the aggregation result is set to the window start timestamp.
  - Every window is grouped by the current Kafka message key.
  - Messages with `None` key will be ignored.
  - The time windows always use the current event time.
  
  
  
<br>
***Example Snippet:***
  
```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a tumbling window of 60s and grace period of 10s
    sdf.tumbling_window(
        duration_ms=timedelta(seconds=60), grace_ms=timedelta(seconds=10.0)
    )

    # Specify the aggregation function
    .agg(value=agg.Sum())

    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```
  
  
<br>
***Arguments:***
  
  - `duration_ms`: The length of each window.
  Can be specified as either an `int` representing milliseconds or a
  `timedelta` object.
  >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
  value.
  - `grace_ms`: The grace period for data arrival.
  It allows late-arriving data (data arriving after the window
  has theoretically closed) to be included in the window.
  Can be specified as either an `int` representing milliseconds
  or as a `timedelta` object.
  >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
  value.
  - `name`: The unique identifier for the window. If not provided, it will be
  automatically generated based on the window's properties.
  - `on_late`: an optional callback to react on late records in windows and
  to configure the logging of such events.
  If the callback returns `True`, the message about a late record will be logged
  (default behavior).
  Otherwise, no message will be logged.
  
  
<br>
***Returns:***
  
  `TumblingTimeWindowDefinition` instance representing the tumbling window
  configuration.
  This object can be further configured with aggregation functions
  like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.tumbling_count_window"></a>

<br><br>

#### StreamingDataFrame.tumbling\_count\_window

```python
def tumbling_count_window(
        count: int,
        name: Optional[str] = None) -> TumblingCountWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1133)

Create a count-based tumbling window transformation on this StreamingDataFrame.

Tumbling windows divide messages into fixed-batch, non-overlapping windows.
They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

**Notes**:

  - The start timestamp of the aggregation result is set to the earliest timestamp.
  - The end timestamp of the aggregation result is set to the latest timestamp.
  - Every window is grouped by the current Kafka message key.
  - Messages with `None` key will be ignored.
  
  
  
<br>
***Example Snippet:***
  
```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)
sdf = (
    # Define a tumbling window of 10 messages
    sdf.tumbling_count_window(count=10)
    # Specify the aggregation function
    .agg(value=agg.Sum())
    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```
  
  
<br>
***Arguments:***
  
  - `count`: The length of each window. The number of messages to include in the window.
  - `name`: The unique identifier for the window. If not provided, it will be
  automatically generated based on the window's properties.
  
  
<br>
***Returns:***
  
  `TumblingCountWindowDefinition` instance representing the tumbling window
  configuration.
  This object can be further configured with aggregation functions
  like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.hopping_window"></a>

<br><br>

#### StreamingDataFrame.hopping\_window

```python
def hopping_window(
    duration_ms: Union[int, timedelta],
    step_ms: Union[int, timedelta],
    grace_ms: Union[int, timedelta] = 0,
    name: Optional[str] = None,
    on_late: Optional[WindowOnLateCallback] = None
) -> HoppingTimeWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1183)

Create a time-based hopping window transformation on this StreamingDataFrame.

Hopping windows divide the data stream into overlapping windows based on time.
The overlap is controlled by the `step_ms` parameter.

They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

**Notes**:

  
  - The timestamp of the aggregation result is set to the window start timestamp.
  - Every window is grouped by the current Kafka message key.
  - Messages with `None` key will be ignored.
  - The time windows always use the current event time.
  
  
  
<br>
***Example Snippet:***
  
```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a hopping window of 60s with step 30s and grace period of 10s
    sdf.hopping_window(
        duration_ms=timedelta(seconds=60),
        step_ms=timedelta(seconds=30),
        grace_ms=timedelta(seconds=10)
    )

    # Specify the aggregation function
    .agg(value=agg.Sum())

    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```
  
  
<br>
***Arguments:***
  
  - `duration_ms`: The length of each window. It defines the time span for
  which each window aggregates data.
  Can be specified as either an `int` representing milliseconds
  or a `timedelta` object.
  >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
  value.
  - `step_ms`: The step size for the window.
  It determines how much each successive window moves forward in time.
  Can be specified as either an `int` representing milliseconds
  or a `timedelta` object.
  >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
  value.
  - `grace_ms`: The grace period for data arrival.
  It allows late-arriving data to be included in the window,
  even if it arrives after the window has theoretically moved forward.
  Can be specified as either an `int` representing milliseconds
  or a `timedelta` object.
  >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
  value.
  - `name`: The unique identifier for the window. If not provided, it will be
  automatically generated based on the window's properties.
  - `on_late`: an optional callback to react on late records in windows and
  to configure the logging of such events.
  If the callback returns `True`, the message about a late record will be logged
  (default behavior).
  Otherwise, no message will be logged.
  
  
<br>
***Returns:***
  
  `HoppingTimeWindowDefinition` instance representing the hopping
  window configuration.
  This object can be further configured with aggregation functions
  like `sum`, `count`, etc. and applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.hopping_count_window"></a>

<br><br>

#### StreamingDataFrame.hopping\_count\_window

```python
def hopping_count_window(
        count: int,
        step: int,
        name: Optional[str] = None) -> HoppingCountWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1286)

Create a count-based hopping window transformation on this StreamingDataFrame.

Hopping windows divide the data stream into overlapping windows.
The overlap is controlled by the `step` parameter.
They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

**Notes**:

  - The start timestamp of the aggregation result is set to the earliest timestamp.
  - The end timestamp of the aggregation result is set to the latest timestamp.
  - Every window is grouped by the current Kafka message key.
  - Messages with `None` key will be ignored.
  
  
  
<br>
***Example Snippet:***
  
```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)
sdf = (
    # Define a hopping window of 10 messages with a step of 5 messages
    sdf.hopping_count_window(
        count=10,
        step=5,
    )
    # Specify the aggregation function
    .agg(value=agg.Sum())
    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```
  
  
<br>
***Arguments:***
  
  - `count`: The length of each window. The number of messages to include in the window.
  - `step`: The step size for the window. It determines the number of messages between windows.
  A  sliding windows is the same as a hopping window with a step of 1 message.
  - `name`: The unique identifier for the window. If not provided, it will be
  automatically generated based on the window's properties.
  
  
<br>
***Returns:***
  
  `HoppingCountWindowDefinition` instance representing the hopping
  window configuration.
  This object can be further configured with aggregation functions
  like `sum`, `count`, etc. and applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.sliding_window"></a>

<br><br>

#### StreamingDataFrame.sliding\_window

```python
def sliding_window(
    duration_ms: Union[int, timedelta],
    grace_ms: Union[int, timedelta] = 0,
    name: Optional[str] = None,
    on_late: Optional[WindowOnLateCallback] = None
) -> SlidingTimeWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1343)

Create a time-based sliding window transformation on this StreamingDataFrame.

Sliding windows continuously evaluate the stream with a fixed step of 1 ms
allowing for overlapping, but not redundant windows of a fixed size.

Sliding windows are similar to hopping windows with step_ms set to 1,
but are siginificantly more perforant.

They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

**Notes**:

  
  - The timestamp of the aggregation result is set to the window start timestamp.
  - Every window is grouped by the current Kafka message key.
  - Messages with `None` key will be ignored.
  - The time windows always use the current event time.
  - Windows are inclusive on both the start end end time.
  - Every window contains a distinct aggregation.
  
  
<br>
***Example Snippet:***
  
```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a sliding window of 60s with a grace period of 10s
    sdf.sliding_window(
        duration_ms=timedelta(seconds=60),
        grace_ms=timedelta(seconds=10)
    )

    # Specify the aggregation function
    .agg(value=agg.Sum())

    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```
  
  
<br>
***Arguments:***
  
  - `duration_ms`: The length of each window.
  Can be specified as either an `int` representing milliseconds or a
  `timedelta` object.
  >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
  value.
  - `grace_ms`: The grace period for data arrival.
  It allows late-arriving data (data arriving after the window
  has theoretically closed) to be included in the window.
  Can be specified as either an `int` representing milliseconds
  or as a `timedelta` object.
  >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
  value.
  - `name`: The unique identifier for the window. If not provided, it will be
  automatically generated based on the window's properties.
  - `on_late`: an optional callback to react on late records in windows and
  to configure the logging of such events.
  If the callback returns `True`, the message about a late record will be logged
  (default behavior).
  Otherwise, no message will be logged.
  
  
<br>
***Returns:***
  
  `SlidingTimeWindowDefinition` instance representing the sliding window
  configuration.
  This object can be further configured with aggregation functions
  like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.sliding_count_window"></a>

<br><br>

#### StreamingDataFrame.sliding\_count\_window

```python
def sliding_count_window(
        count: int,
        name: Optional[str] = None) -> SlidingCountWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1438)

Create a count-based sliding window transformation on this StreamingDataFrame.

Sliding windows continuously evaluate the stream with a fixed step of 1 message
allowing for overlapping, but not redundant windows of a fixed size.
Sliding windows are similar to hopping windows with step set to 1.
They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

**Notes**:

  - The start timestamp of the aggregation result is set to the earliest timestamp.
  - The end timestamp of the aggregation result is set to the latest timestamp.
  - Every window is grouped by the current Kafka message key.
  - Messages with `None` key will be ignored.
  - Every window contains a distinct aggregation.
  
  
  
<br>
***Example Snippet:***
  
```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)
sdf = (
    # Define a sliding window of 10 messages
    sdf.sliding_count_window(count=10)
    # Specify the aggregation function
    .sum(value=agg.Sum())
    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```
  
  
<br>
***Arguments:***
  
  - `count`: The length of each window. The number of messages to include in the window.
  - `name`: The unique identifier for the window. If not provided, it will be
  automatically generated based on the window's properties.
  
  
<br>
***Returns:***
  
  `SlidingCountWindowDefinition` instance representing the sliding window
  configuration.
  This object can be further configured with aggregation functions
  like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.fill"></a>

<br><br>

#### StreamingDataFrame.fill

```python
def fill(*columns: str, **mapping: Any) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1491)

Fill missing values in the message value with a constant value.

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

Example Snippets:

Fill missing values for a single column with a None:
```python
# This would transform {"x": 1} to {"x": 1, "y": None}
sdf.fill("y")
```

Fill missing values for multiple columns with a None:
```python
# This would transform {"x": 1} to {"x": 1, "y": None, "z": None}
sdf.fill("y", "z")
```

Fill missing values in the value with a constant value using a dictionary:
```python
# This would transform {"x": None} to {"x": 1, "y": 2}
sdf.fill(x=1, y=2)
```

Use a combination of positional and keyword arguments:
```python
# This would transform {"y": None} to {"x": None, "y": 2}
sdf.fill("x", y=2)
```


<br>
***Arguments:***

- `columns`: a list of column names as strings.
- `mapping`: a dictionary where keys are column names and values are the fill values.


<br>
***Returns:***

the original `StreamingDataFrame` instance for chaining.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.drop"></a>

<br><br>

#### StreamingDataFrame.drop

```python
def drop(columns: Union[str, List[str]],
         errors: Literal["ignore", "raise"] = "raise") -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1543)

Drop column(s) from the message value (value must support `del`, like a dict).

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).



<br>
***Example Snippet:***

```python
# Remove columns "x" and "y" from the value.
# This would transform {"x": 1, "y": 2, "z": 3} to {"z": 3}

sdf = StreamingDataFrame()
sdf.drop(["x", "y"])
```


<br>
***Arguments:***

- `columns`: a single column name or a list of names, where names are `str`
- `errors`: If "ignore", suppress error and only existing labels are dropped.
Default - `"raise"`.


<br>
***Returns:***

a new StreamingDataFrame instance

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.sink"></a>

<br><br>

#### StreamingDataFrame.sink

```python
def sink(sink: BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1587)

Sink the processed data to the specified destination.

Internally, each processed record is added to a sink, and the sinks are
flushed on each checkpoint.
The offset will be committed only if all the sinks for all topic partitions
are flushed successfully.

Additionally, Sinks may signal the backpressure to the application
(e.g., when the destination is rate-limited).
When this happens, the application will pause the corresponding topic partition
and resume again after the timeout.
The backpressure handling and timeouts are defined by the specific sinks.

Note: `sink()` is a terminal operation - it cannot receive any additional
operations, but branches can still be generated from its originating SDF.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.concat"></a>

<br><br>

#### StreamingDataFrame.concat

```python
def concat(other: "StreamingDataFrame") -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1625)

Concatenate two StreamingDataFrames together and return a new one.

The transformations applied on this new StreamingDataFrame will update data
from both origins.

Use it to concatenate dataframes belonging to different topics as well as to merge the branches
of the same original dataframe.

If concatenated dataframes belong to different topics, the stateful operations
on the new dataframe will create different state stores
unrelated to the original dataframes and topics.
The same is true for the repartition topics created by `.group_by()`.


<br>
***Arguments:***

- `other`: other StreamingDataFrame


<br>
***Returns:***

a new StreamingDataFrame

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.join_asof"></a>

<br><br>

#### StreamingDataFrame.join\_asof

```python
def join_asof(right: "StreamingDataFrame",
              how: AsOfJoinHow = "inner",
              on_merge: Union[OnOverlap, Callable[[Any, Any], Any]] = "raise",
              grace_ms: Union[int, timedelta] = timedelta(days=7),
              name: Optional[str] = None) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1661)

Join the left dataframe with the records of the right dataframe with

the same key whose timestamp is less than or equal to the left timestamp.
This join is built with the enrichment use case in mind, where the left side
represents some measurements and the right side is metadata.

To be joined, the underlying topics of the dataframes must have the same number of partitions
and use the same partitioner (all keys should be distributed across partitions using the same algorithm).

Joining dataframes belonging to the same topics (aka "self-join") is not supported as of now.

How it works:
- Records from the right side get written to the state store without emitting any updates downstream.
- Records on the left side query the right store for the values with the same **key** and the timestamp lower or equal to the record's timestamp.
Left side emits data downstream.
- If the match is found, the two records are merged together into a new one according to the `on_merge` logic.
- The size of the right store is controlled by the "grace_ms":
a newly added "right" record expires other values with the same key with timestamps below "<current timestamp> - <grace_ms>".


<br>
***Arguments:***

- `right`: a StreamingDataFrame to join with.
- `how`: the join strategy. Can be one of:
- "inner" - emit the output for the left record only when the match is found (default)
- "left" - emit the result for each left record even without matches on the right side
Default - `"inner"`.
- `on_merge`: how to merge the matched records together assuming they are dictionaries:
- "raise" - fail with an error if the same keys are found in both dictionaries
- "keep-left" - prefer the keys from the left record.
- "keep-right" - prefer the keys from the right record
- callback - a callback in form "(<left>, <right>) -> <new record>" to merge the records manually.
Use it to customize the merging logic or when one of the records is not a dictionary.
WARNING: Custom merge functions must not mutate the input values as this will lead to
inconsistencies in the state store. Always return a new object instead.
- `grace_ms`: how long to keep the right records in the store in event time.
(the time is taken from the records' timestamps).
It can be specified as either an `int` representing milliseconds or as a `timedelta` object.
The records are expired per key when the new record gets added.
Default - 7 days.
- `name`: The unique identifier of the underlying state store for the "right" dataframe.
If not provided, it will be generated based on the underlying topic names.
Provide a custom name if you need to join the same right dataframe multiple times
within the application.

**Example**:

  
```python
from datetime import timedelta
from quixstreams import Application

app = Application()

sdf_measurements = app.dataframe(app.topic("measurements"))
sdf_metadata = app.dataframe(app.topic("metadata"))

# Join records from the topic "measurements"
# with the latest effective records from the topic "metadata"
# using the "inner" join strategy and keeping the "metadata" records stored for 14 days in event time.
sdf_joined = sdf_measurements.join_asof(sdf_metadata, how="inner", grace_ms=timedelta(days=14))
```

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.join_interval"></a>

<br><br>

#### StreamingDataFrame.join\_interval

```python
def join_interval(
        right: "StreamingDataFrame",
        how: IntervalJoinHow = "inner",
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]] = "raise",
        grace_ms: Union[int, timedelta] = timedelta(days=7),
        name: Optional[str] = None,
        backward_ms: Union[int, timedelta] = 0,
        forward_ms: Union[int, timedelta] = 0) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1737)

Join the left dataframe with records from the right dataframe that fall within

specified time intervals. This join is useful for matching records that occur
within a specific time window of each other, rather than just the latest record.

To be joined, the underlying topics of the dataframes must have the same number of partitions
and use the same partitioner (all keys should be distributed across partitions using the same algorithm).

Joining dataframes belonging to the same topics (aka "self-join") is not supported.

**Notes**:

  When both `backward_ms` and `forward_ms` are set to 0 (default), the join will only match
  records with exactly the same timestamp.
  
  How it works:
  - Records from both sides are stored in the state store
  - For each record on the left side:
  - Look for matching records on the right side that fall within the specified time interval
  - If matches are found, merge the records according to the `on_merge` logic
  - For inner joins, only emit if matches are found
  - For left joins, emit even without matches
  - For each record on the right side:
  - Look for matching records on the left side that fall within the specified time interval
  - Merge all matching records according to the `on_merge` logic
  
  
<br>
***Arguments:***
  
  - `right`: a StreamingDataFrame to join with.
  - `how`: the join strategy. Can be one of:
  - "inner" - emit the output for the left record only when the match is found (default)
  - "left" - emit the result for each left record even without matches on the right side
  - "right" - emit the result for each right record even without matches on the left side
  - "outer" - emit the output for both left and right records even without matches
  Default - `"inner"`.
  - `on_merge`: how to merge the matched records together assuming they are dictionaries:
  - "raise" - fail with an error if the same keys are found in both dictionaries
  - "keep-left" - prefer the keys from the left record
  - "keep-right" - prefer the keys from the right record
  - callback - a callback in form "(<left>, <right>) -> <new record>" to merge the records manually.
  Use it to customize the merging logic or when one of the records is not a dictionary.
- `WARNING` - Custom merge functions must not mutate the input values as this will lead to
  unexpected exceptions or incorrect data in the joined stream. Always return a new object instead.
  - `grace_ms`: how long to keep records in the store in event time.
  (the time is taken from the records' timestamps).
  It can be specified as either an `int` representing milliseconds or as a `timedelta` object.
  The records are expired per key when the new record gets added.
  Default - 7 days.
  - `name`: The unique identifier of the underlying state store.
  If not provided, it will be generated based on the underlying topic names.
  Provide a custom name if you need to join the same right dataframe multiple times
  within the application.
  - `backward_ms`: How far back in time to look for matches from the right side.
  Can be specified as either an `int` representing milliseconds or as a `timedelta` object.
  Must not be greater than `grace_ms`. Default - 0.
  - `forward_ms`: How far forward in time to look for matches from the right side.
  Can be specified as either an `int` representing milliseconds or as a `timedelta` object.
  Default - 0.
  

**Example**:

  
```python
from datetime import timedelta
from quixstreams import Application

app = Application()

sdf_measurements = app.dataframe(app.topic("measurements"))
sdf_events = app.dataframe(app.topic("events"))

# Join records from the topic "measurements"
# with records from "events" that occur within a 5-minute window
# before and after each measurement
sdf_joined = sdf_measurements.join_interval(
    right=sdf_events,
    how="inner",
    on_merge="keep-left",
    grace_ms=timedelta(days=7),
    backward_ms=timedelta(minutes=5),
    forward_ms=timedelta(minutes=5)
)
```

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.join_lookup"></a>

<br><br>

#### StreamingDataFrame.join\_lookup

```python
def join_lookup(
    lookup: BaseLookup,
    fields: dict[str, BaseField],
    on: Optional[Union[str, Callable[[dict[str, Any], Any], str]]] = None
) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1842)

Note: This is an experimental feature, and its API is likely to change in the future.

Enrich the records in this StreamingDataFrame by performing a lookup join using a custom lookup strategy.

This method allows you to enrich each record in the dataframe with additional data fetched from an external
source, using a user-defined lookup strategy (subclass of BaseLookup) and a set of fields
(subclasses of BaseField) that specify how to extract or map the enrichment data.

The join is performed in-place: the input value dictionary is updated with the enrichment data.

Lookup implementation part of the standard quixstreams library:
- `quixstreams.dataframe.joins.lookups.QuixConfigurationService`


<br>
***Arguments:***

- `lookup`: An instance of a subclass of BaseLookup that implements the enrichment logic.
- `fields`: A mapping of field names to the lookup Field objects specifying how to extract or map enrichment data.
- `on`: Specifies how to determine the target key for the lookup:
- If a string, it is interpreted as the column name in the value dict to use as the lookup key.
- If a callable, it should accept (value, key) and return the target key as a string.
- If None (default), the message key is used as the lookup key.


<br>
***Returns:***

StreamingDataFrame: The same StreamingDataFrame instance with the enrichment applied in-place.

**Example**:

  
```python
from quixstreams import Application
from quixstreams.dataframe.joins.lookups import QuixConfigurationService, QuixConfigurationServiceField as Field

app = Application()

sdf = app.dataframe(app.topic("input"))
lookup = QuixConfigurationService(app.topic("config"), config=app.config)

fields = {
    "test": Field(type="test", default="test_default")
}

sdf = sdf.join_lookup(lookup, fields)
```

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.register_store"></a>

<br><br>

#### StreamingDataFrame.register\_store

```python
def register_store(store_type: Optional[StoreTypes] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1931)

Register the default store for the current stream_id in StateStoreManager.

<a id="quixstreams.dataframe.series"></a>

## quixstreams.dataframe.series

<a id="quixstreams.dataframe.series.StreamingSeries"></a>

### StreamingSeries

```python
class StreamingSeries()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L59)

`StreamingSeries` are typically generated by `StreamingDataframes` when getting
elements from, or performing certain operations on, a `StreamingDataframe`,
thus acting as a representation of "column" value.

They share some operations with the `StreamingDataframe`, but also provide some
additional functionality.

Most column value operations are handled by this class, and `StreamingSeries` can
generate other `StreamingSeries` as a result of said operations.



<br>
***What it Does:***

- Allows ways to do simple operations with dataframe "column"/dictionary values:
    - Basic ops like add, subtract, modulo, etc.
- Enables comparisons/inequalities:
    - Greater than, equals, etc.
    - and/or, is/not operations
- Can check for existence of columns in `StreamingDataFrames`
- Enables chaining of various operations together



<br>
***How to Use:***

For the most part, you may not even notice this class exists!
They will naturally be created as a result of typical `StreamingDataFrame` use.

Auto-complete should help you with valid methods and type-checking should alert
you to invalid operations between `StreamingSeries`.

In general, any typical Pands dataframe operation between columns should be valid
with `StreamingSeries`, and you shouldn't have to think about them explicitly.



<br>
***Example Snippet:***

```python
# Random methods for example purposes. More detailed explanations found under
# various methods or in the docs folder.

sdf = StreamingDataFrame()
sdf = sdf["column_a"].apply(a_func).apply(diff_func, stateful=True)
sdf["my_new_bool_field"] = sdf["column_b"].contains("this_string")
sdf["new_sum_field"] = sdf["column_c"] + sdf["column_d"] + 2
sdf = sdf[["column_a"] & (sdf["new_sum_field"] >= 10)]
```

<a id="quixstreams.dataframe.series.StreamingSeries.from_apply_callback"></a>

<br><br>

#### StreamingSeries.from\_apply\_callback

```python
@classmethod
def from_apply_callback(cls, func: ApplyWithMetadataCallback,
                        sdf_id: int) -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L125)

Create a StreamingSeries from a function.

The provided function will be wrapped into `Apply`


<br>
***Arguments:***

- `func`: a function to apply
- `sdf_id`: the id of the calling `SDF`.


<br>
***Returns:***

instance of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.apply"></a>

<br><br>

#### StreamingSeries.apply

```python
def apply(func: ApplyCallback) -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L152)

Add a callable to the execution list for this series.

The provided callable should accept a single argument, which will be its input.
The provided callable should similarly return one output, or None

They can be chained together or included with other operations.



<br>
***Example Snippet:***

```python
# The `StreamingSeries` are generated when `sdf["COLUMN_NAME"]` is called.
# This stores a string in state and capitalizes the column value; the result is
# assigned to a new column.
#  Another apply converts a str column to an int, assigning it to a new column.

def func(value: str, state: State):
    if value != state.get("my_store_key"):
        state.set("my_store_key") = value
    return v.upper()

sdf = StreamingDataFrame()
sdf["new_col"] = sdf["a_column"]["nested_dict_key"].apply(func, stateful=True)
sdf["new_col_2"] = sdf["str_col"].apply(lambda v: int(v)) + sdf["str_col2"] + 2
```


<br>
***Arguments:***

- `func`: a callable with one argument and one output


<br>
***Returns:***

a new `StreamingSeries` with the new callable added

<a id="quixstreams.dataframe.series.StreamingSeries.compose_returning"></a>

<br><br>

#### StreamingSeries.compose\_returning

```python
def compose_returning() -> ReturningExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L186)

Compose a list of functions from this StreamingSeries and its parents into one

big closure that always returns the transformed record.

This closure is to be used to execute the functions in the stream and to get
the result of the transformations.

Stream may only contain simple "apply" functions to be able to compose itself
into a returning function.


<br>
***Returns:***

a callable accepting value, key and timestamp and
returning a tuple "(value, key, timestamp)

<a id="quixstreams.dataframe.series.StreamingSeries.test"></a>

<br><br>

#### StreamingSeries.test

```python
def test(value: Any,
         key: Any,
         timestamp: int,
         headers: Optional[Any] = None,
         ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L201)

A shorthand to test `StreamingSeries` with provided value

and `MessageContext`.


<br>
***Arguments:***

- `value`: value to pass through `StreamingSeries`
- `ctx`: instance of `MessageContext`, optional.
Provide it if the StreamingSeries instance has
functions calling `get_current_key()`.
Default - `None`.


<br>
***Returns:***

result of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.isin"></a>

<br><br>

#### StreamingSeries.isin

```python
def isin(other: Container) -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L266)

Check if series value is in "other".

Same as "StreamingSeries in other".

Runtime result will be a `bool`.



<br>
***Example Snippet:***

```python
from quixstreams import Application

# Check if "str_column" is contained in a column with a list of strings and
# assign the resulting `bool` to a new column: "has_my_str".

sdf = app.dataframe()
sdf["has_my_str"] = sdf["str_column"].isin(sdf["column_with_list_of_strs"])
```


<br>
***Arguments:***

- `other`: a container to check


<br>
***Returns:***

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.contains"></a>

<br><br>

#### StreamingSeries.contains

```python
def contains(other: Union["StreamingSeries", object]) -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L297)

Check if series value contains "other"

Same as "other in StreamingSeries".

Runtime result will be a `bool`.



<br>
***Example Snippet:***

```python
from quixstreams import Application

# Check if "column_a" contains "my_substring" and assign the resulting
# `bool` to a new column: "has_my_substr"

sdf = app.dataframe()
sdf["has_my_substr"] = sdf["column_a"].contains("my_substring")
```


<br>
***Arguments:***

- `other`: object to check


<br>
***Returns:***

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.is_"></a>

<br><br>

#### StreamingSeries.is\_

```python
def is_(other: Union["StreamingSeries", object]) -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L322)

Check if series value refers to the same object as `other`

Runtime result will be a `bool`.



<br>
***Example Snippet:***

```python
# Check if "column_a" is the same as "column_b" and assign the resulting `bool`
#  to a new column: "is_same"

from quixstreams import Application
sdf = app.dataframe()
sdf["is_same"] = sdf["column_a"].is_(sdf["column_b"])
```


<br>
***Arguments:***

- `other`: object to check for "is"


<br>
***Returns:***

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.isnot"></a>

<br><br>

#### StreamingSeries.isnot

```python
def isnot(other: Union["StreamingSeries", object]) -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L345)

Check if series value does not refer to the same object as `other`

Runtime result will be a `bool`.



<br>
***Example Snippet:***

```python
from quixstreams import Application

# Check if "column_a" is the same as "column_b" and assign the resulting `bool`
# to a new column: "is_not_same"

sdf = app.dataframe()
sdf["is_not_same"] = sdf["column_a"].isnot(sdf["column_b"])
```


<br>
***Arguments:***

- `other`: object to check for "is_not"


<br>
***Returns:***

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.isnull"></a>

<br><br>

#### StreamingSeries.isnull

```python
def isnull() -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L369)

Check if series value is None.

Runtime result will be a `bool`.



<br>
***Example Snippet:***

```python
from quixstreams import Application

# Check if "column_a" is null and assign the resulting `bool` to a new column:
# "is_null"

sdf = app.dataframe()
sdf["is_null"] = sdf["column_a"].isnull()
```


<br>
***Returns:***

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.notnull"></a>

<br><br>

#### StreamingSeries.notnull

```python
def notnull() -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L392)

Check if series value is not None.

Runtime result will be a `bool`.



<br>
***Example Snippet:***

```python
from quixstreams import Application

# Check if "column_a" is not null and assign the resulting `bool` to a new column:
# "is_not_null"

sdf = app.dataframe()
sdf["is_not_null"] = sdf["column_a"].notnull()
```


<br>
***Returns:***

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.abs"></a>

<br><br>

#### StreamingSeries.abs

```python
def abs() -> "StreamingSeries"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L415)

Get absolute value of the series value.


<br>
***Example Snippet:***

```python
from quixstreams import Application

# Get absolute value of "int_col" and add it to "other_int_col".
# Finally, assign the result to a new column: "abs_col_sum".

sdf = app.dataframe()
sdf["abs_col_sum"] = sdf["int_col"].abs() + sdf["other_int_col"]
```


<br>
***Returns:***

new StreamingSeries

<a id="quixstreams.dataframe.joins.lookups.base"></a>

## quixstreams.dataframe.joins.lookups.base

<a id="quixstreams.dataframe.joins.lookups.base.BaseLookup"></a>

### BaseLookup

```python
class BaseLookup(abc.ABC, Generic[F])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/base.py#L10)

Abstract base class for implementing custom lookup join strategies for data enrichment in streaming dataframes.

This class defines the interface for lookup joins, where incoming records are enriched with external data based on a key and
a set of fields. Subclasses should implement the `join` method to specify how enrichment is performed.

Typical usage involves passing an instance of a subclass to `StreamingDataFrame.join_lookup`, along with a mapping of field names
to BaseField instances that describe how to extract or map enrichment data.

**Example**:

  class MyLookup(BaseLookup[MyField]):
  def join(self, fields, on, value, key, timestamp, headers):
  # Custom enrichment logic here
  ...

<a id="quixstreams.dataframe.joins.lookups.base.BaseLookup.join"></a>

<br><br>

#### BaseLookup.join

```python
@abc.abstractmethod
def join(fields: Mapping[str, F], on: str, value: dict[str, Any], key: Any,
         timestamp: int, headers: HeadersMapping) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/base.py#L28)

Perform a lookup join operation to enrich the provided value with data from the specified fields.


<br>
***Arguments:***

- `fields`: Mapping of field names to Field objects specifying how to extract and parse configuration data.
- `on`: The key used to fetch data in the lookup.
- `value`: The message value to be updated with enriched configuration values.
- `key`: The message key.
- `timestamp`: The message timestamp, used to select the appropriate configuration version.
- `headers`: The message headers.


<br>
***Returns:***

None. The input value dictionary is updated in-place with the enriched configuration data.

<a id="quixstreams.dataframe.joins.lookups.base.BaseField"></a>

### BaseField

```python
@dataclasses.dataclass(frozen=True)
class BaseField(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/base.py#L53)

Abstract base dataclass for defining a field used in lookup joins.

Subclasses should specify the structure, metadata, and extraction/mapping logic required for a field
to participate in a lookup join operation. Fields are used to describe how enrichment data is mapped
into the target record during a lookup join.

<a id="quixstreams.dataframe.joins.lookups.sqlite"></a>

## quixstreams.dataframe.joins.lookups.sqlite

<a id="quixstreams.dataframe.joins.lookups.sqlite.BaseSQLiteLookupField"></a>

### BaseSQLiteLookupField

```python
@dataclasses.dataclass(frozen=True)
class BaseSQLiteLookupField(BaseField, abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L22)

<a id="quixstreams.dataframe.joins.lookups.sqlite.BaseSQLiteLookupField.build_query"></a>

<br><br>

#### BaseSQLiteLookupField.build\_query

```python
@abc.abstractmethod
def build_query(
    on: str,
    value: dict[str,
                Any]) -> Tuple[str, Union[dict[str, Any], Tuple[str, ...]]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L29)

Build the SQL query string for this field.


<br>
***Arguments:***

- `on`: The key to use in the WHERE clause for lookup.
- `value`: The message value, used to substitute parameters in the query.


<br>
***Returns:***

A tuple of the SQL query string and the parameters.

<a id="quixstreams.dataframe.joins.lookups.sqlite.BaseSQLiteLookupField.result"></a>

<br><br>

#### BaseSQLiteLookupField.result

```python
@abc.abstractmethod
def result(cursor: sqlite3.Cursor) -> Union[dict[str, Any], list[Any]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L43)

Extract the result from the cursor based on the field definition.


<br>
***Arguments:***

- `cursor`: The SQLite cursor containing the query results.


<br>
***Returns:***

The extracted data, either a single row or a list of rows.

<a id="quixstreams.dataframe.joins.lookups.sqlite.SQLiteLookupField"></a>

### SQLiteLookupField

```python
@dataclasses.dataclass(frozen=True)
class SQLiteLookupField(BaseSQLiteLookupField)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L55)

Field definition for use with SQLiteLookup in lookup joins.

Table and column names are sanitized to prevent SQL injection.
Rows will be deserialized into a dictionary with column names as keys.

**Example**:

  
```python
    lookup = SQLiteLookup(path="/path/to/db.sqlite")

    # Select the value in `col1` from the table `my_table` where `col2` matches the `sdf.join_lookup` on parameter.
    fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col1", "col2"], on="col2")}

    # After the lookup the `my_field` column in the message will contains:
    # {"col1": <row1 col1 value>, "col2": <row1 col2 value>}
    sdf = sdf.join_lookup(lookup, fields)
```
  
```python
    lookup = SQLiteLookup(path="/path/to/db.sqlite")

    # Select the value in `col1` from the table `my_table` where `col2` matches the `sdf.join_lookup` on parameter.
    fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col1", "col2"], on="col2", first_match_only=False)}

    # After the lookup the `my_field` column in the message will contains:
    # [
    #   {"col1": <row1 col1 value>, "col2": <row1 col2 value>},
    #   {"col1": <row2 col1 value>, "col2": <row2 col2 value>},
    #   ...
    #   {"col1": <rowN col1 value>, "col2": <rowN col2 value>,},
    # ]
    sdf = sdf.join_lookup(lookup, fields)
```
  
  
<br>
***Arguments:***
  
  - `table`: Name of the table to query in the SQLite database.
  - `columns`: List of columns to select from the table.
  - `on`: The column name to use in the WHERE clause for matching against the target key.
  - `order_by`: Optional ORDER BY clause to sort the results.
  - `order_by_direction`: Direction of the ORDER BY clause, either "ASC" or "DESC". Default is "ASC".
  - `ttl`: Time-to-live for cache in seconds. Default is 60.0.
  - `default`: Default value if no result is found. Default is None.
  - `first_match_only`: If True, only the first row is returned; otherwise, all rows are returned.

<a id="quixstreams.dataframe.joins.lookups.sqlite.SQLiteLookupField.build_query"></a>

<br><br>

#### SQLiteLookupField.build\_query

```python
def build_query(on: str, value: dict[str, Any]) -> Tuple[str, Tuple[str, ...]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L136)

Build the SQL query string for this field.


<br>
***Arguments:***

- `on`: The key to use in the WHERE clause for lookup.
- `value`: The message value, used to substitute parameters in the query.


<br>
***Returns:***

A tuple of the SQL query string and the parameters.

<a id="quixstreams.dataframe.joins.lookups.sqlite.SQLiteLookupField.result"></a>

<br><br>

#### SQLiteLookupField.result

```python
def result(
        cursor: sqlite3.Cursor) -> Union[dict[str, Any], list[dict[str, Any]]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L158)

Extract the result from the cursor based on the field definition.


<br>
***Arguments:***

- `cursor`: The SQLite cursor containing the query results.


<br>
***Returns:***

The extracted data, either a single row or a list of rows.

<a id="quixstreams.dataframe.joins.lookups.sqlite.SQLiteLookupQueryField"></a>

### SQLiteLookupQueryField

```python
@dataclasses.dataclass(frozen=True)
class SQLiteLookupQueryField(BaseSQLiteLookupField)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L176)

Field definition for use with SQLiteLookup in lookup joins.

Enables advanced SQL queries with support for parameter substitution from message columns, allowing dynamic lookups.

The `sdf.join_lookup` `on` parameter is not used in the query itself, but is important for cache management. When caching is enabled, the query is executed once per TTL for each unique target key.

Query results are returned as tuples of values, without additional deserialization.

**Example**:

  
```python
    lookup = SQLiteLookup(path="/path/to/db.sqlite")

    # Select all columns from the first row of `my_table` where `col2` matches the value of `field1` in the message.
    fields = {"my_field": SQLiteLookupQueryField("SELECT * FROM my_table WHERE col2 = :field1")}

    # After the lookup, the `my_field` column in the message will contain:
    # [<row1 col1 value>, <row1 col2 value>, ..., <row1 colN value>]
    sdf = sdf.join_lookup(lookup, fields)
```
  
```python
    lookup = SQLiteLookup(path="/path/to/db.sqlite")

    # Select all columns from all rows of `my_table` where `col2` matches the value of `field1` in the message.
    fields = {"my_field": SQLiteLookupQueryField("SELECT * FROM my_table WHERE col2 = :field1", first_match_only=False)}

    # After the lookup, the `my_field` column in the message will contain:
    # [
    #   [<row1 col1 value>, <row1 col2 value>, ..., <row1 colN value>],
    #   [<row2 col1 value>, <row2 col2 value>, ..., <row2 colN value>],
    #   ...
    #   [<rowN col1 value>, <rowN col2 value>, ..., <rowN colN value>],
    # ]
    sdf = sdf.join_lookup(lookup, fields)
```
  
  
<br>
***Arguments:***
  
  - `query`: SQL query to execute.
  - `ttl`: Time-to-live for cache in seconds. Default is 60.0.
  - `default`: Default value if no result is found. Default is None.
  - `first_match_only`: If True, only the first row is returned; otherwise, all rows are returned.

<a id="quixstreams.dataframe.joins.lookups.sqlite.SQLiteLookupQueryField.result"></a>

<br><br>

#### SQLiteLookupQueryField.result

```python
def result(cursor: sqlite3.Cursor) -> Union[dict[str, Any], list[Any]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L231)

Extract the result from the cursor based on the field definition.


<br>
***Arguments:***

- `cursor`: The SQLite cursor containing the query results.


<br>
***Returns:***

The extracted data, either a single row or a list of rows.

<a id="quixstreams.dataframe.joins.lookups.sqlite.SQLiteLookup"></a>

### SQLiteLookup

```python
class SQLiteLookup(BaseLookup[Union[SQLiteLookupField,
                                    SQLiteLookupQueryField]])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L244)

Lookup join implementation for enriching streaming data with data from a SQLite database.

This class queries a SQLite database for each field, using a persistent connection and per-field caching
based on a configurable TTL. The cache is a least recently used (LRU) cache with a configurable maximum size.

**Example**:

  
```python
    lookup = SQLiteLookup(path="/path/to/db.sqlite")
    fields = {"my_field": SQLiteLookupField(table="my_table", columns=["col2"], on="primary_key_col")}
    sdf = sdf.join_lookup(lookup, fields)
```
  
  
<br>
***Arguments:***
  
  - `path`: Path to the SQLite database file.
  - `cache_size`: Maximum number of fields to keep in the LRU cache. Default is 1000.

<a id="quixstreams.dataframe.joins.lookups.sqlite.SQLiteLookup.join"></a>

<br><br>

#### SQLiteLookup.join

```python
def join(fields: Mapping[str, Union[SQLiteLookupField,
                                    SQLiteLookupQueryField]], on: str,
         value: dict[str,
                     Any], key: Any, timestamp: int, headers: Any) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L342)

Enrich the message value in-place by querying SQLite for each field and caching results per TTL.


<br>
***Arguments:***

- `fields`: Mapping of field names to BaseSQLiteLookupField objects specifying how to extract and map enrichment data.
- `on`: The key used in the WHERE clause for SQLiteLookupField lookup.
- `value`: The message value.
- `key`: The message key.
- `timestamp`: The message timestamp.
- `headers`: The message headers.


<br>
***Returns:***

None. The input value dictionary is updated in-place with the enriched data.

<a id="quixstreams.dataframe.joins.lookups.sqlite.SQLiteLookup.cache_info"></a>

<br><br>

#### SQLiteLookup.cache\_info

```python
def cache_info() -> CacheInfo
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/sqlite.py#L382)

Get cache statistics for the SQLiteLookup LRU cache.


<br>
***Returns:***

A dictionary containing cache statistics: hits, misses, size, maxsize.

<a id="quixstreams.dataframe.joins.lookups.postgresql"></a>

## quixstreams.dataframe.joins.lookups.postgresql

<a id="quixstreams.dataframe.joins.lookups.postgresql.BasePostgresLookupField"></a>

### BasePostgresLookupField

```python
@dataclasses.dataclass(frozen=True)
class BasePostgresLookupField(BaseField, abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L31)

<a id="quixstreams.dataframe.joins.lookups.postgresql.BasePostgresLookupField.build_query"></a>

<br><br>

#### BasePostgresLookupField.build\_query

```python
@abc.abstractmethod
def build_query(
    on: str, value: dict[str, Any]
) -> Tuple[sql.Composable, Union[dict[str, Any], Tuple[str, ...]]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L38)

Build the SQL query string for this field.


<br>
***Arguments:***

- `on`: The key to use in the WHERE clause for lookup.
- `value`: The message value, used to substitute parameters in the query.


<br>
***Returns:***

A tuple of the SQL query string and the parameters.

<a id="quixstreams.dataframe.joins.lookups.postgresql.BasePostgresLookupField.result"></a>

<br><br>

#### BasePostgresLookupField.result

```python
@abc.abstractmethod
def result(cursor: pg_cursor) -> Union[dict[str, Any], list[dict[str, Any]]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L52)

Extract the result from the cursor based on the field definition.


<br>
***Arguments:***

- `cursor`: The Postgres cursor containing the query results.


<br>
***Returns:***

The extracted data, either a single row or a list of rows.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookupField"></a>

### PostgresLookupField

```python
@dataclasses.dataclass(frozen=True)
class PostgresLookupField(BasePostgresLookupField)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L64)

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookupField.build_query"></a>

<br><br>

#### PostgresLookupField.build\_query

```python
def build_query(on: str,
                value: dict[str, Any]) -> Tuple[sql.Composed, Tuple[str, ...]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L103)

Build the SQL query string for this field.


<br>
***Arguments:***

- `on`: The key to use in the WHERE clause for lookup.
- `value`: The message value, used to substitute parameters in the query.


<br>
***Returns:***

A tuple of the SQL query string and the parameters.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookupField.result"></a>

<br><br>

#### PostgresLookupField.result

```python
def result(cursor: pg_cursor) -> Union[dict[str, Any], list[dict[str, Any]]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L133)

Extract the result from the cursor based on the field definition.


<br>
***Arguments:***

- `cursor`: The SQLite cursor containing the query results.


<br>
***Returns:***

The extracted data, either a single row or a list of rows.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookupQueryField"></a>

### PostgresLookupQueryField

```python
@dataclasses.dataclass(frozen=True)
class PostgresLookupQueryField(BasePostgresLookupField)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L148)

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookupQueryField.result"></a>

<br><br>

#### PostgresLookupQueryField.result

```python
def result(cursor: pg_cursor) -> Union[list[Any], Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L161)

Extract the result from the cursor based on the field definition.


<br>
***Arguments:***

- `cursor`: The Postgres cursor containing the query results.


<br>
***Returns:***

The extracted data, either a single row or a list of rows.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookup"></a>

### PostgresLookup

```python
class PostgresLookup(BaseLookup[Union[PostgresLookupField,
                                      PostgresLookupQueryField]])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L174)

Lookup join implementation for enriching streaming data with data from a Postgres database.

This class queries a Postgres database for each field, using a persistent connection and per-field caching
based on a configurable TTL. The cache is a "Least Recently Used" (LRU) cache with a configurable maximum size.

**Example**:

  
  This is a join on kafka record column `k_colX` with table column `t_col2`
  (where their values are equal).
  
```python
    lookup = PostgresLookup(**credentials)
    fields = {"my_field": lookup.field(table="my_table", columns=["t_col2"], on="t_col1")}
    sdf = sdf.join_lookup(lookup, fields, on="k_colX")
```
  Note that `join_lookup` uses `on=<kafka message key>` if a column is not provided.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookup.__init__"></a>

<br><br>

#### PostgresLookup.\_\_init\_\_

```python
def __init__(host: str,
             port: int,
             dbname: str,
             user: str,
             password: str,
             connection_timeout_seconds: int = 30,
             statement_timeout_seconds: int = 30,
             cache_size: int = 1000,
             **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L194)


<br>
***Arguments:***

- `host`: PostgreSQL server address.
- `port`: PostgreSQL server port.
- `dbname`: PostgreSQL database name.
- `user`: Database username.
- `password`: Database user password.
- `connection_timeout_seconds`: Timeout for connection.
- `statement_timeout_seconds`: Timeout for DDL operations such as table
creation or schema updates.
- `cache_size`: Maximum number of fields to keep in the LRU cache. Default is 1000.
- `kwargs`: Additional parameters for `psycopg2.connect`.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookup.field"></a>

<br><br>

#### PostgresLookup.field

```python
def field(table: str,
          columns: list[str],
          on: str,
          order_by: str = "",
          order_by_direction: Literal["ASC", "DESC"] = "ASC",
          schema: str = "public",
          ttl: float = 60.0,
          default: Any = None,
          first_match_only: bool = True) -> PostgresLookupField
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L318)

Field definition for use with PostgresLookup in lookup joins.

Table and column names are sanitized to prevent SQL injection.
Rows will be deserialized into a dictionary with column names as keys.

**Example**:

  With kafka records formatted as:
  row = {"k_colX": "value_a", "k_colY": "value_b"}
  
  We want to join this to DB table record(s) where table column `t_col2` has the
  same value as kafka row's key `k_colX` (`value_a`).
  
```python
    lookup = PostgresLookup(**credentials)

    # Select the value in `db_col1` from the table `my_table` where `col2` matches the `sdf.join_lookup` on parameter.
    fields = {"my_field": lookup.field(table="my_table", columns=["t_col1", "t_col2"], on="t_col2")}

    # After the lookup the `my_field` column in the message contains:
    # {"t_col1": <row1 t_col1 value>, "t_col2": <row1 t_col2 value>}
    sdf = sdf.join_lookup(lookup, fields, on="kafka_col1")
```
  
```python
    lookup = PostgresLookup(**credentials)

    # Select the value in `t_col1` from the table `my_table` where `t_col2` matches the `sdf.join_lookup` on parameter.
    fields = {"my_field": lookup.field(table="my_table", columns=["t_col1", "t_col2"], on="t_col2", first_match_only=False)}

    # After the lookup the `my_field` column in the message contains:
    # [
    #   {"t_col1": <row1 t_col1 value>, "t_col2": <row1 t_col2 value>},
    #   {"t_col1": <row2 t_col1 value>, "t_col2": <row2 t_col2 value>},
    #   ...
    #   {"t_col1": <rowN col1 value>, "t_col2": <rowN t_col2 value>,},
    # ]
    sdf = sdf.join_lookup(lookup, fields, on="k_colX")
```
  
  
<br>
***Arguments:***
  
  - `table`: Name of the table to query in the Postgres database.
  - `columns`: List of columns to select from the table.
  - `on`: The column name to use in the WHERE clause for matching against the target key.
  - `order_by`: Optional ORDER BY clause to sort the results.
  - `order_by_direction`: Direction of the ORDER BY clause, either "ASC" or "DESC". Default is "ASC".
  - `schema`: the table schema; if unsure leave as default ("public").
  - `ttl`: Time-to-live for cache in seconds. Default is 60.0.
  - `default`: Default value if no result is found. Default is None.
  - `first_match_only`: If True, only the first row is returned; otherwise, all rows are returned.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookup.query_field"></a>

<br><br>

#### PostgresLookup.query\_field

```python
def query_field(query: str,
                ttl: float = 60.0,
                default: Any = None,
                first_match_only: bool = True) -> PostgresLookupQueryField
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L392)

Field definition for use with PostgresLookup in lookup joins.

Enables advanced SQL queries with support for parameter substitution from message columns, allowing dynamic lookups.

The `sdf.join_lookup` `on` parameter is not used in the query itself, but is important for cache management. When caching is enabled, the query is executed once per TTL for each unique target key.

Query results are returned as tuples of values, without additional deserialization.

**Example**:

  
```python
    lookup = PostgresLookup(**credentials)

    # Select all columns from the first row of `my_table` where `col2` matches the value of `field1` in the message.
    fields = {"my_field": lookup.query_field("SELECT * FROM my_table WHERE col2 = %(field_1)s")}

    # After the lookup, the `my_field` column in the message will contain:
    # [<row1 col1 value>, <row1 col2 value>, ..., <row1 colN value>]
    sdf = sdf.join_lookup(lookup, fields)
```
  
```python
    lookup = PostgresLookup(**creds)

    # Select all columns from all rows of `my_table` where `col2` matches the value of `field1` in the message.
    fields = {"my_field": lookup.query_field("SELECT * FROM my_table WHERE col2 = %(field_1)s", first_match_only=False)}

    # After the lookup, the `my_field` column in the message will contain:
    # [
    #   [<row1 col1 value>, <row1 col2 value>, ..., <row1 colN value>],
    #   [<row2 col1 value>, <row2 col2 value>, ..., <row2 colN value>],
    #   ...
    #   [<rowN col1 value>, <rowN col2 value>, ..., <rowN colN value>],
    # ]
    sdf = sdf.join_lookup(lookup, fields)
```
  
  
<br>
***Arguments:***
  
  - `query`: SQL query to execute.
  - `ttl`: Time-to-live for cache in seconds. Default is 60.0.
  - `default`: Default value if no result is found. Default is None.
  - `first_match_only`: If True, only the first row is returned; otherwise, all rows are returned.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookup.join"></a>

<br><br>

#### PostgresLookup.join

```python
def join(fields: Mapping[str, Union[PostgresLookupField,
                                    PostgresLookupQueryField]], on: str,
         value: dict[str,
                     Any], key: Any, timestamp: int, headers: Any) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L446)

Enrich the message value in-place by querying SQLite for each field and caching results per TTL.


<br>
***Arguments:***

- `fields`: Mapping of field names to BaseSQLiteLookupField objects specifying how to extract and map enrichment data.
- `on`: The key used in the WHERE clause for SQLiteLookupField lookup.
- `value`: The message value.
- `key`: The message key.
- `timestamp`: The message timestamp.
- `headers`: The message headers.


<br>
***Returns:***

None. The input value dictionary is updated in-place with the enriched data.

<a id="quixstreams.dataframe.joins.lookups.postgresql.PostgresLookup.cache_info"></a>

<br><br>

#### PostgresLookup.cache\_info

```python
def cache_info() -> CacheInfo
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/joins/lookups/postgresql.py#L486)

Get cache statistics for the SQLiteLookup LRU cache.


<br>
***Returns:***

A dictionary containing cache statistics: hits, misses, size, maxsize.

<a id="quixstreams.dataframe.joins.join_asof"></a>

## quixstreams.dataframe.joins.join\_asof

