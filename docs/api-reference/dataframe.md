<a id="quixstreams.dataframe.dataframe"></a>

## quixstreams.dataframe.dataframe

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame"></a>

### StreamingDataFrame

```python
class StreamingDataFrame(BaseStreaming)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L29)

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
sdf = StreamingDataframe()
sdf = sdf.apply(a_func)
sdf = sdf.filter(another_func)
sdf = sdf.to_topic(topic_obj)
```

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.apply"></a>

<br><br>

#### StreamingDataFrame.apply

```python
def apply(func: Union[DataFrameFunc, DataFrameStatefulFunc],
          stateful: bool = False,
          expand: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L99)

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

sdf = StreamingDataframe()
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

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.update"></a>

<br><br>

#### StreamingDataFrame.update

```python
def update(func: Union[DataFrameFunc, DataFrameStatefulFunc],
           stateful: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L142)

Apply a function to mutate value in-place or to perform a side effect

that doesn't update the value (e.g. print a value to the console).

The result of the function will be ignored, and the original value will be
passed downstream.



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

sdf = StreamingDataframe()
sdf = sdf.update(func, stateful=True)
sdf = sdf.update(lambda value: print("Received value: ", value))
```


<br>
***Arguments:***

- `func`: function to update value
- `stateful`: if `True`, the function will be provided with a second argument
of type `State` to perform stateful operations.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.filter"></a>

<br><br>

#### StreamingDataFrame.filter

```python
def filter(func: Union[DataFrameFunc, DataFrameStatefulFunc],
           stateful: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L181)

Filter value using provided function.

If the function returns True-like value, the original value will be
passed downstream.
Otherwise, the `Filtered` exception will be raised (further processing for that
message will be skipped).



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

sdf = StreamingDataframe()
sdf = sdf.filter(func, stateful=True)
```


<br>
***Arguments:***

- `func`: function to filter value
- `stateful`: if `True`, the function will be provided with second argument
of type `State` to perform stateful operations.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.contains"></a>

<br><br>

#### StreamingDataFrame.contains

```python
@staticmethod
def contains(key: str) -> StreamingSeries
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L234)

Check if the key is present in the Row value.


<br>
***Example Snippet:***

```python
# Add new column 'has_column' which contains a boolean indicating
# the presence of 'column_x'

sdf = StreamingDataframe()
sdf['has_column'] = sdf.contains('column_x')
```


<br>
***Arguments:***

- `key`: a column name to check.


<br>
***Returns:***

a Column object that evaluates to True if the key is present
or False otherwise.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.to_topic"></a>

<br><br>

#### StreamingDataFrame.to\_topic

```python
def to_topic(topic: Topic,
             key: Optional[Callable[[object], object]] = None) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L257)

Produce current value to a topic. You can optionally specify a new key.

>***NOTE:*** A `RowProducer` instance must be assigned to
`StreamingDataFrame.producer` if not using :class:`quixstreams.app.Application`
 to facilitate the execution of StreamingDataFrame.



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
sdf = sdf.to_topic(output_topic_1, key=lambda data: data["a_field"])
```


<br>
***Arguments:***

- `topic`: instance of `Topic`
- `key`: a callable to generate a new message key, optional.
If passed, the return type of this callable must be serializable
by `key_serializer` defined for this Topic object.
By default, the current message key will be used.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.compose"></a>

<br><br>

#### StreamingDataFrame.compose

```python
def compose() -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L296)

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
***Returns:***

a function that accepts "value"
and returns a result of StreamingDataFrame

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.test"></a>

<br><br>

#### StreamingDataFrame.test

```python
def test(value: object, ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L326)

A shorthand to test `StreamingDataFrame` with provided value

and `MessageContext`.


<br>
***Arguments:***

- `value`: value to pass through `StreamingDataFrame`
- `ctx`: instance of `MessageContext`, optional.
Provide it if the StreamingDataFrame instance calls `to_topic()`,
has stateful functions or functions calling `get_current_key()`.
Default - `None`.


<br>
***Returns:***

result of `StreamingDataFrame`

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.tumbling_window"></a>

<br><br>

#### StreamingDataFrame.tumbling\_window

```python
def tumbling_window(
        duration_ms: Union[int, timedelta],
        grace_ms: Optional[Union[int,
                                 timedelta]] = 0) -> TumblingWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L344)

Create a tumbling window transformation on this StreamingDataFrame.

Tumbling windows divide time into fixed-sized, non-overlapping windows.

They allow to perform stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

The time windows always use the current event time.


<br>
***Example Snippet:***

```python
app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a tumbling window of 60s and grace period of 10s
    sdf.tumbling_window(
        duration_ms=timedelta(seconds=60), grace_ms=timedelta(seconds=10.0)
    )

    # Specify the aggregation function
    .sum()

    # Specify how the results should be emitted downstream.
    # "all()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .all()
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


<br>
***Returns:***

`TumblingWindowDefinition` instance representing the tumbling window
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
        grace_ms: Optional[Union[int,
                                 timedelta]] = 0) -> HoppingWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L409)

Create a hopping window transformation on this StreamingDataFrame.

Hopping windows divide the data stream into overlapping windows based on time.
The overlap is controlled by the `step_ms` parameter.

They allow to perform stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

The time windows always use the current event time.


<br>
***Example Snippet:***

```python

app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a a hopping window of 60s with step 30s and grace period of 10s
    sdf.hopping_window(
        duration_ms=timedelta(seconds=60),
        step_ms=timedelta(seconds=30),
        grace_ms=timedelta(seconds=10)
    )

    # Specify the aggregation function
    .sum()

    # Specify how the results should be emitted downstream.
    # "all()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .all()
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


<br>
***Returns:***

`HoppingWindowDefinition` instance representing the hopping
window configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. and applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.series"></a>

## quixstreams.dataframe.series

<a id="quixstreams.dataframe.series.StreamingSeries"></a>

### StreamingSeries

```python
class StreamingSeries(BaseStreaming)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L16)

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

sdf = StreamingDataframe()
sdf = sdf["column_a"].apply(a_func).apply(diff_func, stateful=True)
sdf["my_new_bool_field"] = sdf["column_b"].contains("this_string")
sdf["new_sum_field"] = sdf["column_c"] + sdf["column_d"] + 2
sdf = sdf[["column_a"] & (sdf["new_sum_field"] >= 10)]
```

<a id="quixstreams.dataframe.series.StreamingSeries.from_func"></a>

<br><br>

#### StreamingSeries.from\_func

```python
@classmethod
def from_func(cls, func: StreamCallable) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L76)

Create a StreamingSeries from a function.

The provided function will be wrapped into `Apply`


<br>
***Arguments:***

- `func`: a function to apply


<br>
***Returns:***

instance of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.apply"></a>

<br><br>

#### StreamingSeries.apply

```python
def apply(func: StreamCallable) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L90)

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

sdf = StreamingDataframe()
sdf["new_col"] = sdf["a_column"]["nested_dict_key"].apply(func, stateful=True)
sdf["new_col_2"] = sdf["str_col"].apply(lambda v: int(v)) + sdf["str_col2"] + 2
```


<br>
***Arguments:***

- `func`: a callable with one argument and one output


<br>
***Returns:***

a new `StreamingSeries` with the new callable added

<a id="quixstreams.dataframe.series.StreamingSeries.compose"></a>

<br><br>

#### StreamingSeries.compose

```python
def compose(allow_filters: bool = True,
            allow_updates: bool = True) -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L124)

Compose all functions of this StreamingSeries into one big closure.

Closures are more performant than calling all the functions in the
`StreamingDataFrame` one-by-one.

Generally not required by users; the `quixstreams.app.Application` class will
do this automatically.



<br>
***Example Snippet:***

```python
from quixstreams import Application

app = Application(...)

sdf = app.dataframe()
sdf = sdf["column_a"].apply(apply_func)
sdf = sdf["column_b"].contains(filter_func)
sdf = sdf.compose()

result_0 = sdf({"my": "record"})
result_1 = sdf({"other": "record"})
```


<br>
***Arguments:***

- `allow_filters`: If False, this function will fail with ValueError if
the stream has filter functions in the tree. Default - True.
- `allow_updates`: If False, this function will fail with ValueError if
the stream has update functions in the tree. Default - True.

**Raises**:

- `ValueError`: if disallowed functions are present in the tree of
underlying `Stream`.


<br>
***Returns:***

a function that accepts "value"
and returns a result of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.test"></a>

<br><br>

#### StreamingSeries.test

```python
def test(value: Any, ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L171)

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
def isin(other: Container) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L202)

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
def contains(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L229)

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
def is_(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L254)

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
def isnot(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L277)

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
def isnull() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L301)

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
def notnull() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L324)

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
def abs() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L347)

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

<a id="quixstreams.context"></a>

## quixstreams.context

<a id="quixstreams.context.set_message_context"></a>

<br><br>

#### set\_message\_context

```python
def set_message_context(context: Optional[MessageContext])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/context.py#L22)

Set a MessageContext for the current message in the given `contextvars.Context`

>***NOTE:*** This is for advanced usage only. If you need to change the message key,
`StreamingDataFrame.to_topic()` has an argument for it.



<br>
***Example Snippet:***

```python
from quixstreams import Application, set_message_context, message_context

# Changes the current sdf value based on what the message partition is.
def alter_context(value):
    context = message_context()
    if value > 1:
        context.headers = context.headers + (b"cool_new_header", value.encode())
        set_message_context(context)

app = Application()
sdf = app.dataframe()
sdf = sdf.update(lambda value: alter_context(value))
```


<br>
***Arguments:***

- `context`: instance of `MessageContext`

<a id="quixstreams.context.message_context"></a>

<br><br>

#### message\_context

```python
def message_context() -> MessageContext
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/context.py#L53)

Get a MessageContext for the current message, which houses most of the message

metadata, like:
    - key
    - timestamp
    - partition
    - offset



<br>
***Example Snippet:***

```python
from quixstreams import Application, message_context

# Changes the current sdf value based on what the message partition is.

app = Application()
sdf = app.dataframe()
sdf = sdf.apply(lambda value: 1 if message_context().partition == 2 else 0)
```


<br>
***Returns:***

instance of `MessageContext`

<a id="quixstreams.context.message_key"></a>

<br><br>

#### message\_key

```python
def message_key() -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/context.py#L84)

Get the current message's key.


<br>
***Example Snippet:***

```python
from quixstreams import Application, message_key

# Changes the current sdf value based on what the message key is.

app = Application()
sdf = app.dataframe()
sdf = sdf.apply(lambda value: 1 if message_key() == b'1' else 0)
```


<br>
***Returns:***

a deserialized message key

