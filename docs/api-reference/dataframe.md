<a id="quixstreams.context"></a>

## quixstreams.context

<a id="quixstreams.context.set_message_context"></a>

#### set\_message\_context

```python
def set_message_context(context: Optional[MessageContext])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/context.py#L22)

Set a MessageContext for the current message in the given `contextvars.Context`

**Arguments**:

- `context`: instance of `MessageContext`

<a id="quixstreams.context.message_context"></a>

#### message\_context

```python
def message_context() -> MessageContext
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/context.py#L31)

Get a MessageContext for the current message

**Returns**:

instance of `MessageContext`

<a id="quixstreams.context.message_key"></a>

#### message\_key

```python
def message_key() -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/context.py#L43)

Get current a message key.

**Returns**:

a deserialized message key

<a id="quixstreams.dataframe.dataframe"></a>

## quixstreams.dataframe.dataframe

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame"></a>

### StreamingDataFrame

```python
class StreamingDataFrame(BaseStreaming)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/dataframe.py#L26)

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.apply"></a>

#### apply

```python
def apply(func: Union[DataFrameFunc, DataFrameStatefulFunc],
          stateful: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/dataframe.py#L46)

Apply a function to transform the value and return a new value.

The result will be passed downstream as an input value.

**Arguments**:

- `func`: a function to apply
- `stateful`: if `True`, the function will be provided with a second argument
of type `State` to perform stateful operations.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.update"></a>

#### update

```python
def update(func: Union[DataFrameFunc, DataFrameStatefulFunc],
           stateful: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/dataframe.py#L65)

Apply a function to mutate value in-place or to perform a side effect

that doesn't update the value (e.g. print a value to the console).

The result of the function will be ignored, and the original value will be
passed downstream.

**Arguments**:

- `func`: function to update value
- `stateful`: if `True`, the function will be provided with a second argument
of type `State` to perform stateful operations.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.filter"></a>

#### filter

```python
def filter(func: Union[DataFrameFunc, DataFrameStatefulFunc],
           stateful: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/dataframe.py#L86)

Filter value using provided function.

If the function returns True-like value, the original value will be
passed downstream.
Otherwise, the `Filtered` exception will be raised.

**Arguments**:

- `func`: function to filter value
- `stateful`: if `True`, the function will be provided with second argument
of type `State` to perform stateful operations.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.contains"></a>

#### contains

```python
@staticmethod
def contains(key: str) -> StreamingSeries
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/dataframe.py#L119)

Check if the key is present in the Row value.

**Arguments**:

- `key`: a column name to check.

**Returns**:

a Column object that evaluates to True if the key is present or False otherwise.

**Example**:

  >>> df = StreamingDataframe()
  >>> sdf['has_column'] = sdf.contains('column_x')
  # This would add a new column 'has_column' which contains boolean values
  # indicating the presence of 'column_x' in each row.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.to_topic"></a>

#### to\_topic

```python
def to_topic(topic: Topic,
             key: Optional[Callable[[object], object]] = None) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/dataframe.py#L134)

Produce value to the topic.

.. note:: A `RowProducer` instance must be assigned to
`StreamingDataFrame.producer` if not using :class:`quixstreams.app.Application`
 to facilitate the execution of StreamingDataFrame.

**Arguments**:

- `topic`: instance of `Topic`
- `key`: a callable to generate a new message key, optional.
If passed, the return type of this callable must be serializable
by `key_serializer` defined for this Topic object.
By default, the current message key will be used.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.compile"></a>

#### compile

```python
def compile() -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/dataframe.py#L155)

Compile all functions of this StreamingDataFrame into one big closure.

Closures are more performant than calling all the functions in the
`StreamingDataFrame` one-by-one.

**Returns**:

a function that accepts "value"
and returns a result of StreamingDataFrame

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.test"></a>

#### test

```python
def test(value: object, ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/dataframe.py#L167)

A shorthand to test `StreamingDataFrame` with provided value

and `MessageContext`.

**Arguments**:

- `value`: value to pass through `StreamingDataFrame`
- `ctx`: instance of `MessageContext`, optional.
Provide it if the StreamingDataFrame instance calls `to_topic()`,
has stateful functions or functions calling `get_current_key()`.
Default - `None`.

**Returns**:

result of `StreamingDataFrame`

<a id="quixstreams.dataframe.series"></a>

## quixstreams.dataframe.series

<a id="quixstreams.dataframe.series.StreamingSeries"></a>

### StreamingSeries

```python
class StreamingSeries(BaseStreaming)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L16)

<a id="quixstreams.dataframe.series.StreamingSeries.from_func"></a>

#### from\_func

```python
@classmethod
def from_func(cls, func: StreamCallable) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L27)

Createa StreamingSeries from a function.

The provided function will be wrapped into `Apply`

**Arguments**:

- `func`: a function to apply

**Returns**:

instance of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.apply"></a>

#### apply

```python
def apply(func: StreamCallable) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L41)

Add a callable to the execution list for this series.

The provided callable should accept a single argument, which will be its input.
The provided callable should similarly return one output, or None

**Arguments**:

- `func`: a callable with one argument and one output

**Returns**:

a new `StreamingSeries` with the new callable added

<a id="quixstreams.dataframe.series.StreamingSeries.compile"></a>

#### compile

```python
def compile(allow_filters: bool = True,
            allow_updates: bool = True) -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L54)

Compile all functions of this StreamingSeries into one big closure.

Closures are more performant than calling all the functions in the
`StreamingDataFrame` one-by-one.

**Arguments**:

- `allow_filters`: If False, this function will fail with ValueError if
the stream has filter functions in the tree. Default - True.
- `allow_updates`: If False, this function will fail with ValueError if
the stream has update functions in the tree. Default - True.

**Raises**:

- `ValueError`: if disallowed functions are present in the tree of
underlying `Stream`.

**Returns**:

a function that accepts "value"
and returns a result of StreamingDataFrame

<a id="quixstreams.dataframe.series.StreamingSeries.test"></a>

#### test

```python
def test(value: Any, ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L81)

A shorthand to test `StreamingSeries` with provided value

and `MessageContext`.

**Arguments**:

- `value`: value to pass through `StreamingSeries`
- `ctx`: instance of `MessageContext`, optional.
Provide it if the StreamingSeries instance has
functions calling `get_current_key()`.
Default - `None`.

**Returns**:

result of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.isin"></a>

#### isin

```python
def isin(other: Container) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L112)

Check if series value is in "other".

Same as "StreamingSeries in other".

**Arguments**:

- `other`: a container to check

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.contains"></a>

#### contains

```python
def contains(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L124)

Check if series value contains "other"

Same as "other in StreamingSeries".

**Arguments**:

- `other`: object to check

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.is_"></a>

#### is\_

```python
def is_(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L134)

Check if series value refers to the same object as `other`

**Arguments**:

- `other`: object to check for "is"

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.isnot"></a>

#### isnot

```python
def isnot(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L142)

Check if series value refers to the same object as `other`

**Arguments**:

- `other`: object to check for "is"

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.isnull"></a>

#### isnull

```python
def isnull() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L150)

Check if series value is None

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.notnull"></a>

#### notnull

```python
def notnull() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L158)

Check if series value is not None

<a id="quixstreams.dataframe.series.StreamingSeries.abs"></a>

#### abs

```python
def abs() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/dataframe/series.py#L164)

Get absolute value of the series value

