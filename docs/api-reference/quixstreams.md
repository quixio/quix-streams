<a id="quixstreams.app"></a>

## quixstreams.app

<a id="quixstreams.app.Application"></a>

### Application

```python
class Application()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/app.py#L42)

<a id="quixstreams.app.Application.Quix"></a>

#### Quix

```python
@classmethod
def Quix(cls,
         consumer_group: str,
         auto_offset_reset: AutoOffsetReset = "latest",
         auto_commit_enable: bool = True,
         assignment_strategy: AssignmentStrategy = "range",
         partitioner: Partitioner = "murmur2",
         consumer_extra_config: Optional[dict] = None,
         producer_extra_config: Optional[dict] = None,
         state_dir: str = "state",
         rocksdb_options: Optional[RocksDBOptionsType] = None,
         on_consumer_error: Optional[ConsumerErrorCallback] = None,
         on_processing_error: Optional[ProcessingErrorCallback] = None,
         on_producer_error: Optional[ProducerErrorCallback] = None,
         on_message_processed: Optional[MessageProcessedCallback] = None,
         consumer_poll_timeout: float = 1.0,
         producer_poll_timeout: float = 0.0,
         loglevel: Optional[LogLevel] = "INFO",
         quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
         auto_create_topics: bool = True) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/app.py#L157)

Initialize an Application to work with Quix platform,

assuming environment is properly configured (by default in the platform).

It takes the credentials from the environment and configures consumer and
producer to properly connect to the Quix platform.

.. note:: Quix platform requires `consumer_group` and topic names to be prefixed
    with workspace id.
    If the application is created via `Application.Quix()`, the real consumer
    group will be `<workspace_id>-<consumer_group>`,
    and the real topic names will be `<workspace_id>-<topic_name>`.

**Arguments**:

- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`.
.. note:: The consumer group will be prefixed by Quix workspace id.
- `auto_offset_reset`: Consumer `auto.offset.reset` setting
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
- `assignment_strategy`: The name of a partition assignment strategy.
- `partitioner`: A function to be used to determine the outgoing message
partition.
- `consumer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
- `producer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
- `state_dir`: path to the application state directory.
Default - ".state".
- `rocksdb_options`: RocksDB options.
If `None`, the default options will be used.
- `consumer_poll_timeout`: timeout for `RowConsumer.poll()`. Default - 1.0s
- `producer_poll_timeout`: timeout for `RowProducer.poll()`. Default - 0s.
- `on_message_processed`: a callback triggered when message is successfully
processed.
- `loglevel`: a log level for "quixstreams" logger.
Should be a string or None.
    If `None` is passed, no logging will be configured.
    You may pass `None` and configure "quixstreams" logger
    externally using `logging` library.
    Default - "INFO".

To handle errors, `Application` accepts callbacks triggered when exceptions
occur on different stages of stream processing.
If the callback returns `True`, the exception will be ignored. Otherwise, the
exception will be propagated and the processing will eventually stop.
- `on_consumer_error`: triggered when internal `RowConsumer` fails
to poll Kafka or cannot deserialize a message.
- `on_processing_error`: triggered when exception is raised within
`StreamingDataFrame.process()`.
- `on_producer_error`: triggered when RowProducer fails to serialize
or to produce a message to Kafka.


Quix-specific parameters:
- `quix_config_builder`: instance of `QuixKafkaConfigsBuilder` to be used
instead of the default one.
- `auto_create_topics`: Whether to auto-create any topics handed to a
StreamingDataFrame instance (topics_in + topics_out).

**Returns**:

`Application` object

<a id="quixstreams.app.Application.topic"></a>

#### topic

```python
def topic(name: str,
          value_deserializer: DeserializerType = "json",
          key_deserializer: DeserializerType = "bytes",
          value_serializer: SerializerType = "json",
          key_serializer: SerializerType = "bytes",
          creation_configs: Optional[TopicCreationConfigs] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/app.py#L283)

Create a topic definition.

Allows you to specify serialization that should be used when consuming/producing
to the topic in the form of a string name (i.e. "json" for JSON) or a
serialization class instance directly, like JSONSerializer().

**Arguments**:

- `name`: topic name
.. note:: If the application is created via `Quix.Application()`,
the topic name will be prefixed by Quix workspace id, and it will
be `<workspace_id>-<name>`
- `value_deserializer`: a deserializer type for values; default="json"
- `key_deserializer`: a deserializer type for keys; default="bytes"
- `value_serializer`: a serializer type for values; default="json"
- `key_serializer`: a serializer type for keys; default="bytes"
- `creation_configs`: settings for auto topic creation (Quix platform only)
Its name will be overridden by this method's 'name' param.

**Returns**:

`Topic` object

<a id="quixstreams.app.Application.dataframe"></a>

#### dataframe

```python
def dataframe(topic: Topic) -> StreamingDataFrame
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/app.py#L326)

Create a StreamingDataFrame to define message processing pipeline.

See :class:`quixstreams.dataframe.StreamingDataFrame` for more details

**Arguments**:

- `topic`: a `quixstreams.models.Topic` instance
to be used as an input topic.

**Returns**:

`StreamingDataFrame` object

<a id="quixstreams.app.Application.stop"></a>

#### stop

```python
def stop()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/app.py#L343)

Stop the internal poll loop and the message processing.

<a id="quixstreams.app.Application.clear_state"></a>

#### clear\_state

```python
def clear_state()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/app.py#L349)

Clear the state of the application.

<a id="quixstreams.app.Application.run"></a>

#### run

```python
def run(dataframe: StreamingDataFrame)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/app.py#L378)

Start processing data from Kafka using provided `StreamingDataFrame`

**Arguments**:

- `dataframe`: instance of `StreamingDataFrame`

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

<a id="quixstreams.core.stream.functions"></a>

## quixstreams.core.stream.functions

<a id="quixstreams.core.stream.functions.Filter"></a>

#### Filter

```python
def Filter(func: Callable[[T], SupportsBool]) -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/functions.py#L65)

Wraps function into a "Filter" function.

The result of a Filter function is interpreted as boolean.
If it's `True`, the input will be return downstream.
If it's `False`, the `Filtered` exception will be raised to signal that the
value is filtered out.

**Arguments**:

- `func`: a function to filter value

**Returns**:

a Filter function

<a id="quixstreams.core.stream.functions.Update"></a>

#### Update

```python
def Update(func: StreamCallable) -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/functions.py#L88)

Wrap a function into "Update" function.

The provided function is expected to mutate the value.
Its result will always be ignored, and its input is passed
downstream.

**Arguments**:

- `func`: a function to mutate values

**Returns**:

an Update function

<a id="quixstreams.core.stream.functions.Apply"></a>

#### Apply

```python
def Apply(func: StreamCallable) -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/functions.py#L109)

Wrap a function into "Apply" function.

The provided function is expected to return a new value based on input,
and its result will always be passed downstream.

**Arguments**:

- `func`: a function to generate a new value

**Returns**:

an Apply function

<a id="quixstreams.core.stream.stream"></a>

## quixstreams.core.stream.stream

<a id="quixstreams.core.stream.stream.Stream"></a>

### Stream

```python
class Stream()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/stream.py#L16)

<a id="quixstreams.core.stream.stream.Stream.add_filter"></a>

#### add\_filter

```python
def add_filter(func: Callable[[T], R]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/stream.py#L82)

Add a function to filter values from the Stream.

The return value of the function will be interpreted as `bool`.
If the function returns `False`-like result, the Stream will raise `Filtered`
exception during execution.

**Arguments**:

- `func`: a function to filter values from the stream

**Returns**:

a new `Stream` derived from the current one

<a id="quixstreams.core.stream.stream.Stream.add_apply"></a>

#### add\_apply

```python
def add_apply(func: Callable[[T], R]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/stream.py#L95)

Add an "apply" function to the Stream.

The function is supposed to return a new value, which will be passed
further during execution.

**Arguments**:

- `func`: a function to generate a new value

**Returns**:

a new `Stream` derived from the current one

<a id="quixstreams.core.stream.stream.Stream.add_update"></a>

#### add\_update

```python
def add_update(func: Callable[[T], object]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/stream.py#L107)

Add an "update" function to the Stream, that will mutate the input value.

The return of this function will be ignored and its input
will be passed downstream.

**Arguments**:

- `func`: a function to mutate the value

**Returns**:

a new Stream derived from the current one

<a id="quixstreams.core.stream.stream.Stream.diff"></a>

#### diff

```python
def diff(other: "Stream") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/stream.py#L119)

Takes the difference between Streams `self` and `other` based on their last

common parent, and returns a new `Stream` that includes only this difference.

It's impossible to calculate a diff when:
 - Streams don't have a common parent.
 - When the `self` Stream already includes all the nodes from
    the `other` Stream, and the resulting diff is empty.

**Arguments**:

- `other`: a `Stream` to take a diff from.

**Raises**:

- `ValueError`: if Streams don't have a common parent
or if the diff is empty.

**Returns**:

new `Stream` instance including all the Streams from the diff

<a id="quixstreams.core.stream.stream.Stream.tree"></a>

#### tree

```python
def tree() -> List[Self]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/stream.py#L148)

Return a list of all parent Streams including the node itself.

The tree is ordered from child to parent (current node comes first).

**Returns**:

a list of `Stream` objects

<a id="quixstreams.core.stream.stream.Stream.compile"></a>

#### compile

```python
def compile(allow_filters: bool = True,
            allow_updates: bool = True) -> Callable[[T], R]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/stream.py#L162)

Compile a list of functions from this `Stream` and its parents into a single

big closure using a "compiler" function.

Closures are more performant than calling all the functions in the
`Stream.tree()` one-by-one.

**Arguments**:

- `allow_filters`: If False, this function will fail with ValueError if
the stream has filter functions in the tree. Default - True.
- `allow_updates`: If False, this function will fail with ValueError if
the stream has update functions in the tree. Default - True.

**Raises**:

- `ValueError`: if disallowed functions are present in the stream tree.

<a id="quixstreams.core.stream.stream.compiler"></a>

#### compiler

```python
def compiler(outer_func: Callable[[T], R]) -> Callable[[T], R]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/core/stream/stream.py#L219)

A function that wraps two other functions into a closure.

It passes the result of thje inner function as an input to the outer function.

It is used to transform (aka "compile") a list of functions into one large closure
like this:
```
[func, func, func] -> func(func(func()))
```

**Returns**:

a function with one argument (value)

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
Example:
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

<a id="quixstreams.exceptions.assignment"></a>

## quixstreams.exceptions.assignment

<a id="quixstreams.exceptions.assignment.PartitionAssignmentError"></a>

### PartitionAssignmentError

```python
class PartitionAssignmentError(QuixException)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/exceptions/assignment.py#L6)

Error happened during partition rebalancing.
Raised from `on_assign`, `on_revoke` and `on_lost` callbacks

<a id="quixstreams.kafka.consumer"></a>

## quixstreams.kafka.consumer

<a id="quixstreams.kafka.consumer.Consumer"></a>

### Consumer

```python
class Consumer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L66)

<a id="quixstreams.kafka.consumer.Consumer.poll"></a>

#### poll

```python
def poll(timeout: float = None) -> Optional[Message]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L124)

Consumes a single message, calls callbacks and returns events.

The application must check the returned :py:class:`Message`
object's :py:func:`Message.error()` method to distinguish between proper
messages (error() returns None), or an event or error.

Note: Callbacks may be called from this method, such as
``on_assign``, ``on_revoke``, et al.

**Arguments**:

- `timeout` (`float`): Maximum time in seconds to block waiting for message,
event or callback. Default: infinite.

**Raises**:

- `None`: RuntimeError if called on a closed consumer

**Returns**:

A Message object or None on timeout

<a id="quixstreams.kafka.consumer.Consumer.subscribe"></a>

#### subscribe

```python
def subscribe(topics: List[str],
              on_assign: Optional[RebalancingCallback] = None,
              on_revoke: Optional[RebalancingCallback] = None,
              on_lost: Optional[RebalancingCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L143)

Set subscription to supplied list of topics

This replaces a previous subscription.

**Arguments**:

- `topics` (`list(str)`): List of topics (strings) to subscribe to.
- `on_assign` (`callable`): callback to provide handling of customized offsets
on completion of a successful partition re-assignment.
- `on_revoke` (`callable`): callback to provide handling of offset commits to
a customized store on the start of a rebalance operation.
- `on_lost` (`callable`): callback to provide handling in the case the partition
assignment has been lost. Partitions that have been lost may already be
owned by other members in the group and therefore committing offsets,
for example, may fail.

**Raises**:

- `KafkaException`: 
- `None`: RuntimeError if called on a closed consumer
.. py:function:: on_assign(consumer, partitions)
.. py:function:: on_revoke(consumer, partitions)
.. py:function:: on_lost(consumer, partitions)

  :param Consumer consumer: Consumer instance.
  :param list(TopicPartition) partitions: Absolute list of partitions being
  assigned or revoked.

<a id="quixstreams.kafka.consumer.Consumer.unsubscribe"></a>

#### unsubscribe

```python
def unsubscribe()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L237)

Remove current subscription.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.store_offsets"></a>

#### store\_offsets

```python
def store_offsets(message: Optional[Message] = None,
                  offsets: List[TopicPartition] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L245)

.. py:function:: store_offsets([message=None], [offsets=None])

Store offsets for a message or a list of offsets.

``message`` and ``offsets`` are mutually exclusive. The stored offsets
will be committed according to 'auto.commit.interval.ms' or manual
offset-less `commit`.
Note that 'enable.auto.offset.store' must be set to False when using this API.

**Arguments**:

- `message` (`confluent_kafka.Message`): Store message's offset+1.
- `offsets` (`list(TopicPartition)`): List of topic+partitions+offsets to store.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.commit"></a>

#### commit

```python
def commit(message: Message = None,
           offsets: List[TopicPartition] = None,
           asynchronous: bool = True) -> Optional[List[TopicPartition]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L279)

Commit a message or a list of offsets.

The ``message`` and ``offsets`` parameters are mutually exclusive.
If neither is set, the current partition assignment's offsets are used instead.
Use this method to commit offsets if you have 'enable.auto.commit' set to False.

**Arguments**:

- `message` (`confluent_kafka.Message`): Commit the message's offset+1.
Note: By convention, committed offsets reflect the next message
to be consumed, **not** the last message consumed.
- `offsets` (`list(TopicPartition)`): List of topic+partitions+offsets to commit.
- `asynchronous` (`bool`): If true, asynchronously commit, returning None
immediately. If False, the commit() call will block until the commit
succeeds or fails and the committed offsets will be returned (on success).
Note that specific partitions may have failed and the .err field of
each partition should be checked for success.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.committed"></a>

#### committed

```python
def committed(partitions: List[TopicPartition],
              timeout: float = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L319)

.. py:function:: committed(partitions, [timeout=None])

Retrieve committed offsets for the specified partitions.

**Arguments**:

- `partitions` (`list(TopicPartition)`): List of topic+partitions to query for stored offsets.
- `timeout` (`float`): Request timeout (seconds).

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

**Returns**:

`list(TopicPartition)`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.get_watermark_offsets"></a>

#### get\_watermark\_offsets

```python
def get_watermark_offsets(partition: TopicPartition,
                          timeout: float = None,
                          cached: bool = False) -> Tuple[int, int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L339)

Retrieve low and high offsets for the specified partition.

**Arguments**:

- `partition` (`TopicPartition`): Topic+partition to return offsets for.
- `timeout` (`float`): Request timeout (seconds). Ignored if cached=True.
- `cached` (`bool`): Instead of querying the broker, use cached information.
Cached values: The low offset is updated periodically
(if statistics.interval.ms is set) while the high offset is updated on each
message fetched from the broker for this partition.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

**Returns**:

`tuple(int,int)`: Tuple of (low,high) on success or None on timeout.
The high offset is the offset of the last message + 1.

<a id="quixstreams.kafka.consumer.Consumer.list_topics"></a>

#### list\_topics

```python
def list_topics(topic: Optional[str] = None,
                timeout: float = -1) -> ClusterMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L365)

.. py:function:: list_topics([topic=None], [timeout=-1])

Request metadata from the cluster.
This method provides the same information as
listTopics(), describeTopics() and describeCluster() in  the Java Admin client.

**Arguments**:

- `topic` (`str`): If specified, only request information about this topic,
else return results for all topics in cluster.
Warning: If auto.create.topics.enable is set to true on the broker and
an unknown topic is specified, it will be created.
- `timeout` (`float`): The maximum response time before timing out,
or -1 for infinite timeout.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.memberid"></a>

#### memberid

```python
def memberid() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L386)

Return this client's broker-assigned group member id.

The member id is assigned by the group coordinator and is propagated to
the consumer during rebalance.

 :returns: Member id string or None
 :rtype: string
 :raises: RuntimeError if called on a closed consumer


<a id="quixstreams.kafka.consumer.Consumer.offsets_for_times"></a>

#### offsets\_for\_times

```python
def offsets_for_times(partitions: List[TopicPartition],
                      timeout: float = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L399)

Look up offsets by timestamp for the specified partitions.

The returned offset for each partition is the earliest offset whose
timestamp is greater than or equal to the given timestamp in the
corresponding partition. If the provided timestamp exceeds that of the
last message in the partition, a value of -1 will be returned.

 :param list(TopicPartition) partitions: topic+partitions with timestamps
    in the TopicPartition.offset field.
 :param float timeout: Request timeout (seconds).
 :returns: List of topic+partition with offset field set and possibly error set
 :rtype: list(TopicPartition)
 :raises: KafkaException
 :raises: RuntimeError if called on a closed consumer


<a id="quixstreams.kafka.consumer.Consumer.pause"></a>

#### pause

```python
def pause(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L427)

Pause consumption for the provided list of partitions.

**Arguments**:

- `partitions` (`list(TopicPartition)`): List of topic+partitions to pause.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.resume"></a>

#### resume

```python
def resume(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L437)

.. py:function:: resume(partitions)

Resume consumption for the provided list of partitions.

**Arguments**:

- `partitions` (`list(TopicPartition)`): List of topic+partitions to resume.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.position"></a>

#### position

```python
def position(partitions: List[TopicPartition]) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L449)

Retrieve current positions (offsets) for the specified partitions.

**Arguments**:

- `partitions` (`list(TopicPartition)`): List of topic+partitions to return
current offsets for. The current offset is the offset of
the last consumed message + 1.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

**Returns**:

`list(TopicPartition)`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.seek"></a>

#### seek

```python
def seek(partition: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L463)

Set consume position for partition to offset.

The offset may be an absolute (>=0) or a
logical offset (:py:const:`OFFSET_BEGINNING` et.al).

seek() may only be used to update the consume offset of an
actively consumed partition (i.e., after :py:const:`assign()`),
to set the starting offset of partition not being consumed instead
pass the offset in an `assign()` call.

**Arguments**:

- `partition` (`TopicPartition`): Topic+partition+offset to seek to.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.assignment"></a>

#### assignment

```python
def assignment() -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L480)

Returns the current partition assignment.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

**Returns**:

`list(TopicPartition)`: List of assigned topic+partitions.

<a id="quixstreams.kafka.consumer.Consumer.set_sasl_credentials"></a>

#### set\_sasl\_credentials

```python
def set_sasl_credentials(username: str, password: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L493)

Sets the SASL credentials used for this client.
These credentials will overwrite the old ones, and will be used the next
time the client needs to authenticate.
This method will not disconnect existing broker connections that have been
established with the old credentials.
This method is applicable only to SASL PLAIN and SCRAM mechanisms.

<a id="quixstreams.kafka.consumer.Consumer.close"></a>

#### close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/consumer.py#L505)

Close down and terminate the Kafka Consumer.

Actions performed:

- Stops consuming.
- Commits offsets, unless the consumer property 'enable.auto.commit' is set to False.
- Leaves the consumer group.

Registered callbacks may be called from this method,
see `poll()` for more info.


<a id="quixstreams.kafka.producer"></a>

## quixstreams.kafka.producer

<a id="quixstreams.kafka.producer.Producer"></a>

### Producer

```python
class Producer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/producer.py#L54)

<a id="quixstreams.kafka.producer.Producer.produce"></a>

#### produce

```python
def produce(topic: str,
            value: Union[str, bytes],
            key: Optional[Union[str, bytes]] = None,
            headers: Optional[Headers] = None,
            partition: Optional[int] = None,
            timestamp: Optional[int] = None,
            poll_timeout: float = 5.0,
            buffer_error_max_tries: int = 3)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/producer.py#L94)

Produce message to topic.

It also polls Kafka for callbacks before producing in order to minimize
the probability of `BufferError`.
If `BufferError` still happens, the method will poll Kafka with timeout
to free up the buffer and try again.

**Arguments**:

- `topic`: topic name
- `value`: message value
- `key`: message key
- `headers`: message headers
- `partition`: topic partition
- `timestamp`: message timestamp
- `poll_timeout`: timeout for `poll()` call in case of `BufferError`
- `buffer_error_max_tries`: max retries for `BufferError`.
Pass `0` to not retry after `BufferError`.

<a id="quixstreams.kafka.producer.Producer.poll"></a>

#### poll

```python
def poll(timeout: float = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/producer.py#L152)

Polls the producer for events and calls `on_delivery` callbacks.

**Arguments**:

- `timeout`: poll timeout seconds

<a id="quixstreams.kafka.producer.Producer.flush"></a>

#### flush

```python
def flush(timeout: float = None) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/kafka/producer.py#L159)

Wait for all messages in the Producer queue to be delivered.

**Arguments**:

- `timeout`: timeout is seconds

**Returns**:

number of messages delivered

<a id="quixstreams.logging"></a>

## quixstreams.logging

<a id="quixstreams.logging.configure_logging"></a>

#### configure\_logging

```python
def configure_logging(loglevel: Optional[LogLevel]) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/logging.py#L24)

Configure "quixstreams" logger.

.. note:: If "quixstreams" logger already has pre-defined handlers
(e.g. logging has already been configured via `logging`, or the function
is called twice), it will skip configuration and return `False`.

**Arguments**:

- `loglevel`: a valid log level as a string or None.
If None passed, this function is no-op and no logging will be configured.

**Returns**:

True if logging config has been updated, otherwise False.

<a id="quixstreams.models.messagecontext"></a>

## quixstreams.models.messagecontext

<a id="quixstreams.models.messagecontext.MessageContext"></a>

### MessageContext

```python
class MessageContext()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/messagecontext.py#L7)

An object with Kafka message properties.

It is made pseudo-immutable (i.e. public attributes don't have setters), and
it should not be mutated during message processing.

<a id="quixstreams.models.rows"></a>

## quixstreams.models.rows

<a id="quixstreams.models.rows.Row"></a>

### Row

```python
class Row()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/rows.py#L11)

Row is a dict-like interface on top of the message data + some Kafka props

<a id="quixstreams.models.rows.Row.keys"></a>

#### keys

```python
def keys() -> KeysView
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/rows.py#L73)

Also allows unpacking row.value via **row

<a id="quixstreams.models.rows.Row.clone"></a>

#### clone

```python
def clone(value: dict) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/rows.py#L85)

Manually clone the Row; doing it this way is much faster than doing a deepcopy
on the entire Row object.

<a id="quixstreams.models.serializers.base"></a>

## quixstreams.models.serializers.base

<a id="quixstreams.models.serializers.base.SerializationContext"></a>

### SerializationContext

```python
class SerializationContext()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/base.py#L22)

Provides additional context for message serialization/deserialization.

Every `Serializer` and `Deserializer` receives an instance of `SerializationContext`

<a id="quixstreams.models.serializers.base.SerializationContext.to_confluent_ctx"></a>

#### to\_confluent\_ctx

```python
def to_confluent_ctx(field: MessageField) -> _SerializationContext
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/base.py#L35)

Convert `SerializationContext` to `confluent_kafka.SerializationContext`

in order to re-use serialization already provided by `confluent_kafka` library.

**Arguments**:

- `field`: instance of `confluent_kafka.serialization.MessageField`

**Returns**:

instance of `confluent_kafka.serialization.SerializationContext`

<a id="quixstreams.models.serializers.base.Deserializer"></a>

### Deserializer

```python
class Deserializer(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/base.py#L47)

<a id="quixstreams.models.serializers.base.Deserializer.split_values"></a>

#### split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/base.py#L58)

Return True if the deserialized message should be considered as Iterable
and each item in it should be processed as a separate message.

<a id="quixstreams.models.serializers.base.Serializer"></a>

### Serializer

```python
class Serializer(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/base.py#L75)

A base class for all Serializers

<a id="quixstreams.models.serializers.base.Serializer.extra_headers"></a>

#### extra\_headers

```python
@property
def extra_headers() -> MessageHeadersMapping
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/base.py#L81)

Informs producer to set additional headers

for the message it will be serializing

Must return a dictionary with headers.
Keys must be strings, and values must be strings, bytes or None.

**Returns**:

dict with headers

<a id="quixstreams.models.serializers.exceptions"></a>

## quixstreams.models.serializers.exceptions

<a id="quixstreams.models.serializers.exceptions.IgnoreMessage"></a>

### IgnoreMessage

```python
class IgnoreMessage(exceptions.QuixException)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/exceptions.py#L51)

Raise this exception from Deserializer.__call__ in order to ignore the processing
of the particular message.

<a id="quixstreams.models.serializers.quix"></a>

## quixstreams.models.serializers.quix

<a id="quixstreams.models.serializers.quix.QuixDeserializer"></a>

### QuixDeserializer

```python
class QuixDeserializer(JSONDeserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L70)

<a id="quixstreams.models.serializers.quix.QuixDeserializer.split_values"></a>

#### split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L93)

Each Quix message might contain data for multiple Rows.
This property informs the downstream processors about that, so they can
expect an Iterable instead of Mapping.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.deserialize"></a>

#### deserialize

```python
def deserialize(model_key: str, value: Union[List[Mapping],
                                             Mapping]) -> Iterable[Mapping]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L146)

Deserialization function for particular data types (Timeseries or EventData).

**Arguments**:

- `model_key`: value of "__Q_ModelKey" message header
- `value`: deserialized JSON value of the message, list or dict

**Returns**:

Iterable of dicts

<a id="quixstreams.models.serializers.quix.QuixTimeseriesSerializer"></a>

### QuixTimeseriesSerializer

```python
class QuixTimeseriesSerializer(QuixSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L311)

Serialize data to JSON formatted according to Quix Timeseries format.

The serializable object must be dictionary, and each item must be of `str`, `int`,
`float`, `bytes` or `bytearray` type.
Otherwise, the `SerializationError` will be raised.

Example of the format:
    Input:
    ```
        {'a': 1, 'b': 1.1, 'c': "string", 'd': b'bytes', 'Tags': {'tag1': 'tag'}}
    ```

    Output:
    ```
    {
        "Timestamps" [123123123],
        "NumericValues: {"a": [1], "b": [1.1]},
        "StringValues": {"c": ["string"]},
        "BinaryValues: {"d": ["Ynl0ZXM="]},
        "TagValues": {"tag1": ["tag']}
    }
    ```

<a id="quixstreams.models.serializers.quix.QuixEventsSerializer"></a>

### QuixEventsSerializer

```python
class QuixEventsSerializer(QuixSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L400)

Serialize data to JSON formatted according to Quix EventData format.
The input value is expected to be a dictionary with the following keys:
- "Id" (type `str`, default - "")
- "Value" (type `str`, default - ""),
- "Tags" (type `dict`, default - {})

Note: All the other fields will be ignored.

**Example**:

  Input:
    ```
    {
        "Id": "an_event",
        "Value": "any_string"
        "Tags": {"tag1": "tag"}},
    }
    ```
  
  Output:
    ```
    {
        "Id": "an_event",
        "Value": "any_string",
        "Tags": {"tag1": "tag"}},
        "Timestamp":1692703362840389000
    }
    ```

<a id="quixstreams.models.serializers.simple_types"></a>

## quixstreams.models.serializers.simple\_types

<a id="quixstreams.models.serializers.simple_types.wrap_serialization_error"></a>

#### wrap\_serialization\_error

```python
def wrap_serialization_error(func)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L29)

A decorator to wrap `confluent_kafka.SerializationError` into our own type.

<a id="quixstreams.models.serializers.simple_types.BytesDeserializer"></a>

### BytesDeserializer

```python
class BytesDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L44)

A deserializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.BytesSerializer"></a>

### BytesSerializer

```python
class BytesSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L55)

A serializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.IntegerDeserializer"></a>

### IntegerDeserializer

```python
class IntegerDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L84)

Deserializes bytes to integers.

A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.

<a id="quixstreams.models.serializers.simple_types.DoubleDeserializer"></a>

### DoubleDeserializer

```python
class DoubleDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L103)

Deserializes float to IEEE 764 binary64.

A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.

<a id="quixstreams.models.serializers.simple_types.IntegerSerializer"></a>

### IntegerSerializer

```python
class IntegerSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L135)

Serializes integers to bytes

<a id="quixstreams.models.serializers.simple_types.DoubleSerializer"></a>

### DoubleSerializer

```python
class DoubleSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L148)

Serializes floats to bytes

<a id="quixstreams.models.timestamps"></a>

## quixstreams.models.timestamps

<a id="quixstreams.models.timestamps.TimestampType"></a>

### TimestampType

```python
class TimestampType(enum.IntEnum)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/timestamps.py#L9)

<a id="quixstreams.models.timestamps.TimestampType.TIMESTAMP_NOT_AVAILABLE"></a>

#### TIMESTAMP\_NOT\_AVAILABLE

timestamps not supported by broker

<a id="quixstreams.models.timestamps.TimestampType.TIMESTAMP_CREATE_TIME"></a>

#### TIMESTAMP\_CREATE\_TIME

message creation time (or source / producer time)

<a id="quixstreams.models.timestamps.TimestampType.TIMESTAMP_LOG_APPEND_TIME"></a>

#### TIMESTAMP\_LOG\_APPEND\_TIME

broker receive time

<a id="quixstreams.models.timestamps.MessageTimestamp"></a>

### MessageTimestamp

```python
class MessageTimestamp()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/timestamps.py#L15)

Represents a timestamp of incoming Kafka message.

It is made pseudo-immutable (i.e. public attributes don't have setters), and
it should not be mutated during message processing.

<a id="quixstreams.models.timestamps.MessageTimestamp.create"></a>

#### create

```python
@classmethod
def create(cls, timestamp_type: int, milliseconds: int) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/timestamps.py#L42)

Create a Timestamp object based on data

from `confluent_kafka.Message.timestamp()`.

If timestamp type is "TIMESTAMP_NOT_AVAILABLE", the milliseconds are set to None

**Arguments**:

- `timestamp_type`: a timestamp type represented as a number
Can be one of:
- "0" - TIMESTAMP_NOT_AVAILABLE, timestamps not supported by broker.
- "1" - TIMESTAMP_CREATE_TIME, message creation time (or source / producer time).
- "2" - TIMESTAMP_LOG_APPEND_TIME, broker receive time.
- `milliseconds`: the number of milliseconds since the epoch (UTC).

**Returns**:

Timestamp object

<a id="quixstreams.models.topics"></a>

## quixstreams.models.topics

<a id="quixstreams.models.topics.Topic"></a>

### Topic

```python
class Topic()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/topics.py#L58)

<a id="quixstreams.models.topics.Topic.name"></a>

#### name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/topics.py#L87)

Topic name

<a id="quixstreams.models.topics.Topic.row_serialize"></a>

#### row\_serialize

```python
def row_serialize(row: Row, key: Optional[Any] = None) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/topics.py#L93)

Serialize Row to a Kafka message structure

**Arguments**:

- `row`: Row to serialize
- `key`: message key to serialize, optional. Default - current Row key.

**Returns**:

KafkaMessage object with serialized values

<a id="quixstreams.models.topics.Topic.row_deserialize"></a>

#### row\_deserialize

```python
def row_deserialize(
        message: ConfluentKafkaMessageProto) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/topics.py#L116)

Deserialize incoming Kafka message to a Row.

**Arguments**:

- `message`: an object with interface of `confluent_kafka.Message`

**Returns**:

Row, list of Rows or None if the message is ignored.

<a id="quixstreams.models.types"></a>

## quixstreams.models.types

<a id="quixstreams.models.types.ConfluentKafkaMessageProto"></a>

### ConfluentKafkaMessageProto

```python
class ConfluentKafkaMessageProto(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/types.py#L10)

An interface of `confluent_kafka.Message`.

Use it to not depend on exact implementation and simplify testing.

Instances of `confluent_kafka.Message` cannot be directly created from Python,
see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

<a id="quixstreams.platforms.quix.api"></a>

## quixstreams.platforms.quix.api

<a id="quixstreams.platforms.quix.api.QuixPortalApiService"></a>

### QuixPortalApiService

```python
class QuixPortalApiService()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/api.py#L14)

A light wrapper around the Quix Portal Api. If used in the Quix Platform, it will
use that workspaces auth token and portal endpoint, else you must provide it.

Function names closely reflect the respective API endpoint,
each starting with the method [GET, POST, etc.] followed by the endpoint path.

Results will be returned in the form of request's Response.json(), unless something
else is required. Non-200's will raise exceptions.

See the swagger documentation for more info about the endpoints.

<a id="quixstreams.platforms.quix.checks"></a>

## quixstreams.platforms.quix.checks

<a id="quixstreams.platforms.quix.checks.check_state_management_enabled"></a>

#### check\_state\_management\_enabled

```python
def check_state_management_enabled()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/checks.py#L11)

Check if State Management feature is enabled for the current deployment on
Quix platform.
If it's disabled, the exception will be raised.

<a id="quixstreams.platforms.quix.checks.check_state_dir"></a>

#### check\_state\_dir

```python
def check_state_dir(state_dir: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/checks.py#L28)

Check if Application "state_dir" matches the state dir on Quix platform.

If it doesn't match, the warning will be logged.

**Arguments**:

- `state_dir`: application state_dir path

<a id="quixstreams.platforms.quix.config"></a>

## quixstreams.platforms.quix.config

<a id="quixstreams.platforms.quix.config.TopicCreationConfigs"></a>

### TopicCreationConfigs

```python
@dataclasses.dataclass
class TopicCreationConfigs()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L24)

<a id="quixstreams.platforms.quix.config.TopicCreationConfigs.name"></a>

#### name

Required when not created by a Quix App.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder"></a>

### QuixKafkaConfigsBuilder

```python
class QuixKafkaConfigsBuilder()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L33)

Retrieves all the necessary information from the Quix API and builds all the
objects required to connect a confluent-kafka client to the Quix Platform.

If not executed within the Quix platform directly, you must provide a Quix
"streaming" (aka "sdk") token, or Personal Access Token.

Ideally you also know your workspace name or id. If not, you can search for it
using a known topic name, but note the search space is limited to the access level
of your token.

It also currently handles the app_auto_create_topics setting for Application.Quix.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.append_workspace_id"></a>

#### append\_workspace\_id

```python
def append_workspace_id(s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L149)

Add the workspace ID to a given string, typically a topic or consumer group id

**Arguments**:

- `s`: the string to append to

**Returns**:

the string with workspace_id appended

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_for_workspace"></a>

#### search\_for\_workspace

```python
def search_for_workspace(
        workspace_name_or_id: Optional[str] = None) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L158)

Search for a workspace given an expected workspace name or id.

**Arguments**:

- `workspace_name_or_id`: the expected name or id of a workspace

**Returns**:

the workspace data dict if search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_workspace_info"></a>

#### get\_workspace\_info

```python
def get_workspace_info(known_workspace_topic: Optional[str] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L181)

Queries for workspace data from the Quix API, regardless of instance cache,

and updates instance attributes from query result.

**Arguments**:

- `known_workspace_topic`: a topic you know to exist in some workspace

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_workspace_for_topic"></a>

#### search\_workspace\_for\_topic

```python
def search_workspace_for_topic(workspace_id: str, topic: str) -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L208)

Search through all the topics in the given workspace id to see if there is a

match with the provided topic.

**Arguments**:

- `workspace_id`: the workspace to search in
- `topic`: the topic to search for

**Returns**:

the workspace_id if success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_for_topic_workspace"></a>

#### search\_for\_topic\_workspace

```python
def search_for_topic_workspace(topic: str) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L224)

Find what workspace a topic belongs to.

If there is only one workspace altogether, it is assumed to be the workspace.
More than one means each workspace will be searched until the first hit.

**Arguments**:

- `topic`: the topic to search for

**Returns**:

workspace data dict if topic search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_workspace_ssl_cert"></a>

#### get\_workspace\_ssl\_cert

```python
def get_workspace_ssl_cert(extract_to_folder: Optional[Path] = None) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L245)

Gets and extracts zipped certificate from the API to provided folder.

If no path was provided, will dump to /tmp. Expects cert named 'ca.cert'.

**Arguments**:

- `extract_to_folder`: path to folder to dump zipped cert file to

**Returns**:

full cert filepath as string

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.create_topics"></a>

#### create\_topics

```python
def create_topics(topics: Iterable[TopicCreationConfigs],
                  finalize_timeout_seconds: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L308)

Create topics in a Quix cluster.

**Arguments**:

- `topics`: an iterable with TopicCreationConfigs instances
- `finalize_timeout_seconds`: How long to wait for the topics to be
marked as "Ready" (and thus ready to produce to/consume from).

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.confirm_topics_exist"></a>

#### confirm\_topics\_exist

```python
def confirm_topics_exist(topics: Iterable[Union[Topic, TopicCreationConfigs]])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L354)

Confirm whether the desired set of topics exists in the Quix workspace.

**Arguments**:

- `topics`: an iterable with Either Topic or TopicCreationConfigs instances

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_confluent_broker_config"></a>

#### get\_confluent\_broker\_config

```python
def get_confluent_broker_config(known_topic: Optional[str] = None) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L394)

Get the full client config dictionary required to authenticate a confluent-kafka

client to a Quix platform broker/workspace.

The returned config can be used directly by any confluent-kafka-python consumer/
producer (add your producer/consumer-specific configs afterward).

**Arguments**:

- `known_topic`: a topic known to exist in some workspace

**Returns**:

a dict of confluent-kafka-python client settings (see librdkafka
config for more details)

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_confluent_client_configs"></a>

#### get\_confluent\_client\_configs

```python
def get_confluent_client_configs(
    topics: list,
    consumer_group_id: Optional[str] = None
) -> Tuple[dict, List[str], Optional[str]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/config.py#L416)

Get all the values you need in order to use a confluent_kafka-based client

with a topic on a Quix platform broker/workspace.

The returned config can be used directly by any confluent-kafka-python consumer/
producer (add your producer/consumer-specific configs afterward).

The topics and consumer group are appended with any necessary values.

**Arguments**:

- `topics`: list of topics
- `consumer_group_id`: consumer group id, if needed

**Returns**:

a tuple with configs and altered versions of the topics
and consumer group name

<a id="quixstreams.platforms.quix.env"></a>

## quixstreams.platforms.quix.env

<a id="quixstreams.platforms.quix.env.QuixEnvironment"></a>

### QuixEnvironment

```python
class QuixEnvironment()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/env.py#L7)

Class to access various Quix platform environment settings

<a id="quixstreams.platforms.quix.env.QuixEnvironment.state_management_enabled"></a>

#### state\_management\_enabled

```python
@property
def state_management_enabled() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/env.py#L19)

Check whether "State management" is enabled for the current deployment

**Returns**:

True if state management is enabled, otherwise False

<a id="quixstreams.platforms.quix.env.QuixEnvironment.deployment_id"></a>

#### deployment\_id

```python
@property
def deployment_id() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/env.py#L27)

Return current Quix deployment id.

This variable is meant to be set only by Quix Platform and only
when the application is deployed.

**Returns**:

deployment id or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.workspace_id"></a>

#### workspace\_id

```python
@property
def workspace_id() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/env.py#L39)

Return Quix workspace id if set

**Returns**:

workspace id or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.portal_api"></a>

#### portal\_api

```python
@property
def portal_api() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/env.py#L47)

Return Quix Portal API url if set

**Returns**:

portal API URL or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.sdk_token"></a>

#### sdk\_token

```python
@property
def sdk_token() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/env.py#L56)

Return Quix SDK token if set

**Returns**:

sdk token or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.state_dir"></a>

#### state\_dir

```python
@property
def state_dir() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/platforms/quix/env.py#L64)

Return application state directory on Quix.

**Returns**:

path to state dir

<a id="quixstreams.rowconsumer"></a>

## quixstreams.rowconsumer

<a id="quixstreams.rowconsumer.RowConsumer"></a>

### RowConsumer

```python
class RowConsumer(Consumer, RowConsumerProto)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/rowconsumer.py#L59)

<a id="quixstreams.rowconsumer.RowConsumer.subscribe"></a>

#### subscribe

```python
def subscribe(topics: List[Topic],
              on_assign: Optional[RebalancingCallback] = None,
              on_revoke: Optional[RebalancingCallback] = None,
              on_lost: Optional[RebalancingCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/rowconsumer.py#L115)

Set subscription to supplied list of topics.

This replaces a previous subscription.

This method also updates the internal mapping with topics that is used
to deserialize messages to Rows.

**Arguments**:

- `topics`: list of `Topic` instances to subscribe to.
- `on_assign` (`callable`): callback to provide handling of customized offsets
on completion of a successful partition re-assignment.
- `on_revoke` (`callable`): callback to provide handling of offset commits to
a customized store on the start of a rebalance operation.
- `on_lost` (`callable`): callback to provide handling in the case the partition
assignment has been lost. Partitions that have been lost may already be
owned by other members in the group and therefore committing offsets,
for example, may fail.

<a id="quixstreams.rowconsumer.RowConsumer.poll_row"></a>

#### poll\_row

```python
def poll_row(timeout: float = None) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/rowconsumer.py#L149)

Consumes a single message and deserialize it to Row or a list of Rows.

The message is deserialized according to the corresponding Topic.
If deserializer raises `IgnoreValue` exception, this method will return None.
If Kafka returns an error, it will be raised as exception.

**Arguments**:

- `timeout`: poll timeout seconds

**Returns**:

single Row, list of Rows or None

<a id="quixstreams.rowproducer"></a>

## quixstreams.rowproducer

<a id="quixstreams.rowproducer.RowProducer"></a>

### RowProducer

```python
class RowProducer(Producer, RowProducerProto)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/rowproducer.py#L25)

A producer class that is capable of serializing Rows to bytes and send them to Kafka.

The serialization is performed according to the Topic serialization settings.

    It overrides `.subscribe()` method of Consumer class to accept `Topic`
    objects instead of strings.

    :param broker_address: Kafka broker host and port in format `<host>:<port>`.
        Passed as `bootstrap.servers` to `confluent_kafka.Producer`.
    :param partitioner: A function to be used to determine the outgoing message
        partition.
        Available values: "random", "consistent_random", "murmur2", "murmur2_random",
        "fnv1a", "fnv1a_random"
        Default - "murmur2".
    :param extra_config: A dictionary with additional options that
        will be passed to `confluent_kafka.Producer` as is.
        Note: values passed as arguments override values in `extra_config`.
    :param on_error: a callback triggered when `RowProducer.produce_row()`
        or `RowProducer.poll()` fail`.
        If producer fails and the callback returns `True`, the exception
        will be logged but not propagated.
        The default callback logs an exception and returns `False`.


<a id="quixstreams.rowproducer.RowProducer.produce_row"></a>

#### produce\_row

```python
def produce_row(row: Row,
                topic: Topic,
                key: Optional[Any] = None,
                partition: Optional[int] = None,
                timestamp: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/rowproducer.py#L66)

Serialize Row to bytes according to the Topic serialization settings

and produce it to Kafka

If this method fails, it will trigger the provided "on_error" callback.

**Arguments**:

- `row`: Row object
- `topic`: Topic object
- `key`: message key, optional
- `partition`: partition number, optional
- `timestamp`: timestamp in milliseconds, optional

<a id="quixstreams.rowproducer.RowProducer.poll"></a>

#### poll

```python
def poll(timeout: float = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/rowproducer.py#L103)

Polls the producer for events and calls `on_delivery` callbacks.

If poll fails, it will trigger the provided "on_error" callback

**Arguments**:

- `timeout`: timeout in seconds

<a id="quixstreams.state.manager"></a>

## quixstreams.state.manager

<a id="quixstreams.state.manager.StateStoreManager"></a>

### StateStoreManager

```python
class StateStoreManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L27)

Class for managing state stores and partitions.

StateStoreManager is responsible for:
 - reacting to rebalance callbacks
 - managing the individual state stores
 - providing access to store transactions

<a id="quixstreams.state.manager.StateStoreManager.stores"></a>

#### stores

```python
@property
def stores() -> Dict[str, Dict[str, Store]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L63)

Map of registered state stores

**Returns**:

dict in format {topic: {store_name: store}}

<a id="quixstreams.state.manager.StateStoreManager.get_store"></a>

#### get\_store

```python
def get_store(topic: str,
              store_name: str = _DEFAULT_STATE_STORE_NAME) -> Store
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L70)

Get a store for given name and topic

**Arguments**:

- `topic`: topic name
- `store_name`: store name

**Returns**:

instance of `Store`

<a id="quixstreams.state.manager.StateStoreManager.register_store"></a>

#### register\_store

```python
def register_store(topic_name: str,
                   store_name: str = _DEFAULT_STATE_STORE_NAME)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L86)

Register a state store to be managed by StateStoreManager.

During processing, the StateStoreManager will react to rebalancing callbacks
and assign/revoke the partitions for registered stores.

Each store can be registered only once for each topic.

**Arguments**:

- `topic_name`: topic name
- `store_name`: store name

<a id="quixstreams.state.manager.StateStoreManager.clear_stores"></a>

#### clear\_stores

```python
def clear_stores()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L109)

Delete all state stores managed by StateStoreManager.

<a id="quixstreams.state.manager.StateStoreManager.on_partition_assign"></a>

#### on\_partition\_assign

```python
def on_partition_assign(tp: TopicPartition) -> List[StorePartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L124)

Assign store partitions for each registered store for the given `TopicPartition`

and return a list of assigned `StorePartition` objects.

**Arguments**:

- `tp`: `TopicPartition` from Kafka consumer

**Returns**:

list of assigned `StorePartition`

<a id="quixstreams.state.manager.StateStoreManager.on_partition_revoke"></a>

#### on\_partition\_revoke

```python
def on_partition_revoke(tp: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L138)

Revoke store partitions for each registered store for the given `TopicPartition`

**Arguments**:

- `tp`: `TopicPartition` from Kafka consumer

<a id="quixstreams.state.manager.StateStoreManager.on_partition_lost"></a>

#### on\_partition\_lost

```python
def on_partition_lost(tp: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L147)

Revoke and close store partitions for each registered store for the given

`TopicPartition`

**Arguments**:

- `tp`: `TopicPartition` from Kafka consumer

<a id="quixstreams.state.manager.StateStoreManager.init"></a>

#### init

```python
def init()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L157)

Initialize `StateStoreManager` and create a store directory


<a id="quixstreams.state.manager.StateStoreManager.close"></a>

#### close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L164)

Close all registered stores

<a id="quixstreams.state.manager.StateStoreManager.get_store_transaction"></a>

#### get\_store\_transaction

```python
def get_store_transaction(
        store_name: str = _DEFAULT_STATE_STORE_NAME) -> PartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L172)

Get active `PartitionTransaction` for the store

**Arguments**:

- `store_name`: 

<a id="quixstreams.state.manager.StateStoreManager.start_store_transaction"></a>

#### start\_store\_transaction

```python
@contextlib.contextmanager
def start_store_transaction(topic: str, partition: int,
                            offset: int) -> Iterator["_MultiStoreTransaction"]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/manager.py#L187)

Starting the multi-store transaction for the Kafka message.

This transaction will keep track of all used stores and flush them in the end.
If any exception is catched during this transaction, none of them
will be flushed as a best effort to keep stores consistent in "at-least-once" setting.

There can be only one active transaction at a time. Starting a new transaction
before the end of the current one will fail.

**Arguments**:

- `topic`: message topic
- `partition`: message partition
- `offset`: message offset

<a id="quixstreams.state.rocksdb.options"></a>

## quixstreams.state.rocksdb.options

<a id="quixstreams.state.rocksdb.options.RocksDBOptions"></a>

### RocksDBOptions

```python
@dataclasses.dataclass(frozen=True)
class RocksDBOptions(RocksDBOptionsType)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/options.py#L25)

Common RocksDB database options.

Please see `rocksdict.Options` for a complete description of each option.

To provide extra options that are not presented in this class, feel free
to override it and specify the additional values.

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.write_buffer_size"></a>

#### write\_buffer\_size

64MB

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.target_file_size_base"></a>

#### target\_file\_size\_base

64MB

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.block_cache_size"></a>

#### block\_cache\_size

128MB

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.to_options"></a>

#### to\_options

```python
def to_options() -> rocksdict.Options
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/options.py#L46)

Convert parameters to `rocksdict.Options`

**Returns**:

instance of `rocksdict.Options`

<a id="quixstreams.state.rocksdb.partition"></a>

## quixstreams.state.rocksdb.partition

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition"></a>

### RocksDBStorePartition

```python
class RocksDBStorePartition(StorePartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L50)

A base class to access state in RocksDB.

It represents a single RocksDB database.

Responsibilities:
 1. Managing access to the RocksDB instance
 2. Creating transactions to interact with data
 3. Flushing WriteBatches to the RocksDB

It opens the RocksDB on `__init__`. If the db is locked by another process,
it will retry according to `open_max_retries` and `open_retry_backoff`.

**Arguments**:

- `path`: an absolute path to the RocksDB folder
- `options`: RocksDB options. If `None`, the default options will be used.
- `open_max_retries`: number of times to retry opening the database
if it's locked by another process. To disable retrying, pass 0.
- `open_retry_backoff`: number of seconds to wait between each retry.

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.begin"></a>

#### begin

```python
def begin() -> "RocksDBPartitionTransaction"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L83)

Create a new `RocksDBTransaction` object.

Using `RocksDBTransaction` is a recommended way for accessing the data.

**Returns**:

an instance of `RocksDBTransaction`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.write"></a>

#### write

```python
def write(batch: rocksdict.WriteBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L94)

Write `WriteBatch` to RocksDB

**Arguments**:

- `batch`: an instance of `rocksdict.WriteBatch`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get"></a>

#### get

```python
def get(key: bytes, default: Any = None) -> Union[None, bytes, Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L101)

Get a key from RocksDB.

**Arguments**:

- `key`: a key encoded to `bytes`
- `default`: a default value to return if the key is not found.

**Returns**:

a value if the key is present in the DB. Otherwise, `None` or `default`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.exists"></a>

#### exists

```python
def exists(key: bytes) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L111)

Check if a key is present in the DB.

**Arguments**:

- `key`: a key encoded to `bytes`.

**Returns**:

`True` if the key is present, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get_processed_offset"></a>

#### get\_processed\_offset

```python
def get_processed_offset() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L120)

Get last processed offset for the given partition

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.close"></a>

#### close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L129)

Close the underlying RocksDB

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.path"></a>

#### path

```python
@property
def path() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L138)

Get absolute path to RocksDB database folder

**Returns**:

file path

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.destroy"></a>

#### destroy

```python
@classmethod
def destroy(cls, path: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L146)

Delete underlying RocksDB database

The database must be closed first.

**Arguments**:

- `path`: an absolute path to the RocksDB folder

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction"></a>

### RocksDBPartitionTransaction

```python
class RocksDBPartitionTransaction(PartitionTransaction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L221)

A transaction class to perform simple key-value operations like

"get", "set", "delete" and "exists" on a single RocksDB partition.

Serialization
*************
`RocksDBTransaction` automatically serializes keys and values to bytes.

Prefixing
*********
`RocksDBTransaction` allows to set prefixes for the keys in the given code block
using :meth:`with_prefix()` context manager.
Normally, `StreamingDataFrame` class will use message keys as prefixes
in order to namespace the stored keys across different messages.

Transactional properties
************************
`RocksDBTransaction` uses a combination of in-memory update cache
and RocksDB's WriteBatch in order to accumulate all the state mutations
in a single batch, flush them atomically, and allow the updates be visible
within the transaction before it's flushed (aka "read-your-own-writes" problem).

If any mutation fails during the transaction
(e.g. we failed to write the updates to the RocksDB), the whole transaction
will be marked as failed and cannot be used anymore.
In this case, a new `RocksDBTransaction` should be created.

`RocksDBTransaction` can be used only once.

**Arguments**:

- `partition`: instance of `RocksDBStatePartition` to be used for accessing
the underlying RocksDB
- `dumps`: a function to serialize data to bytes.
- `loads`: a function to deserialize data from bytes.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.with_prefix"></a>

#### with\_prefix

```python
@contextlib.contextmanager
def with_prefix(prefix: Any = b"") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L289)

A context manager set the prefix for all keys in the scope.

Normally, it's called by Streaming DataFrames engine to ensure that every
message key is stored separately.

The `with_prefix` calls should not be nested.
Only one prefix can be set at a time.

**Arguments**:

- `prefix`: a prefix string to be used.
Should be either `bytes` or object serializable to `bytes`
by `dumps` function.
The prefix doesn't need to contain the separator, it will be added
automatically between the key and the prefix if the prefix
is not empty.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.get"></a>

#### get

```python
@_validate_transaction_state
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L320)

Get a key from the store.

It first looks up the key in the update cache in case it has been updated
but not flushed yet.

It returns `None` if the key is not found and `default` is not provided.

**Arguments**:

- `key`: a key to get from DB
- `default`: value to return if the key is not present in the state.
It can be of any type.

**Returns**:

value or `default`

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.set"></a>

#### set

```python
@_validate_transaction_state
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L349)

Set a key to the store.

It first updates the key in the update cache.

**Arguments**:

- `key`: key to store in DB
- `value`: value to store in DB

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.delete"></a>

#### delete

```python
@_validate_transaction_state
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L370)

Delete a key from the store.

It first deletes the key from the update cache.

**Arguments**:

- `key`: key to delete from DB

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.exists"></a>

#### exists

```python
@_validate_transaction_state
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L387)

Check if a key exists in the store.

It first looks up the key in the update cache.

**Arguments**:

- `key`: a key to check in DB

**Returns**:

`True` if the key exists, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.completed"></a>

#### completed

```python
@property
def completed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L403)

Check if the transaction is completed.

It doesn't indicate whether transaction is successful or not.
Use `RocksDBTransaction.failed` for that.

The completed transaction should not be re-used.

**Returns**:

`True` if transaction is completed, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.failed"></a>

#### failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L417)

Check if the transaction has failed.

The failed transaction should not be re-used because the update cache
and

**Returns**:

`True` if transaction is failed, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.maybe_flush"></a>

#### maybe\_flush

```python
@_validate_transaction_state
def maybe_flush(offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/partition.py#L429)

Flush the recent updates to the database and empty the update cache.

It writes the WriteBatch to RocksDB and marks itself as finished.

If writing fails, the transaction will be also marked as "failed" and
cannot be used anymore.

.. note:: If no keys have been modified during the transaction
    (i.e no "set" or "delete" have been called at least once), it will
    not flush ANY data to the database including the offset in order to optimize
    I/O.

**Arguments**:

- `offset`: offset of the last processed message, optional.

<a id="quixstreams.state.rocksdb.store"></a>

## quixstreams.state.rocksdb.store

<a id="quixstreams.state.rocksdb.store.RocksDBStore"></a>

### RocksDBStore

```python
class RocksDBStore(Store)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/store.py#L18)

<a id="quixstreams.state.rocksdb.store.RocksDBStore.topic"></a>

#### topic

```python
@property
def topic() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/store.py#L52)

Store topic name

<a id="quixstreams.state.rocksdb.store.RocksDBStore.name"></a>

#### name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/store.py#L59)

Store name

<a id="quixstreams.state.rocksdb.store.RocksDBStore.partitions"></a>

#### partitions

```python
@property
def partitions() -> Dict[int, RocksDBStorePartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/store.py#L66)

Mapping of assigned store partitions

<a id="quixstreams.state.rocksdb.store.RocksDBStore.assign_partition"></a>

#### assign\_partition

```python
def assign_partition(partition: int) -> RocksDBStorePartition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/store.py#L72)

Open and assign store partition.

If the partition is already assigned, it will not re-open it and return
the existing partition instead.

**Arguments**:

- `partition`: partition number

**Returns**:

instance of`RocksDBStorePartition`

<a id="quixstreams.state.rocksdb.store.RocksDBStore.revoke_partition"></a>

#### revoke\_partition

```python
def revoke_partition(partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/store.py#L107)

Revoke and close the assigned store partition.

If the partition is not assigned, it will log the message and return.

**Arguments**:

- `partition`: partition number

<a id="quixstreams.state.rocksdb.store.RocksDBStore.start_partition_transaction"></a>

#### start\_partition\_transaction

```python
def start_partition_transaction(partition: int) -> RocksDBPartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/store.py#L128)

Start a new partition transaction.

`RocksDBPartitionTransaction` is the primary interface for working with data in
the underlying RocksDB.

**Arguments**:

- `partition`: partition number

**Returns**:

instance of `RocksDBPartitionTransaction`

<a id="quixstreams.state.rocksdb.store.RocksDBStore.close"></a>

#### close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/rocksdb/store.py#L150)

Close the store and revoke all assigned partitions

<a id="quixstreams.state.state"></a>

## quixstreams.state.state

<a id="quixstreams.state.state.TransactionState"></a>

### TransactionState

```python
class TransactionState(State)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/state.py#L6)

<a id="quixstreams.state.state.TransactionState.get"></a>

#### get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/state.py#L15)

Get the value for key if key is present in the state, else default

**Arguments**:

- `key`: key
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.state.TransactionState.set"></a>

#### set

```python
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/state.py#L25)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.state.TransactionState.delete"></a>

#### delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/state.py#L33)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.state.TransactionState.exists"></a>

#### exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/state.py#L42)

Check if the key exists in state.

**Arguments**:

- `key`: key

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.state.types"></a>

## quixstreams.state.types

<a id="quixstreams.state.types.Store"></a>

### Store

```python
class Store(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L9)

Abstract state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

<a id="quixstreams.state.types.Store.topic"></a>

#### topic

```python
@property
def topic() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L18)

Topic name

<a id="quixstreams.state.types.Store.name"></a>

#### name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L24)

Store name

<a id="quixstreams.state.types.Store.partitions"></a>

#### partitions

```python
@property
def partitions() -> Dict[int, "StorePartition"]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L30)

Mapping of assigned store partitions

**Returns**:

dict of "{partition: <StorePartition>}"

<a id="quixstreams.state.types.Store.assign_partition"></a>

#### assign\_partition

```python
def assign_partition(partition: int) -> "StorePartition"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L37)

Assign new store partition

**Arguments**:

- `partition`: partition number

**Returns**:

instance of `StorePartition`

<a id="quixstreams.state.types.Store.revoke_partition"></a>

#### revoke\_partition

```python
def revoke_partition(partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L46)

Revoke assigned store partition

**Arguments**:

- `partition`: partition number

<a id="quixstreams.state.types.Store.start_partition_transaction"></a>

#### start\_partition\_transaction

```python
def start_partition_transaction(
        partition: int) -> Optional["PartitionTransaction"]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L55)

Start a new partition transaction.

`PartitionTransaction` is the primary interface for working with data in Stores.

**Arguments**:

- `partition`: partition number

**Returns**:

instance of `PartitionTransaction`

<a id="quixstreams.state.types.Store.close"></a>

#### close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L66)

Close store and revoke all store partitions

<a id="quixstreams.state.types.StorePartition"></a>

### StorePartition

```python
class StorePartition(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L78)

A base class to access state in the underlying storage.
It represents a single instance of some storage (e.g. a single database for
the persistent storage).

<a id="quixstreams.state.types.StorePartition.begin"></a>

#### begin

```python
def begin() -> "PartitionTransaction"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L86)

State new `PartitionTransaction`

<a id="quixstreams.state.types.State"></a>

### State

```python
class State(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L95)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.types.State.get"></a>

#### get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L100)

Get the value for key if key is present in the state, else default

**Arguments**:

- `key`: key
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.types.State.set"></a>

#### set

```python
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L109)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.types.State.delete"></a>

#### delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L116)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.types.State.exists"></a>

#### exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L124)

Check if the key exists in state.

**Arguments**:

- `key`: key

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.state.types.PartitionTransaction"></a>

### PartitionTransaction

```python
class PartitionTransaction(State)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L132)

A transaction class to perform simple key-value operations like
"get", "set", "delete" and "exists" on a single storage partition.

<a id="quixstreams.state.types.PartitionTransaction.state"></a>

#### state

```python
@property
def state() -> State
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L139)

An instance of State to be provided to `StreamingDataFrame` functions


<a id="quixstreams.state.types.PartitionTransaction.failed"></a>

#### failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L146)

Return `True` if transaction failed to update data at some point.

Failed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.types.PartitionTransaction.completed"></a>

#### completed

```python
@property
def completed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L155)

Return `True` if transaction is completed.

Completed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.types.PartitionTransaction.with_prefix"></a>

#### with\_prefix

```python
def with_prefix(prefix: Any = b"") -> Iterator[Self]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L164)

A context manager set the prefix for all keys in the scope.

Normally, it's called by `StreamingDataFrame` internals to ensure that every
message key is stored separately.

**Arguments**:

- `prefix`: key prefix

**Returns**:

context maager

<a id="quixstreams.state.types.PartitionTransaction.maybe_flush"></a>

#### maybe\_flush

```python
def maybe_flush(offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/state/types.py#L174)

Flush the recent updates and last processed offset to the storage.

**Arguments**:

- `offset`: offset of the last processed message, optional.

<a id="quixstreams.utils.json"></a>

## quixstreams.utils.json

<a id="quixstreams.utils.json.dumps"></a>

#### dumps

```python
def dumps(value: Any) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/utils/json.py#L8)

Serialize to JSON using `orjson` package.

**Arguments**:

- `value`: value to serialize to JSON

**Returns**:

bytes

<a id="quixstreams.utils.json.loads"></a>

#### loads

```python
def loads(value: bytes) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/utils/json.py#L18)

Deserialize from JSON using `orjson` package.

Main differences:
- It returns `bytes`
- It doesn't allow non-str keys in dictionaries

**Arguments**:

- `value`: value to deserialize from

**Returns**:

object

