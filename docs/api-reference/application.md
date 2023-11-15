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

