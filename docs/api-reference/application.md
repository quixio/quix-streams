<a id="quixstreams.app"></a>

## quixstreams.app

<a id="quixstreams.app.Application"></a>

### Application

```python
class Application()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L89)

The main Application class.

Typically, the primary object needed to get a kafka application up and running.

Most functionality is explained the various methods, except for
"column assignment".



<br>
***What it Does:***

- On init:
    - Provides defaults or helper methods for commonly needed objects
    - If `quix_sdk_token` is passed, configures the app to use the Quix Cloud.
- When executed via `.run()` (after setup):
    - Initializes Topics and StreamingDataFrames
    - Facilitates processing of Kafka messages with a `StreamingDataFrame`
    - Handles all Kafka client consumer/producer responsibilities.



<br>
***Example Snippet:***

```python
from quixstreams import Application

# Set up an `app = Application` and `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything.

app = Application(broker_address='localhost:9092', consumer_group='group')
topic = app.topic('test-topic')
df = app.dataframe(topic)
df.apply(lambda value, context: print('New message', value))

app.run()
```

<a id="quixstreams.app.Application.__init__"></a>

<br><br>

#### Application.\_\_init\_\_

```python
def __init__(broker_address: Optional[Union[str, ConnectionConfig]] = None,
             *,
             quix_sdk_token: Optional[str] = None,
             quix_portal_api: Optional[str] = None,
             consumer_group: Optional[str] = None,
             auto_offset_reset: AutoOffsetReset = "latest",
             commit_interval: float = 5.0,
             commit_every: int = 0,
             consumer_extra_config: Optional[dict] = None,
             producer_extra_config: Optional[dict] = None,
             state_dir: Union[None, str, Path] = None,
             rocksdb_options: Optional[RocksDBOptionsType] = None,
             on_consumer_error: Optional[ConsumerErrorCallback] = None,
             on_processing_error: Optional[ProcessingErrorCallback] = None,
             on_producer_error: Optional[ProducerErrorCallback] = None,
             on_message_processed: Optional[MessageProcessedCallback] = None,
             consumer_poll_timeout: float = 1.0,
             producer_poll_timeout: float = 0.0,
             loglevel: Optional[Union[int, LogLevel]] = "INFO",
             auto_create_topics: bool = True,
             use_changelog_topics: bool = True,
             quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
             topic_manager: Optional[TopicManager] = None,
             request_timeout: float = 30,
             topic_create_timeout: float = 60,
             processing_guarantee: ProcessingGuarantee = "at-least-once",
             max_partition_buffer_size: int = 10000)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L127)


<br>
***Arguments:***

- `broker_address`: Connection settings for Kafka.
Used by Producer, Consumer, and Admin clients.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
Either this OR `quix_sdk_token` must be set to use `Application` (not both).
Takes priority over quix auto-configuration.
Linked Environment Variable: `Quix__Broker__Address`.
Default: `None`
- `quix_sdk_token`: If using the Quix Cloud, the SDK token to connect with.
Either this OR `broker_address` must be set to use Application (not both).
Linked Environment Variable: `Quix__Sdk__Token`.
Default: None (if not run on Quix Cloud)
  >***NOTE:*** the environment variable is set for you in the Quix Cloud
- `quix_portal_api`: If using the Quix Cloud, the cluster API URL to use.
Use it to connect to the dedicated Quix Cloud environment.
Linked Environment Variable: `Quix__Portal__Api`.
Default: `https://portal-api.platform.quix.io/`.
  >***NOTE:*** the environment variable is set for you in the Quix Cloud
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`.
Linked Environment Variable: `Quix__Consumer_Group`.
Default - "quixstreams-default" (set during init)
  >***NOTE:*** Quix Applications will prefix it with the Quix workspace id.
- `commit_interval`: How often to commit the processed messages in seconds.
Default - 5.0.
- `commit_every`: Commit the checkpoint after processing N messages.
Use this parameter for more granular control of the commit schedule.
If the value is > 0, the application will commit the checkpoint after
processing the specified number of messages across all the assigned
partitions.
If the value is <= 0, only the `commit_interval` will be considered.
Default - 0.
    >***NOTE:*** Only input offsets are counted, and the application
    > may produce more results than the number of incoming messages.
- `auto_offset_reset`: Consumer `auto.offset.reset` setting
- `consumer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
- `producer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
- `state_dir`: path to the application state directory.
Linked Environment Variable: `Quix__State__Dir`.
Default - `"state"`.
- `rocksdb_options`: RocksDB options.
If `None`, the default options will be used.
- `consumer_poll_timeout`: timeout for `InternalConsumer.poll()`. Default - `1.0`s
- `producer_poll_timeout`: timeout for `InternalProducer.poll()`. Default - `0`s.
- `on_message_processed`: a callback triggered when message is successfully
processed.
- `loglevel`: a log level for "quixstreams" logger.
Should be a string or None.
If `None` is passed, no logging will be configured.
You may pass `None` and configure "quixstreams" logger
externally using `logging` library.
Default - `"INFO"`.
- `auto_create_topics`: Create all `Topic`s made via Application.topic()
Default - `True`
- `use_changelog_topics`: Use changelog topics to back stateful operations
Default - `True`
- `topic_manager`: A `TopicManager` instance
- `request_timeout`: timeout (seconds) for REST-based requests
- `topic_create_timeout`: timeout (seconds) for topic create finalization
- `processing_guarantee`: Use "exactly-once" or "at-least-once" processing.
- `max_partition_buffer_size`: the maximum number of messages to buffer per topic partition to consider it full.
The buffering is used to consume messages in-order between multiple partitions with the same number.
    It is a soft limit, and the actual number of buffered messages can be up to x2 higher.
    Lower value decreases the memory use, but increases the latency.
    Default - `10000`.

<br><br>***Error Handlers***<br>
To handle errors, `Application` accepts callbacks triggered when
    exceptions occur on different stages of stream processing. If the callback
    returns `True`, the exception will be ignored. Otherwise, the exception
    will be propagated and the processing will eventually stop.
- `on_consumer_error`: triggered when internal `InternalConsumer` fails
to poll Kafka or cannot deserialize a message.
- `on_processing_error`: triggered when exception is raised within
`StreamingDataFrame.process()`.
- `on_producer_error`: triggered when `InternalProducer` fails to serialize
or to produce a message to Kafka.
<br><br>***Quix Cloud Parameters***<br>
- `quix_config_builder`: instance of `QuixKafkaConfigsBuilder` to be used
instead of the default one.
> NOTE: It is recommended to just use `quix_sdk_token` instead.

<a id="quixstreams.app.Application.Quix"></a>

<br><br>

#### Application.Quix

```python
@classmethod
def Quix(cls, *args, **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L397)

RAISES EXCEPTION: DEPRECATED.

use Application() with "quix_sdk_token" parameter or set the "Quix__Sdk__Token"
environment variable.

<a id="quixstreams.app.Application.topic"></a>

<br><br>

#### Application.topic

```python
def topic(name: str,
          value_deserializer: DeserializerType = "json",
          key_deserializer: DeserializerType = "bytes",
          value_serializer: SerializerType = "json",
          key_serializer: SerializerType = "bytes",
          config: Optional[TopicConfig] = None,
          timestamp_extractor: Optional[TimestampExtractor] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L429)

Create a topic definition.

Allows you to specify serialization that should be used when consuming/producing
to the topic in the form of a string name (i.e. "json" for JSON) or a
serialization class instance directly, like JSONSerializer().



<br>
***Example Snippet:***

```python
from quixstreams import Application

# Specify an input and output topic for a `StreamingDataFrame` instance,
# where the output topic requires adjusting the key serializer.

app = Application()
input_topic = app.topic("input-topic", value_deserializer="json")
output_topic = app.topic(
    "output-topic", key_serializer="str", value_serializer=JSONSerializer()
)
sdf = app.dataframe(input_topic)
sdf.to_topic(output_topic)
```


<br>
***Arguments:***

- `name`: topic name
>***NOTE:*** If the application is created via `Quix.Application()`,
the topic name will be prefixed by Quix workspace id, and it will
be `<workspace_id>-<name>`
- `value_deserializer`: a deserializer type for values; default="json"
- `key_deserializer`: a deserializer type for keys; default="bytes"
- `value_serializer`: a serializer type for values; default="json"
- `key_serializer`: a serializer type for keys; default="bytes"
- `config`: optional topic configurations (for creation/validation)
>***NOTE:*** will not create without Application's auto_create_topics set
to True (is True by default)
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message. Default - `None`.


<br>
***Example Snippet:***

```python
app = Application(...)


def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    return value["timestamp"]

topic = app.topic("input-topic", timestamp_extractor=custom_ts_extractor)
```


<br>
***Returns:***

`Topic` object

<a id="quixstreams.app.Application.dataframe"></a>

<br><br>

#### Application.dataframe

```python
def dataframe(topic: Optional[Topic] = None,
              source: Optional[BaseSource] = None) -> StreamingDataFrame
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L509)

A simple helper method that generates a `StreamingDataFrame`, which is used

to define your message processing pipeline.

The topic is what the `StreamingDataFrame` will use as its input, unless
a source is provided (`topic` is optional when using a `source`).

If both `topic` AND `source` are provided, the source will write to that topic
instead of its default topic (which the `StreamingDataFrame` then consumes).

See :class:`quixstreams.dataframe.StreamingDataFrame` for more details.


<br>
***Example Snippet:***

```python
from quixstreams import Application

# Set up an `app = Application` and  `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything.

app = Application(broker_address='localhost:9092', consumer_group='group')
topic = app.topic('test-topic')
df = app.dataframe(topic)
df.apply(lambda value, context: print('New message', value)

app.run()
```


<br>
***Arguments:***

- `topic`: a `quixstreams.models.Topic` instance
to be used as an input topic.
- `source`: a `quixstreams.sources` "BaseSource" instance


<br>
***Returns:***

`StreamingDataFrame` object

<a id="quixstreams.app.Application.stop"></a>

<br><br>

#### Application.stop

```python
def stop(fail: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L565)

Stop the internal poll loop and the message processing.

Only necessary when manually managing the lifecycle of the `Application` (
likely through some sort of threading).

To otherwise stop an application, either send a `SIGTERM` to the process
(like Kubernetes does) or perform a typical `KeyboardInterrupt` (`Ctrl+C`).


<br>
***Arguments:***

- `fail`: if True, signals that application is stopped due
to unhandled exception, and it shouldn't commit the current checkpoint.

<a id="quixstreams.app.Application.get_producer"></a>

<br><br>

#### Application.get\_producer

```python
def get_producer(transactional: bool = False) -> Producer
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L610)

Create and return a pre-configured Producer instance.

The Producer is initialized with params passed to Application.

It's useful for producing data to Kafka outside the standard Application processing flow,
(e.g. to produce test data into a topic).
Using this within the StreamingDataFrame functions is not recommended, as it creates a new Producer
instance each time, which is not optimized for repeated use in a streaming pipeline.


<br>
***Arguments:***

- `transactional`: if True, the producer will be configured to use transactions
regardless of Application's processing guarantee setting. But the responsibility
    for beginning and committing the transaction is on the user.
    Default - False.


<br>
***Example Snippet:***

```python
from quixstreams import Application

app = Application(...)
topic = app.topic("input")

with app.get_producer() as producer:
    for i in range(100):
        producer.produce(topic=topic.name, key=b"key", value=b"value")
```

<a id="quixstreams.app.Application.get_consumer"></a>

<br><br>

#### Application.get\_consumer

```python
def get_consumer(auto_commit_enable: bool = True) -> Consumer
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L678)

Create and return a pre-configured Consumer instance.

The Consumer is initialized with params passed to Application.

It's useful for consuming data from Kafka outside the standard
Application processing flow.
(e.g., to consume test data from a topic).
Using it within the StreamingDataFrame functions is not recommended, as it
creates a new Consumer instance
each time, which is not optimized for repeated use in a streaming pipeline.

Note: By default, this consumer does not autocommit the consumed offsets to allow
at-least-once processing.
To store the offset call store_offsets() after processing a message.
If autocommit is necessary set `enable.auto.offset.store` to True in
the consumer config when creating the app.


<br>
***Example Snippet:***



<br>
***Arguments:***

- `auto_commit_enable`: Enable or disable auto commit
Default - True
```python
from quixstreams import Application

app = Application(...)
topic = app.topic("input")

with app.get_consumer() as consumer:
    consumer.subscribe([topic.name])
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None:
            # Process message
            # Optionally commit the offset
            # consumer.store_offsets(msg)

```

<a id="quixstreams.app.Application.clear_state"></a>

<br><br>

#### Application.clear\_state

```python
def clear_state()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L727)

Clear the state of the application.

<a id="quixstreams.app.Application.add_source"></a>

<br><br>

#### Application.add\_source

```python
def add_source(source: BaseSource, topic: Optional[Topic] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L733)

Add a source to the application.

Use when no transformations (which requires a `StreamingDataFrame`) are needed.

See :class:`quixstreams.sources.base.BaseSource` for more details.


<br>
***Arguments:***

- `source`: a :class:`quixstreams.sources.BaseSource` instance
- `topic`: the :class:`quixstreams.models.Topic` instance the source will produce to
Default - the topic generated by the `source.default_topic()` method.
Note: the names of default topics are prefixed with "source__".

<a id="quixstreams.app.Application.run"></a>

<br><br>

#### Application.run

```python
def run(dataframe: Optional[StreamingDataFrame] = None,
        timeout: float = 0.0,
        count: int = 0,
        collect: bool = True,
        metadata: bool = False) -> list[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L766)

Start processing data from Kafka using provided `StreamingDataFrame`

Once started, it can be safely terminated with a `SIGTERM` signal
(like Kubernetes does) or a typical `KeyboardInterrupt` (`Ctrl+C`).

Alternatively, stop conditions can be set (typically for debugging purposes);
    has the option of stopping after a number of outputs, timeout, or both.

Not setting a timeout or count limit will result in the Application running
  indefinitely (expected production behavior).


Stop Condition Details:

A `timeout` will immediately stop an Application once no new messages have
been consumed after T seconds (after rebalance and recovery).

A `count` will make the application to wait until N total outputs
are processed from all the input topics after an initial rebalance
and recovery.
Note that each message may produce from 0 to N outputs depending
on the processing code.

If `timeout` is not set, the Application runs until the `count` is hit.

If `timeout` and `count` are used together (which is the recommended pattern for
debugging), either condition will trigger a stop.



<br>
***Example Snippet:***

```python
from quixstreams import Application

# Set up an `app = Application` and  `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything.

app = Application(broker_address='localhost:9092', consumer_group='group')
topic = app.topic('test-topic')
df = app.dataframe(topic)
df.apply(lambda value, context: print('New message', value)

app.run()  # could pass `timeout=5` here, for example
```


<br>
***Arguments:***

- `dataframe`: DEPRECATED - do not use; sdfs are now automatically tracked.
- `timeout`: maximum time to wait for a new message.
Default: 0.0 (infinite)
- `count`: stop the application after processing N outputs.
Default: 0 (infinite)
- `collect`: if True, collect the outputs and return them as a list of dictionaries
in the format defined by the `metadata` parameter.
This parameter is effective only when `timeout` or `count` are passed.
Default: `True`.
- `metadata`: if True, the collected outputs will contain values, keys,
timestamps, offsets, topics and partitions.
Otherwise, only values are collected.
This parameter is effective only if `collect=True` and `timeout` or `count` are passed.
Default - `False`.

<a id="quixstreams.app.ApplicationConfig"></a>

### ApplicationConfig

```python
class ApplicationConfig(BaseSettings)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L1124)

Immutable object holding the application configuration

For details see :class:`quixstreams.Application`

<a id="quixstreams.app.ApplicationConfig.settings_customise_sources"></a>

<br><br>

#### ApplicationConfig.settings\_customise\_sources

```python
@classmethod
def settings_customise_sources(
    cls, settings_cls: Type[PydanticBaseSettings],
    init_settings: PydanticBaseSettingsSource,
    env_settings: PydanticBaseSettingsSource,
    dotenv_settings: PydanticBaseSettingsSource,
    file_secret_settings: PydanticBaseSettingsSource
) -> Tuple[PydanticBaseSettingsSource, ...]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L1160)

Included to ignore reading/setting values from the environment

<a id="quixstreams.app.ApplicationConfig.copy"></a>

<br><br>

#### ApplicationConfig.copy

```python
def copy(**kwargs) -> "ApplicationConfig"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L1173)

Update the application config and return a copy

<a id="quixstreams.app.resolve_transactional_id"></a>

<br><br>

#### resolve\_transactional\_id

```python
def resolve_transactional_id(transactional_id: Optional[str],
                             prefix: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L1195)

Utility function to resolve the transactional.id based
on existing config and provided prefix.

