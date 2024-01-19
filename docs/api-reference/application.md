<a id="quixstreams.app"></a>

## quixstreams.app

<a id="quixstreams.app.Application"></a>

### Application

```python
class Application()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L43)

The main Application class.

Typically, the primary object needed to get a kafka application up and running.

Most functionality is explained the various methods, except for
"column assignment".



<br>
***What it Does:***

- During user setup:
    - Provides defaults or helper methods for commonly needed objects
    - For Quix Platform Users: Configures the app for it
        (see `Application.Quix()`)
- When executed via `.run()` (after setup):
    - Initializes Topics and StreamingDataFrames
    - Facilitates processing of Kafka messages with a `StreamingDataFrame`
    - Handles all Kafka client consumer/producer responsibilities.



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

app.run(dataframe=df)
```

<a id="quixstreams.app.Application.__init__"></a>

<br><br>

#### Application.\_\_init\_\_

```python
def __init__(broker_address: str,
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
             loglevel: Optional[LogLevel] = "INFO")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L82)


<br>
***Arguments:***

- `broker_address`: Kafka broker host and port in format `<host>:<port>`.
Passed as `bootstrap.servers` to `confluent_kafka.Consumer`.
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`
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

***Error Handlers***

To handle errors, `Application` accepts callbacks triggered when
    exceptions occur on different stages of stream processing. If the callback
    returns `True`, the exception will be ignored. Otherwise, the exception
    will be propagated and the processing will eventually stop.
- `on_consumer_error`: triggered when internal `RowConsumer` fails
to poll Kafka or cannot deserialize a message.
- `on_processing_error`: triggered when exception is raised within
`StreamingDataFrame.process()`.
- `on_producer_error`: triggered when RowProducer fails to serialize
or to produce a message to Kafka.

<a id="quixstreams.app.Application.Quix"></a>

<br><br>

#### Application.Quix

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L188)

Initialize an Application to work with Quix platform,

assuming environment is properly configured (by default in the platform).

It takes the credentials from the environment and configures consumer and
producer to properly connect to the Quix platform.

>***NOTE:*** Quix platform requires `consumer_group` and topic names to be
    prefixed with workspace id.
    If the application is created via `Application.Quix()`, the real consumer
    group will be `<workspace_id>-<consumer_group>`,
    and the real topic names will be `<workspace_id>-<topic_name>`.




<br>
***Example Snippet:***

```python
from quixstreams import Application

# Set up an `app = Application.Quix` and  `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything. Also shows off how to
# use the quix-specific serializers and deserializers.

app = Application.Quix()
input_topic = app.topic("topic-in", value_deserializer="quix")
output_topic = app.topic("topic-out", value_serializer="quix_timeseries")
df = app.dataframe(topic_in)
df = df.to_topic(output_topic)

app.run(dataframe=df)
```


<br>
***Arguments:***

- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`.
>***NOTE:*** The consumer group will be prefixed by Quix workspace id.
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

***Error Handlers***

To handle errors, `Application` accepts callbacks triggered when
    exceptions occur on different stages of stream processing. If the callback
    returns `True`, the exception will be ignored. Otherwise, the exception
    will be propagated and the processing will eventually stop.
- `on_consumer_error`: triggered when internal `RowConsumer` fails to poll
Kafka or cannot deserialize a message.
- `on_processing_error`: triggered when exception is raised within
`StreamingDataFrame.process()`.
- `on_producer_error`: triggered when RowProducer fails to serialize
or to produce a message to Kafka.


***Quix-specific Parameters***
- `quix_config_builder`: instance of `QuixKafkaConfigsBuilder` to be used
instead of the default one.
- `auto_create_topics`: Whether to auto-create any topics handed to a
StreamingDataFrame instance (topics_in + topics_out).


<br>
***Returns:***

`Application` object

<a id="quixstreams.app.Application.topic"></a>

<br><br>

#### Application.topic

```python
def topic(name: str,
          value_deserializer: DeserializerType = "json",
          key_deserializer: DeserializerType = "bytes",
          value_serializer: SerializerType = "json",
          key_serializer: SerializerType = "bytes",
          creation_configs: Optional[TopicCreationConfigs] = None,
          timestamp_extractor: Optional[TimestampExtractor] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L334)

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
- `creation_configs`: settings for auto topic creation (Quix platform only)
Its name will be overridden by this method's 'name' param.


<br>
***Returns:***

`Topic` object

<a id="quixstreams.app.Application.dataframe"></a>

<br><br>

#### Application.dataframe

```python
def dataframe(topic: Topic) -> StreamingDataFrame
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L400)

A simple helper method that generates a `StreamingDataFrame`, which is used

to define your message processing pipeline.

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

app.run(dataframe=df)
```


<br>
***Arguments:***

- `topic`: a `quixstreams.models.Topic` instance
to be used as an input topic.


<br>
***Returns:***

`StreamingDataFrame` object

<a id="quixstreams.app.Application.stop"></a>

<br><br>

#### Application.stop

```python
def stop()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L436)

Stop the internal poll loop and the message processing.

Only necessary when manually managing the lifecycle of the `Application` (
likely through some sort of threading).

To otherwise stop an application, either send a `SIGTERM` to the process
(like Kubernetes does) or perform a typical `KeyboardInterrupt` (`Ctrl+C`).

<a id="quixstreams.app.Application.get_producer"></a>

<br><br>

#### Application.get\_producer

```python
def get_producer() -> Producer
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L448)

Create and return a pre-configured Producer instance.
The Producer is initialized with params passed to Application.

It's useful for producing data to Kafka outside the standard Application processing flow,
(e.g. to produce test data into a topic).
Using this within the StreamingDataFrame functions is not recommended, as it creates a new Producer
instance each time, which is not optimized for repeated use in a streaming pipeline.


<br>
***Example Snippet:***

```python
from quixstreams import Application

app = Application.Quix(...)
topic = app.topic("input")

with app.get_producer() as producer:
    for i in range(100):
        producer.produce(topic=topic.name, key=b"key", value=b"value")
```

<a id="quixstreams.app.Application.get_consumer"></a>

<br><br>

#### Application.get\_consumer

```python
def get_consumer() -> Consumer
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L484)

Create and return a pre-configured Consumer instance.
The Consumer is initialized with params passed to Application.

It's useful for consuming data from Kafka outside the standard Application processing flow.
(e.g. to consume test data from a topic).
Using it within the StreamingDataFrame functions is not recommended, as it creates a new Consumer instance
each time, which is not optimized for repeated use in a streaming pipeline.

Note: By default this consumer does not autocommit consumed offsets to allow exactly-once processing.
To store the offset call store_offsets() after processing a message.
If autocommit is necessary set `enable.auto.offset.store` to True in the consumer config when creating the app.


<br>
***Example Snippet:***

```python
from quixstreams import Application

app = Application.Quix(...)
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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L533)

Clear the state of the application.

<a id="quixstreams.app.Application.run"></a>

<br><br>

#### Application.run

```python
def run(dataframe: StreamingDataFrame)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L562)

Start processing data from Kafka using provided `StreamingDataFrame`

One started, can be safely terminated with a `SIGTERM` signal
(like Kubernetes does) or a typical `KeyboardInterrupt` (`Ctrl+C`).



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

app.run(dataframe=df)
```


<br>
***Arguments:***

- `dataframe`: instance of `StreamingDataFrame`

<a id="quixstreams.state.types"></a>

## quixstreams.state.types

<a id="quixstreams.state.types.State"></a>

### State

```python
class State(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L102)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.types.State.get"></a>

<br><br>

#### State.get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L107)

Get the value for key if key is present in the state, else default


<br>
***Arguments:***

- `key`: key
- `default`: default value to return if the key is not found


<br>
***Returns:***

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.types.State.set"></a>

<br><br>

#### State.set

```python
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L116)

Set value for the key.


<br>
***Arguments:***

- `key`: key
- `value`: value

<a id="quixstreams.state.types.State.delete"></a>

<br><br>

#### State.delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L123)

Delete value for the key.

This function always returns `None`, even if value is not found.


<br>
***Arguments:***

- `key`: key

<a id="quixstreams.state.types.State.exists"></a>

<br><br>

#### State.exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L131)

Check if the key exists in state.


<br>
***Arguments:***

- `key`: key


<br>
***Returns:***

True if key exists, False otherwise

