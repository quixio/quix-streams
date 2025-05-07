<a id="quixstreams.sources.base.source"></a>

## quixstreams.sources.base.source

<a id="quixstreams.sources.base.source.BaseSource"></a>

### BaseSource

```python
class BaseSource(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L28)

This is the base class for all sources.

Sources are executed in a sub-process of the main application.

To create your own source you need to implement:

* `start`
* `stop`
* `default_topic`

`BaseSource` is the most basic interface, and the framework expects every
source to implement it.
Use `Source` to benefit from a base implementation.

You can connect a source to a StreamingDataframe using the Application.

Example snippet:

```python
class RandomNumbersSource(BaseSource):
def __init__(self):
    super().__init__()
    self._running = False

def start(self):
    self._running = True

    while self._running:
        number = random.randint(0, 100)
        serialized = self._producer_topic.serialize(value=number)
        self._producer.produce(
            topic=self._producer_topic.name,
            key=serialized.key,
            value=serialized.value,
        )

def stop(self):
    self._running = False

def default_topic(self) -> Topic:
    return Topic(
        name="topic-name",
        value_deserializer="json",
        value_serializer="json",
    )


def main():
    app = Application(broker_address="localhost:9092")
    source = RandomNumbersSource()

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()
```

<a id="quixstreams.sources.base.source.BaseSource.configure"></a>

<br><br>

#### BaseSource.configure

```python
def configure(topic: Topic, producer: InternalProducer, **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L116)

This method is triggered before the source is started.

It configures the source's Kafka producer, the topic it will produce to and optional dependencies.

<a id="quixstreams.sources.base.source.BaseSource.setup"></a>

<br><br>

#### BaseSource.setup

```python
def setup()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L125)

When applicable, set up the client here along with any validation to affirm a
valid/successful authentication/connection.

<a id="quixstreams.sources.base.source.BaseSource.start"></a>

<br><br>

#### BaseSource.start

```python
@abstractmethod
def start() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L145)

This method is triggered in the subprocess when the source is started.

The subprocess will run as long as the start method executes.
Use it to fetch data and produce it to Kafka.

<a id="quixstreams.sources.base.source.BaseSource.stop"></a>

<br><br>

#### BaseSource.stop

```python
@abstractmethod
def stop() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L154)

This method is triggered when the application is shutting down.

The source must ensure that the `run` method is completed soon.

<a id="quixstreams.sources.base.source.BaseSource.default_topic"></a>

<br><br>

#### BaseSource.default\_topic

```python
@abstractmethod
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L162)

This method is triggered when the topic is not provided to the source.

The source must return a default topic configuration.

Note: if the default topic is used, the Application will prefix its name with "source__".

<a id="quixstreams.sources.base.source.Source"></a>

### Source

```python
class Source(BaseSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L172)

A base class for custom Sources that provides a basic implementation of `BaseSource`
interface.
It is recommended to interface to create custom sources.

Subclass it and implement the `run` method to fetch data and produce it to Kafka.

**Example**:

  
```python
import random
import time

from quixstreams import Application
from quixstreams.sources import Source


class RandomNumbersSource(Source):
    def run(self):
        while self.running:
            number = random.randint(0, 100)
            serialized = self._producer_topic.serialize(value=number)
            self.produce(key=str(number), value=serialized.value)
            time.sleep(0.5)


def main():
    app = Application(broker_address="localhost:9092")
    source = RandomNumbersSource(name="random-source")

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()
```
  
  
  Helper methods and properties:
  
  * `serialize()`
  * `produce()`
  * `flush()`
  * `running`

<a id="quixstreams.sources.base.source.Source.__init__"></a>

<br><br>

#### Source.\_\_init\_\_

```python
def __init__(
    name: str,
    shutdown_timeout: float = 10,
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L222)


<br>
***Arguments:***

- `name`: The source unique name. It is used to generate the topic configuration.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.base.source.Source.running"></a>

<br><br>

#### Source.running

```python
@property
def running() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L251)

Property indicating if the source is running.

The `stop` method will set it to `False`. Use it to stop the source gracefully.

<a id="quixstreams.sources.base.source.Source.cleanup"></a>

<br><br>

#### Source.cleanup

```python
def cleanup(failed: bool) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L259)

This method is triggered once the `run` method completes.

Use it to clean up the resources and shut down the source gracefully.

It flushes the producer when `_run` completes successfully.

<a id="quixstreams.sources.base.source.Source.stop"></a>

<br><br>

#### Source.stop

```python
def stop() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L270)

This method is triggered when the application is shutting down.

It sets the `running` property to `False`.

<a id="quixstreams.sources.base.source.Source.start"></a>

<br><br>

#### Source.start

```python
def start() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L278)

This method is triggered in the subprocess when the source is started.

It marks the source as running, execute it's run method and ensure cleanup happens.

<a id="quixstreams.sources.base.source.Source.run"></a>

<br><br>

#### Source.run

```python
@abstractmethod
def run()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L295)

This method is triggered in the subprocess when the source is started.

The subprocess will run as long as the run method executes.
Use it to fetch data and produce it to Kafka.

<a id="quixstreams.sources.base.source.Source.serialize"></a>

<br><br>

#### Source.serialize

```python
def serialize(key: Optional[object] = None,
              value: Optional[object] = None,
              headers: Optional[Headers] = None,
              timestamp_ms: Optional[int] = None) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L303)

Serialize data to bytes using the producer topic serializers and return a `quixstreams.models.messages.KafkaMessage`.


<br>
***Returns:***

`quixstreams.models.messages.KafkaMessage`

<a id="quixstreams.sources.base.source.Source.produce"></a>

<br><br>

#### Source.produce

```python
def produce(value: Optional[Union[str, bytes]] = None,
            key: Optional[Union[str, bytes]] = None,
            headers: Optional[Headers] = None,
            partition: Optional[int] = None,
            timestamp: Optional[int] = None,
            poll_timeout: float = PRODUCER_POLL_TIMEOUT,
            buffer_error_max_tries: int = PRODUCER_ON_ERROR_RETRIES) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L319)

Produce a message to the configured source topic in Kafka.

<a id="quixstreams.sources.base.source.Source.flush"></a>

<br><br>

#### Source.flush

```python
def flush(timeout: Optional[float] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L344)

This method flush the producer.

It ensures all messages are successfully delivered to Kafka.


<br>
***Arguments:***

- `timeout` (`float`): time to attempt flushing (seconds).
None use producer default or -1 is infinite. Default: None

**Raises**:

- `CheckpointProducerTimeout`: if any message fails to produce before the timeout

<a id="quixstreams.sources.base.source.Source.default_topic"></a>

<br><br>

#### Source.default\_topic

```python
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L362)

Return a default topic matching the source name.

The default topic will not be used if the topic has already been provided to the source.

Note: if the default topic is used, the Application will prefix its name with "source__".


<br>
***Returns:***

`quixstreams.models.topics.Topic`

<a id="quixstreams.sources.base.source.StatefulSource"></a>

### StatefulSource

```python
class StatefulSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L381)

A `Source` class for custom Sources that need a state.

Subclasses are responsible for flushing, by calling `flush`, at reasonable intervals.

**Example**:

  
```python
import random
import time

from quixstreams import Application
from quixstreams.sources import StatefulSource


class RandomNumbersSource(StatefulSource):
    def run(self):

        i = 0
        while self.running:
            previous = self.state.get("number", 0)
            current = random.randint(0, 100)
            self.state.set("number", current)

            serialized = self._producer_topic.serialize(value=current + previous)
            self.produce(key=str(current), value=serialized.value)
            time.sleep(0.5)

            # flush the state every 10 messages
            i += 1
            if i % 10 == 0:
                self.flush()


def main():
    app = Application(broker_address="localhost:9092")
    source = RandomNumbersSource(name="random-source")

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()
```

<a id="quixstreams.sources.base.source.StatefulSource.__init__"></a>

<br><br>

#### StatefulSource.\_\_init\_\_

```python
def __init__(
    name: str,
    shutdown_timeout: float = 10,
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L431)


<br>
***Arguments:***

- `name`: The source unique name. It is used to generate the topic configuration.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.base.source.StatefulSource.configure"></a>

<br><br>

#### StatefulSource.configure

```python
def configure(topic: Topic,
              producer: InternalProducer,
              *,
              store_partition: Optional[StorePartition] = None,
              **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L458)

This method is triggered before the source is started.

It configures the source's Kafka producer, the topic it will produce to and the store partition.

<a id="quixstreams.sources.base.source.StatefulSource.store_partitions_count"></a>

<br><br>

#### StatefulSource.store\_partitions\_count

```python
@property
def store_partitions_count() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L477)

Count of store partitions.

Used to configure the number of partition in the changelog topic.

<a id="quixstreams.sources.base.source.StatefulSource.assigned_store_partition"></a>

<br><br>

#### StatefulSource.assigned\_store\_partition

```python
@property
def assigned_store_partition() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L486)

The store partition assigned to this instance

<a id="quixstreams.sources.base.source.StatefulSource.store_name"></a>

<br><br>

#### StatefulSource.store\_name

```python
@property
def store_name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L493)

The source store name

<a id="quixstreams.sources.base.source.StatefulSource.state"></a>

<br><br>

#### StatefulSource.state

```python
@property
def state() -> State
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L500)

Access the `State` of the source.

The `State` lifecycle is tied to the store transaction. A transaction is only valid until the next `.flush()` call. If no valid transaction exist, a new transaction is created.

Important: after each `.flush()` call, a previously returned instance is invalidated and cannot be used. The property must be called again.

<a id="quixstreams.sources.base.source.StatefulSource.flush"></a>

<br><br>

#### StatefulSource.flush

```python
def flush(timeout: Optional[float] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L519)

This method commit the state and flush the producer.

It ensures the state is published to the changelog topic and all messages are successfully delivered to Kafka.


<br>
***Arguments:***

- `timeout` (`float`): time to attempt flushing (seconds).
None use producer default or -1 is infinite. Default: None

**Raises**:

- `CheckpointProducerTimeout`: if any message fails to produce before the timeout

<a id="quixstreams.sources.core.csv"></a>

## quixstreams.sources.core.csv

<a id="quixstreams.sources.core.csv.CSVSource"></a>

### CSVSource

```python
class CSVSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/csv.py#L13)

<a id="quixstreams.sources.core.csv.CSVSource.__init__"></a>

<br><br>

#### CSVSource.\_\_init\_\_

```python
def __init__(path: Union[str, Path],
             name: str,
             key_extractor: Optional[Callable[[dict], Union[str,
                                                            bytes]]] = None,
             timestamp_extractor: Optional[Callable[[dict], int]] = None,
             delay: float = 0,
             shutdown_timeout: float = 10,
             dialect: str = "excel") -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/csv.py#L14)

A base CSV source that reads data from a CSV file and produces rows

to the Kafka topic in JSON format.


<br>
***Arguments:***

- `path`: a path to the CSV file.
- `name`: a unique name for the Source.
It is used as a part of the default topic name.
- `key_extractor`: an optional callable to extract the message key from the row.
It must return either `str` or `bytes`.
If empty, the Kafka messages will be produced without keys.
Default - `None`.
- `timestamp_extractor`: an optional callable to extract the message timestamp from the row.
It must return time in milliseconds as `int`.
If empty, the current epoch will be used.
Default - `None`
- `delay`: an optional delay after producing each row for stream simulation.
Default - `0`.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shut down.
- `dialect`: a CSV dialect to use. It affects quoting and delimiters.
See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
Default - `"excel"`.

<a id="quixstreams.sources.core.kafka.kafka"></a>

## quixstreams.sources.core.kafka.kafka

<a id="quixstreams.sources.core.kafka.kafka.KafkaReplicatorSource"></a>

### KafkaReplicatorSource

```python
class KafkaReplicatorSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/kafka.py#L27)

Source implementation that replicates a topic from a Kafka broker to your application broker.

Running multiple instances of this source is supported.


<br>
***Example Snippet:***

```python
from quixstreams import Application
from quixstreams.sources.kafka import KafkaReplicatorSource

app = Application(
    consumer_group="group",
)

source = KafkaReplicatorSource(
    name="source-second-kafka",
    app_config=app.config,
    topic="second-kafka-topic",
    broker_address="localhost:9092",
)

sdf = app.dataframe(source=source)
sdf = sdf.print()
app.run()
```

<a id="quixstreams.sources.core.kafka.kafka.KafkaReplicatorSource.__init__"></a>

<br><br>

#### KafkaReplicatorSource.\_\_init\_\_

```python
def __init__(
    name: str,
    app_config: "ApplicationConfig",
    topic: str,
    broker_address: Union[str, ConnectionConfig],
    auto_offset_reset: Optional[AutoOffsetReset] = "latest",
    consumer_extra_config: Optional[dict] = None,
    consumer_poll_timeout: Optional[float] = None,
    shutdown_timeout: float = 10,
    on_consumer_error: ConsumerErrorCallback = default_on_consumer_error,
    value_deserializer: DeserializerType = "json",
    key_deserializer: DeserializerType = "bytes",
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/kafka.py#L56)


<br>
***Arguments:***

- `name`: The source unique name.
It is used to generate the default topic name and consumer group name on the source broker.
Running multiple instances of `KafkaReplicatorSource` with the same name connected
to the same broker will make them share the same consumer group.
- `app_config`: The configuration of the application. Used by the source to connect to the application kafka broker.
- `topic`: The topic to replicate.
- `broker_address`: The connection settings for the source Kafka.
- `auto_offset_reset`: Consumer `auto.offset.reset` setting.
Default - Use the Application `auto_offset_reset` setting.
- `consumer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
Default - `None`
- `consumer_poll_timeout`: timeout for `InternalConsumer.poll()`
Default - Use the Application `consumer_poll_timeout` setting.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown.
- `on_consumer_error`: Triggered when the source `Consumer` fails to poll Kafka.
- `value_deserializer`: The default topic value deserializer, used by StreamingDataframe connected to the source.
Default - `json`
- `key_deserializer`: The default topic key deserializer, used by StreamingDataframe connected to the source.
Default - `json`
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.core.kafka.quix"></a>

## quixstreams.sources.core.kafka.quix

<a id="quixstreams.sources.core.kafka.quix.QuixEnvironmentSource"></a>

### QuixEnvironmentSource

```python
class QuixEnvironmentSource(KafkaReplicatorSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/quix.py#L23)

Source implementation that replicates a topic from a Quix Cloud environment to your application broker.
It can copy messages for development and testing without risking producing them back or affecting the consumer groups.

Running multiple instances of this source is supported.


<br>
***Example Snippet:***

```python
from quixstreams import Application
from quixstreams.sources.kafka import QuixEnvironmentSource

app = Application(
    consumer_group="group",
)

source = QuixEnvironmentSource(
    name="source-quix",
    app_config=app.config,
    quix_workspace_id="WORKSPACE_ID",
    quix_sdk_token="WORKSPACE_SDK_TOKEN",
    topic="quix-source-topic",
)

sdf = app.dataframe(source=source)
sdf = sdf.print()
app.run()
```

<a id="quixstreams.sources.core.kafka.quix.QuixEnvironmentSource.__init__"></a>

<br><br>

#### QuixEnvironmentSource.\_\_init\_\_

```python
def __init__(
    name: str,
    app_config: "ApplicationConfig",
    topic: str,
    quix_sdk_token: str,
    quix_workspace_id: str,
    quix_portal_api: Optional[str] = None,
    auto_offset_reset: Optional[AutoOffsetReset] = None,
    consumer_extra_config: Optional[dict] = None,
    consumer_poll_timeout: Optional[float] = None,
    shutdown_timeout: float = 10,
    on_consumer_error: ConsumerErrorCallback = default_on_consumer_error,
    value_deserializer: DeserializerType = "json",
    key_deserializer: DeserializerType = "bytes",
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/quix.py#L54)


<br>
***Arguments:***

- `quix_workspace_id`: The Quix workspace ID of the source environment.
- `quix_sdk_token`: Quix cloud sdk token used to connect to the source environment.
- `quix_portal_api`: The Quix portal API URL of the source environment.
Default - `Quix__Portal__Api` environment variable or Quix cloud production URL

For other parameters See `quixstreams.sources.kafka.KafkaReplicatorSource`

<a id="quixstreams.sources.community.file.azure"></a>

## quixstreams.sources.community.file.azure

<a id="quixstreams.sources.community.file.azure.AzureFileSource"></a>

### AzureFileSource

```python
class AzureFileSource(FileSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/azure.py#L36)

A source for extracting records stored within files in an Azure Filestore container.

It recursively iterates from the provided path (file or folder) and
processes all found files by parsing and producing the given records contained
in each file as individual messages to a kafka topic (same topic for all).

<a id="quixstreams.sources.community.file.azure.AzureFileSource.__init__"></a>

<br><br>

#### AzureFileSource.\_\_init\_\_

```python
def __init__(connection_string: str,
             container: str,
             filepath: Union[str, Path],
             key_setter: Optional[Callable[[object], object]] = None,
             value_setter: Optional[Callable[[object], object]] = None,
             timestamp_setter: Optional[Callable[[object], int]] = None,
             file_format: Union[Format, FormatName] = "json",
             compression: Optional[CompressionName] = None,
             has_partition_folders: bool = False,
             replay_speed: float = 1.0,
             name: Optional[str] = None,
             shutdown_timeout: float = 30,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/azure.py#L45)


<br>
***Arguments:***

- `connection_string`: Azure client authentication string.
- `container`: Azure container name.
- `filepath`: folder to recursively iterate from (a file will be used directly).
- `key_setter`: sets the kafka message key for a record in the file.
- `value_setter`: sets the kafka message value for a record in the file.
- `timestamp_setter`: sets the kafka message timestamp for a record in the file.
- `file_format`: what format the files are stored as (ex: "json").
- `compression`: what compression was used on the files, if any (ex. "gzip").
- `has_partition_folders`: whether files are nested within partition folders.
If True, FileSource will match the output topic partition count with it.
Set this flag to True if Quix Streams FileSink was used to dump data.
Note: messages will only align with these partitions if original key is used.
Example structure - a 2 partition topic (0, 1):
[/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
- `replay_speed`: Produce messages with this speed multiplier, which
roughly reflects the time "delay" between the original message producing.
Use any float >= 0, where 0 is no delay, and 1 is the original speed.
NOTE: Time delay will only be accurate per partition, NOT overall.
- `name`: The name of the Source application (Default: last folder name).
- `shutdown_timeout`: Time in seconds the application waits for the source
to gracefully shut down.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.file.azure.AzureFileSource.file_partition_counter"></a>

<br><br>

#### AzureFileSource.file\_partition\_counter

```python
def file_partition_counter() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/azure.py#L135)

This is a simplified version of the recommended way to retrieve folder
names based on the azure SDK docs examples.

<a id="quixstreams.sources.community.file.base"></a>

## quixstreams.sources.community.file.base

<a id="quixstreams.sources.community.file.base.FileSource"></a>

### FileSource

```python
class FileSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L23)

An interface for extracting records using a file-based client.

It recursively iterates from a provided path (file or folder) and
processes all found files by parsing and producing the given records contained
in each file as individual messages to a kafka topic.

Requires defining methods for navigating folders and retrieving/opening raw
files for the respective client.

When these abstract methods are defined, a FileSource will be able to:
1. Prepare a list of files to download, and retrieve them sequentially
2. Retrieve file contents asynchronously by downloading the upcoming one in the
    background
3. Decompress and deserialize the current file to loop through its records
4. Apply a replay delay for each contained record based on previous record
5. Serialize and produce respective messages to Kafka based on provided `setters`

<a id="quixstreams.sources.community.file.base.FileSource.__init__"></a>

<br><br>

#### FileSource.\_\_init\_\_

```python
def __init__(filepath: Union[str, Path],
             key_setter: Optional[Callable[[object], object]] = None,
             value_setter: Optional[Callable[[object], object]] = None,
             timestamp_setter: Optional[Callable[[object], int]] = None,
             file_format: Union[Format, FormatName] = "json",
             compression: Optional[CompressionName] = None,
             has_partition_folders: bool = False,
             replay_speed: float = 1.0,
             name: Optional[str] = None,
             shutdown_timeout: float = 30,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L43)


<br>
***Arguments:***

- `filepath`: folder to recursively iterate from (a file will be used directly).
- `key_setter`: sets the kafka message key for a record in the file.
- `value_setter`: sets the kafka message value for a record in the file.
- `timestamp_setter`: sets the kafka message timestamp for a record in the file.
- `file_format`: what format the files are stored as (ex: "json").
- `compression`: what compression was used on the files, if any (ex. "gzip").
- `has_partition_folders`: whether files are nested within partition folders.
If True, FileSource will match the output topic partition count with it.
Set this flag to True if Quix Streams FileSink was used to dump data.
Note: messages will only align with these partitions if original key is used.
Example structure - a 2 partition topic (0, 1):
[/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
- `replay_speed`: Produce messages with this speed multiplier, which
roughly reflects the time "delay" between the original message producing.
Use any float >= 0, where 0 is no delay, and 1 is the original speed.
NOTE: Time delay will only be accurate per partition, NOT overall.
- `name`: The name of the Source application (Default: last folder name).
- `shutdown_timeout`: Time in seconds the application waits for the source
to gracefully shut down.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.file.base.FileSource.get_file_list"></a>

<br><br>

#### FileSource.get\_file\_list

```python
@abstractmethod
def get_file_list(filepath: Path) -> Iterable[Path]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L106)

Find all files/"blobs" starting from a root folder.

Each item in the iterable should be a resolvable filepath.


<br>
***Arguments:***

- `filepath`: a starting filepath


<br>
***Returns:***

an iterable will all desired files in their desired processing order

<a id="quixstreams.sources.community.file.base.FileSource.read_file"></a>

<br><br>

#### FileSource.read\_file

```python
@abstractmethod
def read_file(filepath: Path) -> BinaryIO
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L117)

Returns a filepath as an unaltered, open filestream.

Result should be ready for deserialization (and/or decompression).

<a id="quixstreams.sources.community.file.base.FileSource.process_record"></a>

<br><br>

#### FileSource.process\_record

```python
def process_record(record: object)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L124)

Applies replay delay, serializes the record, and produces it to Kafka.

<a id="quixstreams.sources.community.file.base.FileSource.file_partition_counter"></a>

<br><br>

#### FileSource.file\_partition\_counter

```python
def file_partition_counter() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L166)

Can optionally define a way of counting folders to intelligently
set the "default_topic" partition count to match partition folder count.

If defined, class flag "has_partition_folders" can then be set to employ it.

It is not required since this operation may not be easy to implement, and the
file structure may not be used outside Quix Streams FileSink.

Example structure with 2 partitions (0,1):
```
topic_name/
├── 0/               # partition 0
│   ├── file_a.ext
│   └── file_b.ext
└── 1/               # partition 1
    ├── file_x.ext
    └── file_y.ext
```

<a id="quixstreams.sources.community.file.base.FileSource.default_topic"></a>

<br><br>

#### FileSource.default\_topic

```python
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L192)

Optionally allows the file structure to define the partition count for the

internal topic with file_partition_counter (instead of the default of 1).


<br>
***Returns:***

the default topic with optionally altered partition count

<a id="quixstreams.sources.community.file.local"></a>

## quixstreams.sources.community.file.local

<a id="quixstreams.sources.community.file.local.LocalFileSource"></a>

### LocalFileSource

```python
class LocalFileSource(FileSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/local.py#L20)

A source for extracting records stored within files in a local filesystem.

It recursively iterates from the provided path (file or folder) and
processes all found files by parsing and producing the given records contained
in each file as individual messages to a kafka topic (same topic for all).

<a id="quixstreams.sources.community.file.local.LocalFileSource.__init__"></a>

<br><br>

#### LocalFileSource.\_\_init\_\_

```python
def __init__(filepath: Union[str, Path],
             key_setter: Optional[Callable[[object], object]] = None,
             value_setter: Optional[Callable[[object], object]] = None,
             timestamp_setter: Optional[Callable[[object], int]] = None,
             file_format: Union[Format, FormatName] = "json",
             compression: Optional[CompressionName] = None,
             has_partition_folders: bool = False,
             replay_speed: float = 1.0,
             name: Optional[str] = None,
             shutdown_timeout: float = 30,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/local.py#L29)


<br>
***Arguments:***

- `filepath`: folder to recursively iterate from (a file will be used directly).
- `key_setter`: sets the kafka message key for a record in the file.
- `value_setter`: sets the kafka message value for a record in the file.
- `timestamp_setter`: sets the kafka message timestamp for a record in the file.
- `file_format`: what format the files are stored as (ex: "json").
- `compression`: what compression was used on the files, if any (ex. "gzip").
- `has_partition_folders`: whether files are nested within partition folders.
If True, FileSource will match the output topic partition count with it.
Set this flag to True if Quix Streams FileSink was used to dump data.
Note: messages will only align with these partitions if original key is used.
Example structure - a 2 partition topic (0, 1):
[/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
- `replay_speed`: Produce messages with this speed multiplier, which
roughly reflects the time "delay" between the original message producing.
Use any float >= 0, where 0 is no delay, and 1 is the original speed.
NOTE: Time delay will only be accurate per partition, NOT overall.
- `name`: The name of the Source application (Default: last folder name).
- `shutdown_timeout`: Time in seconds the application waits for the source
to gracefully shut down.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.file.s3"></a>

## quixstreams.sources.community.file.s3

<a id="quixstreams.sources.community.file.s3.S3FileSource"></a>

### S3FileSource

```python
class S3FileSource(FileSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/s3.py#L38)

A source for extracting records stored within files in an S3 bucket location.

It recursively iterates from the provided path (file or folder) and
processes all found files by parsing and producing the given records contained
in each file as individual messages to a kafka topic (same topic for all).

<a id="quixstreams.sources.community.file.s3.S3FileSource.__init__"></a>

<br><br>

#### S3FileSource.\_\_init\_\_

```python
def __init__(
        filepath: Union[str, Path],
        bucket: str,
        region_name: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        has_partition_folders: bool = False,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[
            ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[
            ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/s3.py#L47)


<br>
***Arguments:***

- `filepath`: folder to recursively iterate from (a file will be used directly).
- `bucket`: The S3 bucket name only (ex: 'your-bucket').
- `region_name`: The AWS region.
NOTE: can alternatively set the AWS_REGION environment variable
- `aws_access_key_id`: the AWS access key ID.
NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
- `aws_secret_access_key`: the AWS secret access key.
NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
- `endpoint_url`: the endpoint URL to use; only required for connecting
to a locally hosted S3.
NOTE: can alternatively set the AWS_ENDPOINT_URL_S3 environment variable
- `key_setter`: sets the kafka message key for a record in the file.
- `value_setter`: sets the kafka message value for a record in the file.
- `timestamp_setter`: sets the kafka message timestamp for a record in the file.
- `file_format`: what format the files are stored as (ex: "json").
- `compression`: what compression was used on the files, if any (ex. "gzip").
- `has_partition_folders`: whether files are nested within partition folders.
If True, FileSource will match the output topic partition count with it.
Set this flag to True if Quix Streams FileSink was used to dump data.
Note: messages will only align with these partitions if original key is used.
Example structure - a 2 partition topic (0, 1):
[/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
- `replay_speed`: Produce messages with this speed multiplier, which
roughly reflects the time "delay" between the original message producing.
Use any float >= 0, where 0 is no delay, and 1 is the original speed.
NOTE: Time delay will only be accurate per partition, NOT overall.
- `name`: The name of the Source application (Default: last folder name).
- `shutdown_timeout`: Time in seconds the application waits for the source
to gracefully shut down.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.file.compressions.gzip"></a>

## quixstreams.sources.community.file.compressions.gzip

<a id="quixstreams.sources.community.file.formats.json"></a>

## quixstreams.sources.community.file.formats.json

<a id="quixstreams.sources.community.file.formats.json.JSONFormat"></a>

### JSONFormat

```python
class JSONFormat(Format)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/formats/json.py#L12)

<a id="quixstreams.sources.community.file.formats.json.JSONFormat.__init__"></a>

<br><br>

#### JSONFormat.\_\_init\_\_

```python
def __init__(compression: Optional[CompressionName],
             loads: Optional[Callable[[str], dict]] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/formats/json.py#L13)

Read a JSON-formatted file (along with decompressing it).


<br>
***Arguments:***

- `compression`: the compression type used on the file
- `loads`: A custom function to deserialize objects to the expected dict
with {_key: str, _value: dict, _timestamp: int}.

<a id="quixstreams.sources.community.file.formats.parquet"></a>

## quixstreams.sources.community.file.formats.parquet

<a id="quixstreams.sources.community.kinesis.kinesis"></a>

## quixstreams.sources.community.kinesis.kinesis

<a id="quixstreams.sources.community.kinesis.kinesis.KinesisSource"></a>

### KinesisSource

```python
class KinesisSource(StatefulSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/kinesis/kinesis.py#L22)

NOTE: Requires `pip install quixstreams[kinesis]` to work.

This source reads data from an Amazon Kinesis stream, dumping it to a
kafka topic using desired `StreamingDataFrame`-based transformations.

Provides "at-least-once" guarantees.

The incoming message value will be in bytes, so transform in your SDF accordingly.

Example Usage:

```python
from quixstreams import Application
from quixstreams.sources.community.kinesis import KinesisSource


kinesis = KinesisSource(
    stream_name="<YOUR STREAM>",
    aws_access_key_id="<YOUR KEY ID>",
    aws_secret_access_key="<YOUR SECRET KEY>",
    aws_region="<YOUR REGION>",
    auto_offset_reset="earliest",  # start from the beginning of the stream (vs end)
)

app = Application(
    broker_address="<YOUR BROKER INFO>",
    consumer_group="<YOUR GROUP>",
)

sdf = app.dataframe(source=kinesis).print(metadata=True)
# YOUR LOGIC HERE!

if __name__ == "__main__":
    app.run()
```

<a id="quixstreams.sources.community.kinesis.kinesis.KinesisSource.__init__"></a>

<br><br>

#### KinesisSource.\_\_init\_\_

```python
def __init__(
        stream_name: str,
        aws_region: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        aws_endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_KINESIS"),
        shutdown_timeout: float = 10,
        auto_offset_reset: AutoOffsetResetType = "latest",
        max_records_per_shard: int = 1000,
        commit_interval: float = 5.0,
        retry_backoff_secs: float = 5.0,
        on_client_connect_success: Optional[
            ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[
            ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/kinesis/kinesis.py#L61)


<br>
***Arguments:***

- `stream_name`: name of the desired Kinesis stream to consume.
- `aws_region`: The AWS region.
NOTE: can alternatively set the AWS_REGION environment variable
- `aws_access_key_id`: the AWS access key ID.
NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
- `aws_secret_access_key`: the AWS secret access key.
NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
- `aws_endpoint_url`: the endpoint URL to use; only required for connecting
to a locally hosted Kinesis.
NOTE: can alternatively set the AWS_ENDPOINT_URL_KINESIS environment variable
- `shutdown_timeout`: 
- `auto_offset_reset`: When no previous offset has been recorded, whether to
start from the beginning ("earliest") or end ("latest") of the stream.
- `max_records_per_shard`: During round-robin consumption, how many records
to consume per shard (partition) per consume (NOT per-commit).
- `commit_interval`: the time between commits
- `retry_backoff_secs`: how long to back off from doing HTTP calls for a
shard when Kinesis consumer encounters handled/expected errors.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.pubsub.pubsub"></a>

## quixstreams.sources.community.pubsub.pubsub

<a id="quixstreams.sources.community.pubsub.pubsub.PubSubSource"></a>

### PubSubSource

```python
class PubSubSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/pubsub.py#L20)

This source enables reading from a Google Cloud Pub/Sub topic,
dumping it to a kafka topic using desired SDF-based transformations.

Provides "at-least-once" guarantees.

Currently, forwarding message keys ("ordered messages" in Pub/Sub) is unsupported.

The incoming message value will be in bytes, so transform in your SDF accordingly.

Example Usage:

```python
from quixstreams import Application
from quixstreams.sources.community.pubsub import PubSubSource
from os import environ

source = PubSubSource(
    # Suggested: pass JSON-formatted credentials from an environment variable.
    service_account_json = environ["PUBSUB_SERVICE_ACCOUNT_JSON"],
    project_id="<project ID>",
    topic_id="<topic ID>",  # NOTE: NOT the full /x/y/z path!
    subscription_id="<subscription ID>",  # NOTE: NOT the full /x/y/z path!
    create_subscription=True,
)
app = Application(
    broker_address="localhost:9092",
    auto_offset_reset="earliest",
    consumer_group="gcp",
    loglevel="INFO"
)
sdf = app.dataframe(source=source).print(metadata=True)

if __name__ == "__main__":
    app.run()
```

<a id="quixstreams.sources.community.pubsub.pubsub.PubSubSource.__init__"></a>

<br><br>

#### PubSubSource.\_\_init\_\_

```python
def __init__(project_id: str,
             topic_id: str,
             subscription_id: str,
             service_account_json: Optional[str] = None,
             commit_every: int = 100,
             commit_interval: float = 5.0,
             create_subscription: bool = False,
             enable_message_ordering: bool = False,
             shutdown_timeout: float = 10.0,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/pubsub.py#L59)


<br>
***Arguments:***

- `project_id`: a Google Cloud project ID.
- `topic_id`: a Pub/Sub topic ID (NOT the full path).
- `subscription_id`: a Pub/Sub subscription ID (NOT the full path).
- `service_account_json`: a Google Cloud Credentials JSON as a string
Can instead use environment variables (which have different behavior):
- "GOOGLE_APPLICATION_CREDENTIALS" set to a JSON filepath i.e. /x/y/z.json
- "PUBSUB_EMULATOR_HOST" set to a URL if using an emulated Pub/Sub
- `commit_every`: max records allowed to be processed before committing.
- `commit_interval`: max allowed elapsed time between commits.
- `create_subscription`: whether to attempt to create a subscription at
startup; if it already exists, it instead logs its details (DEBUG level).
- `enable_message_ordering`: When creating a Pub/Sub subscription, whether
to allow message ordering. NOTE: does NOT affect existing subscriptions!
- `shutdown_timeout`: How long to wait for a graceful shutdown of the source.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.pandas"></a>

## quixstreams.sources.community.pandas

<a id="quixstreams.sources.community.pandas.PandasDataFrameSource"></a>

### PandasDataFrameSource

```python
class PandasDataFrameSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pandas.py#L20)

<a id="quixstreams.sources.community.pandas.PandasDataFrameSource.__init__"></a>

<br><br>

#### PandasDataFrameSource.\_\_init\_\_

```python
def __init__(df: pd.DataFrame,
             key_column: str,
             timestamp_column: str = None,
             delay: float = 0,
             shutdown_timeout: float = 10,
             keep_meta_as_values: bool = True,
             name: str = "pandas-dataframe-source") -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pandas.py#L21)

A source that reads data from a pandas.DataFrame and produces rows to a Kafka topic in JSON format.


<br>
***Arguments:***

- `df`: the pandas.DataFrame object to read data from.
- `key_column`: a column name that contains the messages keys.
The values in dataframe[key_column] must be either strings or `None`.
- `timestamp_column`: an optional argument to specify a dataframe column that contains the messages timestamps.
The values in dataframe[timestamp_column] must be time in milliseconds as `int`.
If empty, the current epoch will be used.
Default - `None`
- `name`: a unique name for the Source, used as a part of the default topic name.
Default - `"pandas-dataframe-source"`.
- `delay`: an optional delay after producing each row for stream simulation.
Default - `0`.
- `shutdown_timeout`: Time in seconds the application waits for the source to gracefully shut down.
- `keep_meta_as_values`: Whether to keep metadata (timestamp_column and key_column) as-values data too.
If True, timestamp and key columns are passed both as metadata and values in the message.
If False, timestamp and key columns are passed only as the message's metadata.
Default - `True`.

<a id="quixstreams.sources.community.pandas.PandasDataFrameSource.run"></a>

<br><br>

#### PandasDataFrameSource.run

```python
def run()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pandas.py#L107)

Produces data from the DataFrame row by row.

<a id="quixstreams.sources.community.influxdb3.influxdb3"></a>

## quixstreams.sources.community.influxdb3.influxdb3

<a id="quixstreams.sources.community.influxdb3.influxdb3.InfluxDB3Source"></a>

### InfluxDB3Source

```python
class InfluxDB3Source(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/influxdb3/influxdb3.py#L79)

InfluxDB3Source extracts data from a specified set of measurements in a
  database (or all available ones if none are specified).

It processes measurements sequentially by gathering/producing a tumbling
  "time_delta"-sized window of data, starting from 'start_date' and eventually
  stopping at 'end_date', completing that measurement.

It then starts the next measurement, continuing until all are complete.

If no 'end_date' is provided, it will run indefinitely for a single
  measurement (which means no other measurements will be processed!).

<a id="quixstreams.sources.community.influxdb3.influxdb3.InfluxDB3Source.__init__"></a>

<br><br>

#### InfluxDB3Source.\_\_init\_\_

```python
def __init__(
    host: str,
    token: str,
    organization_id: str,
    database: str,
    key_setter: Optional[Callable[[object], object]] = None,
    timestamp_setter: Optional[Callable[[object], int]] = None,
    start_date: datetime = datetime.now(tz=timezone.utc),
    end_date: Optional[datetime] = None,
    measurements: Optional[Union[str, list[str]]] = None,
    measurement_column_name: str = "_measurement_name",
    sql_query: Optional[str] = None,
    time_delta: str = "5m",
    delay: float = 0,
    max_retries: int = 5,
    name: Optional[str] = None,
    shutdown_timeout: float = 10,
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/influxdb3/influxdb3.py#L94)


<br>
***Arguments:***

- `host`: Host URL of the InfluxDB instance.
- `token`: Authentication token for InfluxDB.
- `organization_id`: Organization name in InfluxDB.
- `database`: Database name in InfluxDB.
- `key_setter`: sets the kafka message key for a measurement record.
By default, will set the key to the measurement's name.
- `timestamp_setter`: sets the kafka message timestamp for a measurement record.
By default, the timestamp will be the Kafka default (Kafka produce time).
- `start_date`: The start datetime for querying InfluxDB. Uses current time by default.
- `end_date`: The end datetime for querying InfluxDB.
If none provided, runs indefinitely for a single measurement.
- `measurements`: The measurements to query. If None, all measurements will be processed.
- `measurement_column_name`: The column name used for appending the measurement name to the record.
- `sql_query`: Custom SQL query for retrieving data.
Query expects a `{start_time}`, `{end_time}`, and `{measurement_name}` for later formatting.
If provided, it overrides the default window-query logic.
- `time_delta`: Time interval for batching queries, e.g., "5m" for 5 minutes.
- `delay`: An optional delay between producing batches.
- `name`: A unique name for the Source, used as part of the topic name.
- `shutdown_timeout`: Time in seconds to wait for graceful shutdown.
- `max_retries`: Maximum number of retries for querying or producing.
Note that consecutive retries have a multiplicative backoff.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

