<a id="quixstreams.sources.base.source"></a>

## quixstreams.sources.base.source

<a id="quixstreams.sources.base.source.BaseSource"></a>

### BaseSource

```python
class BaseSource(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L22)

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
def configure(topic: Topic, producer: RowProducer) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L94)

This method is triggered when the source is registered to the Application.

It configures the source's Kafka producer and the topic it will produce to.

<a id="quixstreams.sources.base.source.BaseSource.start"></a>

<br><br>

#### BaseSource.start

```python
@abstractmethod
def start() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L113)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L122)

This method is triggered when the application is shutting down.

The source must ensure that the `run` method is completed soon.

<a id="quixstreams.sources.base.source.BaseSource.default_topic"></a>

<br><br>

#### BaseSource.default\_topic

```python
@abstractmethod
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L130)

This method is triggered when the topic is not provided to the source.

The source must return a default topic configuration.

<a id="quixstreams.sources.base.source.Source"></a>

### Source

```python
class Source(BaseSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L138)

A base class for custom Sources that provides a basic implementation of `BaseSource`
interface.
It is recommended to interface to create custom sources.

Subclass it and implement the `run` method to fetch data and produce it to Kafka.

**Example**:

  
```python
from quixstreams import Application
import random

from quixstreams.sources import Source


class RandomNumbersSource(Source):
    def run(self):
        while self.running:
            number = random.randint(0, 100)
            serialized = self._producer_topic.serialize(value=number)
            self.produce(key=str(number), value=serialized.value)


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
def __init__(name: str, shutdown_timeout: float = 10) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L186)


<br>
***Arguments:***

- `name`: The source unique name. Used to generate the topic configurtion
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown

<a id="quixstreams.sources.base.source.Source.running"></a>

<br><br>

#### Source.running

```python
@property
def running() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L200)

Property indicating if the source is running.

The `stop` method will set it to `False`. Use it to stop the source gracefully.

<a id="quixstreams.sources.base.source.Source.cleanup"></a>

<br><br>

#### Source.cleanup

```python
def cleanup(failed: bool) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L208)

This method is triggered once the `run` method completes.

Use it to clean up the resources and shut down the source gracefully.

It flushes the producer when `_run` completes successfully.

<a id="quixstreams.sources.base.source.Source.stop"></a>

<br><br>

#### Source.stop

```python
def stop() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L219)

This method is triggered when the application is shutting down.

It sets the `running` property to `False`.

<a id="quixstreams.sources.base.source.Source.start"></a>

<br><br>

#### Source.start

```python
def start()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L228)

This method is triggered in the subprocess when the source is started.

It marks the source as running, execute it's run method and ensure cleanup happens.

<a id="quixstreams.sources.base.source.Source.run"></a>

<br><br>

#### Source.run

```python
@abstractmethod
def run()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L244)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L252)

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
            poll_timeout: float = 5.0,
            buffer_error_max_tries: int = 3) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L268)

Produce a message to the configured source topic in Kafka.

<a id="quixstreams.sources.base.source.Source.flush"></a>

<br><br>

#### Source.flush

```python
def flush(timeout: Optional[float] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L293)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L311)

Return a default topic matching the source name.

The default topic will not be used if the topic has already been provided to the source.


<br>
***Returns:***

`quixstreams.models.topics.Topic`

<a id="quixstreams.sources.core.csv"></a>

## quixstreams.sources.core.csv

<a id="quixstreams.sources.core.csv.CSVSource"></a>

### CSVSource

```python
class CSVSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/csv.py#L9)

<a id="quixstreams.sources.core.csv.CSVSource.__init__"></a>

<br><br>

#### CSVSource.\_\_init\_\_

```python
def __init__(path: str,
             dialect: str = "excel",
             name: Optional[str] = None,
             shutdown_timeout: float = 10,
             key_deserializer: Callable[[Any], str] = str,
             value_deserializer: Callable[[Any], str] = json.loads) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/csv.py#L10)

A base CSV source that reads data from a single CSV file.

Best used with `quixstreams.sinks.csv.CSVSink`.

Required columns: key, value
Optional columns: timestamp


<br>
***Arguments:***

- `path`: path to the CSV file
- `dialect`: a CSV dialect to use. It affects quoting and delimiters.
See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
Default - `"excel"`.
- `key_deseralizer`: a callable to convert strings to key.
Default - `str`
- `value_deserializer`: a callable to convert strings to value.
Default - `json.loads`

<a id="quixstreams.sources.core.kafka.kafka"></a>

## quixstreams.sources.core.kafka.kafka

<a id="quixstreams.sources.core.kafka.kafka.KafkaReplicatorSource"></a>

### KafkaReplicatorSource

```python
class KafkaReplicatorSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/kafka.py#L26)

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
def __init__(name: str,
             app_config: "ApplicationConfig",
             topic: str,
             broker_address: Union[str, ConnectionConfig],
             auto_offset_reset: AutoOffsetReset = "latest",
             consumer_extra_config: Optional[dict] = None,
             consumer_poll_timeout: Optional[float] = None,
             shutdown_timeout: float = 10,
             on_consumer_error: Optional[
                 ConsumerErrorCallback] = default_on_consumer_error,
             value_deserializer: DeserializerType = "json",
             key_deserializer: DeserializerType = "bytes") -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/kafka.py#L55)


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
- `consumer_poll_timeout`: timeout for `RowConsumer.poll()`
Default - Use the Application `consumer_poll_timeout` setting.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown.
- `on_consumer_error`: Triggered when the source `Consumer` fails to poll Kafka.
- `value_deserializer`: The default topic value deserializer, used by StreamingDataframe connected to the source.
Default - `json`
- `key_deserializer`: The default topic key deserializer, used by StreamingDataframe connected to the source.
Default - `json`

<a id="quixstreams.sources.core.kafka.quix"></a>

## quixstreams.sources.core.kafka.quix

<a id="quixstreams.sources.core.kafka.quix.QuixEnvironmentSource"></a>

### QuixEnvironmentSource

```python
class QuixEnvironmentSource(KafkaReplicatorSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/quix.py#L18)

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
def __init__(name: str,
             app_config: "ApplicationConfig",
             topic: str,
             quix_sdk_token: str,
             quix_workspace_id: str,
             quix_portal_api: Optional[str] = None,
             auto_offset_reset: Optional[AutoOffsetReset] = None,
             consumer_extra_config: Optional[dict] = None,
             consumer_poll_timeout: Optional[float] = None,
             shutdown_timeout: float = 10,
             on_consumer_error: Optional[
                 ConsumerErrorCallback] = default_on_consumer_error,
             value_deserializer: DeserializerType = "json",
             key_deserializer: DeserializerType = "bytes") -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/quix.py#L49)


<br>
***Arguments:***

- `quix_workspace_id`: The Quix workspace ID of the source environment.
- `quix_sdk_token`: Quix cloud sdk token used to connect to the source environment.
- `quix_portal_api`: The Quix portal API URL of the source environment.
Default - `Quix__Portal__Api` environment variable or Quix cloud production URL

For other parameters See `quixstreams.sources.kafka.KafkaReplicatorSource`

