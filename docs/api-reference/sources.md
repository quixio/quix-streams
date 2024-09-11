<a id="quixstreams.sources.base"></a>

## quixstreams.sources.base

<a id="quixstreams.sources.base.BaseSource"></a>

### BaseSource

```python
class BaseSource(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L22)

This is the base class for all sources.

Sources are executed in a sub-process of the main application.

To create your own source you need to implement:
    * `run`
    * `stop`
    * `default_topic`

`BaseSource` is the most basic interface, and the framework expects every
sources to implement it. Use `Source` to benefit from a base implementation.

You can connect a source to a StreamingDataframe using the Application.

Example snippet:

```python
from quixstreams import Application
from quixstreams.sources import Source

class MySource(Source):
    def run(self):
        for _ in range(1000):
            self.produce(
                key="foo",
                value=b"foo"
            )

def main():
    app = Application()
    source = MySource(name="my_source")

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run(sdf)

if __name__ == "__main__":
    main()
```

<a id="quixstreams.sources.base.BaseSource.configure"></a>

<br><br>

#### BaseSource.configure

```python
def configure(topic: Topic, producer: RowProducer) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L74)

This method is triggered when the source is registered to the Application.

It configures the source's Kafka producer and the topic it will produce to.

<a id="quixstreams.sources.base.BaseSource.start"></a>

<br><br>

#### BaseSource.start

```python
@abstractmethod
def start() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L93)

This method is triggered in the subprocess when the source is started.

The subprocess will run as long as the start method executes.
Use it to fetch data and produce it to Kafka.

<a id="quixstreams.sources.base.BaseSource.stop"></a>

<br><br>

#### BaseSource.stop

```python
@abstractmethod
def stop() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L102)

This method is triggered when the application is shutting down.

The source must ensure that the `run` method is completed soon.


<br>
***Example Snippet:***

```python
class MySource(BaseSource):
    def start(self):
        self._running = True
        while self._running:
            self._producer.produce(
                topic=self._producer_topic,
                value="foo",
            )
            time.sleep(1)

    def stop(self):
        self._running = False
```

<a id="quixstreams.sources.base.BaseSource.default_topic"></a>

<br><br>

#### BaseSource.default\_topic

```python
@abstractmethod
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L127)

This method is triggered when the topic is not provided to the source.

The source must return a default topic configuration.

<a id="quixstreams.sources.base.Source"></a>

### Source

```python
class Source(BaseSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L135)

BaseSource class implementation providing

Implementation for the abstract method:
    * `default_topic`
    * `start`
    * `stop`

Helper methods
    * serialize
    * produce
    * flush

Helper property
    * running

Subclass it and implement the `run` method to fetch data and produce it to Kafka.

<a id="quixstreams.sources.base.Source.__init__"></a>

<br><br>

#### Source.\_\_init\_\_

```python
def __init__(name: str, shutdown_timeout: float = 10) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L155)


<br>
***Arguments:***

- `name`: The source unique name. Used to generate the topic configurtion
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown

<a id="quixstreams.sources.base.Source.running"></a>

<br><br>

#### Source.running

```python
@property
def running() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L169)

Property indicating if the source is running.

The `stop` method will set it to `False`. Use it to stop the source gracefully.

Example snippet:

```python
class MySource(Source):
    def run(self):
        while self.running:
            self.produce(
                value="foo",
            )
            time.sleep(1)
```

<a id="quixstreams.sources.base.Source.cleanup"></a>

<br><br>

#### Source.cleanup

```python
def cleanup(failed: bool) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L189)

This method is triggered once the `run` method completes.

Use it to clean up the resources and shut down the source gracefully.

It flush the producer when `_run` completes successfully.

<a id="quixstreams.sources.base.Source.stop"></a>

<br><br>

#### Source.stop

```python
def stop() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L200)

This method is triggered when the application is shutting down.

It sets the `running` property to `False`.

<a id="quixstreams.sources.base.Source.start"></a>

<br><br>

#### Source.start

```python
def start()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L209)

This method is triggered in the subprocess when the source is started.

It marks the source as running, execute it's run method and ensure cleanup happens.

<a id="quixstreams.sources.base.Source.run"></a>

<br><br>

#### Source.run

```python
@abstractmethod
def run()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L225)

This method is triggered in the subprocess when the source is started.

The subprocess will run as long as the run method executes.
Use it to fetch data and produce it to Kafka.

<a id="quixstreams.sources.base.Source.serialize"></a>

<br><br>

#### Source.serialize

```python
def serialize(key: Optional[object] = None,
              value: Optional[object] = None,
              headers: Optional[Headers] = None,
              timestamp_ms: Optional[int] = None) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L233)

Serialize data to bytes using the producer topic serializers and return a `quixstreams.models.messages.KafkaMessage`.


<br>
***Returns:***

`quixstreams.models.messages.KafkaMessage`

<a id="quixstreams.sources.base.Source.produce"></a>

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L249)

Produce a message to the configured source topic in Kafka.

<a id="quixstreams.sources.base.Source.flush"></a>

<br><br>

#### Source.flush

```python
def flush(timeout: Optional[float] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L274)

This method flush the producer.

It ensures all messages are successfully delivered to Kafka.


<br>
***Arguments:***

- `timeout` (`float`): time to attempt flushing (seconds).
None use producer default or -1 is infinite. Default: None

**Raises**:

- `CheckpointProducerTimeout`: if any message fails to produce before the timeout

<a id="quixstreams.sources.base.Source.default_topic"></a>

<br><br>

#### Source.default\_topic

```python
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base.py#L292)

Return a default topic matching the source name.

The default topic will not be used if the topic has already been provided to the source.


<br>
***Returns:***

`quixstreams.models.topics.Topic`

