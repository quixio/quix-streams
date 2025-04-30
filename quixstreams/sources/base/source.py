import logging
from abc import ABC, abstractmethod
from typing import Callable, Optional, Union

from quixstreams.checkpointing.exceptions import CheckpointProducerTimeout
from quixstreams.internal_producer import InternalProducer
from quixstreams.kafka.producer import PRODUCER_ON_ERROR_RETRIES, PRODUCER_POLL_TIMEOUT
from quixstreams.models.messages import KafkaMessage
from quixstreams.models.topics import Topic
from quixstreams.models.types import Headers
from quixstreams.state import PartitionTransaction, State, StorePartition

logger = logging.getLogger(__name__)

ClientConnectSuccessCallback = Callable[[], None]
ClientConnectFailureCallback = Callable[[Optional[Exception]], None]


def _default_on_client_connect_success():
    logger.info("CONNECTED!")


def _default_on_client_connect_failure(exception: Exception):
    logger.error(f"ERROR: Failed while connecting to client: {exception}")
    raise exception


class BaseSource(ABC):
    """
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
    """

    # time in seconds the application will wait for the source to stop.
    shutdown_timeout: float = 10

    def __init__(
        self,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        self._producer: Optional[InternalProducer] = None
        self._producer_topic: Optional[Topic] = None
        self._on_client_connect_success = (
            on_client_connect_success or _default_on_client_connect_success
        )
        self._on_client_connect_failure = (
            on_client_connect_failure or _default_on_client_connect_failure
        )

    def _init_client(self):
        try:
            self.setup()
            self._on_client_connect_success()
        except Exception as e:
            self._on_client_connect_failure(e)

    def configure(self, topic: Topic, producer: InternalProducer, **kwargs) -> None:
        """
        This method is triggered before the source is started.

        It configures the source's Kafka producer, the topic it will produce to and optional dependencies.
        """
        self._producer = producer
        self._producer_topic = topic

    def setup(self):
        """
        When applicable, set up the client here along with any validation to affirm a
        valid/successful authentication/connection.
        """
        return

    @property
    def producer(self) -> InternalProducer:
        if self._producer is None:
            raise RuntimeError("source not configured")
        return self._producer

    @property
    def producer_topic(self) -> Topic:
        if self._producer_topic is None:
            raise RuntimeError("source not configured")
        return self._producer_topic

    @abstractmethod
    def start(self) -> None:
        """
        This method is triggered in the subprocess when the source is started.

        The subprocess will run as long as the start method executes.
        Use it to fetch data and produce it to Kafka.
        """

    @abstractmethod
    def stop(self) -> None:
        """
        This method is triggered when the application is shutting down.

        The source must ensure that the `run` method is completed soon.
        """

    @abstractmethod
    def default_topic(self) -> Topic:
        """
        This method is triggered when the topic is not provided to the source.

        The source must return a default topic configuration.

        Note: if the default topic is used, the Application will prefix its name with "source__".
        """


class Source(BaseSource):
    """
    A base class for custom Sources that provides a basic implementation of `BaseSource`
    interface.
    It is recommended to interface to create custom sources.

    Subclass it and implement the `run` method to fetch data and produce it to Kafka.

    Example:

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
    """

    def __init__(
        self,
        name: str,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        """
        :param name: The source unique name. It is used to generate the topic configuration.
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        # used to generate a unique topic for the source.
        self.name = name

        self.shutdown_timeout = shutdown_timeout
        self._running = False

    @property
    def running(self) -> bool:
        """
        Property indicating if the source is running.

        The `stop` method will set it to `False`. Use it to stop the source gracefully.
        """
        return self._running

    def cleanup(self, failed: bool) -> None:
        """
        This method is triggered once the `run` method completes.

        Use it to clean up the resources and shut down the source gracefully.

        It flushes the producer when `_run` completes successfully.
        """
        if not failed:
            self.flush(self.shutdown_timeout / 2)

    def stop(self) -> None:
        """
        This method is triggered when the application is shutting down.

        It sets the `running` property to `False`.
        """
        self._running = False

    def start(self) -> None:
        """
        This method is triggered in the subprocess when the source is started.

        It marks the source as running, execute it's run method and ensure cleanup happens.
        """
        self._running = True
        try:
            self._init_client()
            self.run()
        except BaseException:
            self.cleanup(failed=True)
            raise
        else:
            self.cleanup(failed=False)

    @abstractmethod
    def run(self):
        """
        This method is triggered in the subprocess when the source is started.

        The subprocess will run as long as the run method executes.
        Use it to fetch data and produce it to Kafka.
        """

    def serialize(
        self,
        key: Optional[object] = None,
        value: Optional[object] = None,
        headers: Optional[Headers] = None,
        timestamp_ms: Optional[int] = None,
    ) -> KafkaMessage:
        """
        Serialize data to bytes using the producer topic serializers and return a `quixstreams.models.messages.KafkaMessage`.

        :return: `quixstreams.models.messages.KafkaMessage`
        """
        return self.producer_topic.serialize(
            key=key, value=value, headers=headers, timestamp_ms=timestamp_ms
        )

    def produce(
        self,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        poll_timeout: float = PRODUCER_POLL_TIMEOUT,
        buffer_error_max_tries: int = PRODUCER_ON_ERROR_RETRIES,
    ) -> None:
        """
        Produce a message to the configured source topic in Kafka.
        """

        self.producer.produce(
            topic=self.producer_topic.name,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp=timestamp,
            poll_timeout=poll_timeout,
            buffer_error_max_tries=buffer_error_max_tries,
        )

    def flush(self, timeout: Optional[float] = None) -> None:
        """
        This method flush the producer.

        It ensures all messages are successfully delivered to Kafka.

        :param float timeout: time to attempt flushing (seconds).
            None use producer default or -1 is infinite. Default: None

        :raises CheckpointProducerTimeout: if any message fails to produce before the timeout
        """
        logger.debug("Flushing source")
        unproduced_msg_count = self.producer.flush(timeout)
        if unproduced_msg_count > 0:
            raise CheckpointProducerTimeout(
                f"'{unproduced_msg_count}' messages failed to be produced before the producer flush timeout"
            )

    def default_topic(self) -> Topic:
        """
        Return a default topic matching the source name.
        The default topic will not be used if the topic has already been provided to the source.

        Note: if the default topic is used, the Application will prefix its name with "source__".

        :return: `quixstreams.models.topics.Topic`
        """
        return Topic(
            name=self.name,
            value_deserializer="json",
            value_serializer="json",
        )

    def __repr__(self):
        return self.name


class StatefulSource(Source):
    """
    A `Source` class for custom Sources that need a state.

    Subclasses are responsible for flushing, by calling `flush`, at reasonable intervals.

    Example:

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
    """

    def __init__(
        self,
        name: str,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        """
        :param name: The source unique name. It is used to generate the topic configuration.
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            name,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._store_partition: Optional[StorePartition] = None
        self._store_transaction: Optional[PartitionTransaction] = None
        self._store_state: Optional[State] = None

    def configure(
        self,
        topic: Topic,
        producer: InternalProducer,
        *,
        store_partition: Optional[StorePartition] = None,
        **kwargs,
    ) -> None:
        """
        This method is triggered before the source is started.

        It configures the source's Kafka producer, the topic it will produce to and the store partition.
        """
        super().configure(topic=topic, producer=producer)
        self._store_partition = store_partition
        self._store_transaction = None
        self._store_state = None

    @property
    def store_partitions_count(self) -> int:
        """
        Count of store partitions.

        Used to configure the number of partition in the changelog topic.
        """
        return 1

    @property
    def assigned_store_partition(self) -> int:
        """
        The store partition assigned to this instance
        """
        return 0

    @property
    def store_name(self) -> str:
        """
        The source store name
        """
        return f"source-{self.name}"

    @property
    def state(self) -> State:
        """
        Access the `State` of the source.

        The `State` lifecycle is tied to the store transaction. A transaction is only valid until the next `.flush()` call. If no valid transaction exist, a new transaction is created.

        Important: after each `.flush()` call, a previously returned instance is invalidated and cannot be used. The property must be called again.
        """
        if self._store_partition is None:
            raise RuntimeError("source is not configured")

        if self._store_transaction is None:
            self._store_transaction = self._store_partition.begin()

        if self._store_state is None:
            self._store_state = self._store_transaction.as_state()

        return self._store_state

    def flush(self, timeout: Optional[float] = None) -> None:
        """
        This method commit the state and flush the producer.

        It ensures the state is published to the changelog topic and all messages are successfully delivered to Kafka.

        :param float timeout: time to attempt flushing (seconds).
            None use producer default or -1 is infinite. Default: None

        :raises CheckpointProducerTimeout: if any message fails to produce before the timeout
        """
        if self._store_transaction:
            self._store_transaction.prepare(None)
            self._store_transaction.flush()
            self._store_transaction = None
            self._store_state = None

        super().flush(timeout)
