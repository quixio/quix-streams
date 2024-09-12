import logging

from abc import ABC, abstractmethod
from typing import Optional, Union


from quixstreams.models.messages import KafkaMessage
from quixstreams.models.topics import Topic
from quixstreams.models.types import Headers
from quixstreams.rowproducer import RowProducer
from quixstreams.checkpointing.exceptions import CheckpointProducerTimeout


logger = logging.getLogger(__name__)

__all__ = (
    "BaseSource",
    "Source",
)


class BaseSource(ABC):
    """
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
    """

    # time in seconds the application will wait for the source to stop.
    shutdown_timeout: float = 10

    def __init__(self):
        self._producer: Optional[RowProducer] = None
        self._producer_topic: Optional[Topic] = None
        self._configured: bool = False

    def configure(self, topic: Topic, producer: RowProducer) -> None:
        """
        This method is triggered when the source is registered to the Application.

        It configures the source's Kafka producer and the topic it will produce to.
        """
        self._producer = producer
        self._producer_topic = topic
        self._configured = True

    @property
    def configured(self):
        return self._configured

    @property
    def producer_topic(self):
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

        Example Snippet:

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
        """

    @abstractmethod
    def default_topic(self) -> Topic:
        """
        This method is triggered when the topic is not provided to the source.

        The source must return a default topic configuration.
        """


class Source(BaseSource):
    """
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
    """

    def __init__(self, name: str, shutdown_timeout: float = 10) -> None:
        """
        :param name: The source unique name. Used to generate the topic configurtion
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown
        """
        super().__init__()

        # used to generate a unique topic for the source.
        self.name = name

        self.shutdown_timeout = shutdown_timeout
        self._running = False

    @property
    def running(self) -> bool:
        """
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
        """
        return self._running

    def cleanup(self, failed: bool) -> None:
        """
        This method is triggered once the `run` method completes.

        Use it to clean up the resources and shut down the source gracefully.

        It flush the producer when `_run` completes successfully.
        """
        if not failed:
            self.flush(self.shutdown_timeout / 2)

    def stop(self) -> None:
        """
        This method is triggered when the application is shutting down.

        It sets the `running` property to `False`.
        """
        self._running = False
        super().stop()

    def start(self):
        """
        This method is triggered in the subprocess when the source is started.

        It marks the source as running, execute it's run method and ensure cleanup happens.
        """
        self._running = True
        try:
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
        return self._producer_topic.serialize(
            key=key, value=value, headers=headers, timestamp_ms=timestamp_ms
        )

    def produce(
        self,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        poll_timeout: float = 5.0,
        buffer_error_max_tries: int = 3,
    ) -> None:
        """
        Produce a message to the configured source topic in Kafka.
        """

        self._producer.produce(
            topic=self._producer_topic.name,
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
        unproduced_msg_count = self._producer.flush(timeout)
        if unproduced_msg_count > 0:
            raise CheckpointProducerTimeout(
                f"'{unproduced_msg_count}' messages failed to be produced before the producer flush timeout"
            )

    def default_topic(self) -> Topic:
        """
        Return a default topic matching the source name.
        The default topic will not be used if the topic has already been provided to the source.

        :return: `quixstreams.models.topics.Topic`
        """
        return Topic(
            name=self.name,
            value_deserializer="json",
            value_serializer="json",
        )

    def __repr__(self):
        return self.name
