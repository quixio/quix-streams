import threading
import logging

from abc import ABC, abstractmethod
from typing import Optional, Union


from quixstreams.models.messages import KafkaMessage
from quixstreams.models.topics import TopicConfig, Topic
from quixstreams.models.types import Headers
from quixstreams.rowproducer import RowProducer
from quixstreams.checkpointing.exceptions import CheckpointProducerTimeout


logger = logging.getLogger(__name__)

__all__ = (
    "BaseSource",
    "Source",
    "PollingSource",
)


class BaseSource(ABC):
    """
    This is the base class for all sources.

    Sources are executed in a sub-process of the main application.

    To create your own source you need to implement:
        * `run`
        * `cleanup`
        * `stop`
        * `default_topic`
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

        It's used to configure the source with the kafka producer and the topic it should use
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
    def run(self) -> None:
        """
        This method is triggered in the subprocess when the source is started.

        The subprocess will run as long as the run method execute. You should use it to fetch data and produce it to kafka.
        """

    @abstractmethod
    def cleanup(self, failed: bool) -> None:
        """
        This method is triggered once the `run` method completes.

        You can use it to perform any kind of shutting down cleanup. For example
        flushing the producer.
        """

    @abstractmethod
    def stop(self) -> None:
        """
        This method is triggered when the application is shutting down.

        The source should make sure it's `run` method completes soon.

        Example Snippet:

        ```python
        class MySource(BaseSource):
            def run(self):
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
        This method is triggered when the user hasn't specified a topic for the source.

        The source should return a default topic configuration
        """


class Source(BaseSource):
    """
    BaseSource class implementation providing

    Implementation for the abstract method:
        * `default_topic`

    Helper methods
        * serialize
        * produce
        * flush
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

    def serialize(
        self,
        key: Optional[object] = None,
        value: Optional[object] = None,
        headers: Optional[Headers] = None,
        timestamp_ms: Optional[int] = None,
    ) -> KafkaMessage:
        """
        Serialize data into a :class:`quixstreams.models.messages.KafkaMessage` using the producer topic serializers.

        :return: :class:`quixstreams.models.messages.KafkaMessage`
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
        Produce data to kafka using the source topic.
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

        It ensure all message are successfully delived to kafka

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
        Return a topic matching the source name.

        :return: `:class:`quixstreams.models.topics.Topic`
        """
        return Topic(
            name=self.name,
            value_deserializer="json",
            value_serializer="json",
            config=TopicConfig(num_partitions=1, replication_factor=1),
        )

    def __repr__(self):
        return self.name


class PollingSource(Source):
    """
    Source implementation for polling sources

    Periodically call the `poll` method to get a new message.
    """

    def __init__(
        self,
        name: str,
        polling_delay: float = 1,
        shutdown_timeout: float = 10,
    ) -> None:
        """
        :param name: The source unique name. Used to generate the topic configurtion
        :param polling_delay: Time in second the source will sleep when `poll` returns `None`
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown
        """
        super().__init__(name, shutdown_timeout)

        self._polling_delay = polling_delay
        self._stopping: Optional[threading.Event] = None

    def run(self) -> None:
        super().run()

        self._stopping = threading.Event()
        while not self._stopping.is_set():
            try:
                msg = self.poll()
            except StopIteration:
                return

            if msg is None:
                self.sleep(self._polling_delay)
                self._producer.poll()
                continue

            self.produce(
                key=msg.key,
                value=msg.value,
                headers=msg.headers,
                timestamp=msg.timestamp,
            )

    def sleep(self, seconds: float):
        """
        Sleep up to `seconds` seconds or raise `StopIteration` to shutdown the source
        """
        if self._stopping.wait(seconds):
            raise StopIteration(f"{self} shutdown")

    def stop(self) -> None:
        if self._stopping is not None:
            self._stopping.set()
        super().stop()

    def cleanup(self, failed: bool) -> None:
        super().cleanup(failed)
        self.flush(self.shutdown_timeout / 2)

    @abstractmethod
    def poll(self) -> Optional[KafkaMessage]:
        """
        This method is triggered when the source needs new data

        You can return :
            * a :class:`quixstreams.models.messages.KafkaMessage` to produce it
            * `None` to make the source sleep for `polling_delay`

        or raise a `StopIteration` to shutdown the source.
        """
        raise NotImplementedError(self.poll)
