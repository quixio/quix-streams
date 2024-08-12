import threading
import logging

from abc import ABC, abstractmethod
from typing import Optional, Union


from quixstreams.models import Topic, TopicManager
from quixstreams.models.messages import KafkaMessage
from quixstreams.models.types import Headers
from quixstreams.rowproducer import RowProducer

from quixstreams.checkpointing.exceptions import CheckpointProducerTimeout

logger = logging.getLogger(__name__)

__all__ = (
    "BaseSource",
    "Source",
    "PollingSource",
    "SourceStoppingException",
)


class BaseSource(ABC):
    """
    This is the base class for all sources.

    Source producer only support at-least-once delivery.

    Subclass it and implement its methods to create your own source.
    """

    # time in seconds the application will wait for the source to stop.
    shutdown_timeout: int = 10

    def __init__(self):
        self._producer: Optional[RowProducer] = None
        self._producer_topic: Optional[Topic] = None
        self._configured: bool = False

    def configure(self, topic: Topic, producer: RowProducer) -> None:
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
    def checkpoint(self) -> None:
        """
        This method is triggered by the application when it commits.

        You can flush the producer here to ensure all messages are successfully produced. This method is triggered in a
        different thread than the `run` method. Locking can be necessary.
        """

    @abstractmethod
    def run(self) -> None:
        """
        This method is triggered once when the source is started.

        The source is considered running for as long as the run method execute.
        """

    @abstractmethod
    def stop(self) -> None:
        """
        This method is triggered either when the source `run` method as completed or when it needs to complete.

        The SourceManager will wait up to `shutdown_timeout` seconds after calling `stop` for the `run` method to complete.
        """

    @abstractmethod
    def default_topic(self, topic_manager: TopicManager) -> Topic:
        """
        This method is triggered when the user hasn't specified a topic for the source.

        In this case the source can define a default topic using the topic manager to create one.
        """


class Source(BaseSource):
    """
    This is an helper implementation for sources.

    It provides implementation for some abstract method.
        * checkpoint
        * stop
        * default_topic

    Helper methods
        * serialize
        * produce
        * sleep

    A `stopping` :class:`threading.Event` to handle graceful shutdown and a lock to handle concurrent `produce` and `checkpoint` calls
    """

    def __init__(self, name: str, shutdown_timeout: int = 10) -> None:
        """
        :param name: The source unique name. Used to generate the default topic
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown
        """
        super().__init__()

        # used to generate a unique topic for the source.
        self.name = name

        self.shutdown_timeout = shutdown_timeout

        self._topic: Optional[Topic] = None
        self._producer: Optional[RowProducer] = None

        self._lock = threading.Lock()
        self.stopping = threading.Event()

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

        with self._lock:
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

    def checkpoint(self) -> None:
        """
        This method is triggered by the application when it commits.

        It flushes the kafka producer and raise an error if any message
        fails to be produced. You can override it to implement any
        additional operations needed when checkpointing.
        """
        with self._lock:
            logger.debug("checkpoint: checkpointing source %s", self)
            unproduced_msg_count = self._producer.flush()
            if unproduced_msg_count > 0:
                raise CheckpointProducerTimeout(
                    f"'{unproduced_msg_count}' messages failed to be produced before the producer flush timeout"
                )

    def sleep(self, seconds: float) -> None:
        """
        Helper method uses to put the source to sleep. It will raise a `SourceStoppingException` whenever the source needs to stop.

        :raises: `SourceStoppingException` when the source needs to stop.
        """
        if self.stopping.wait(seconds):
            raise SourceStoppingException("shutdown")

    def default_topic(self, topic_manager: TopicManager) -> Topic:
        """
        This method is triggered when the user hasn't specified a topic for the source.

        Return a topic matching the source name.

        :return: `:class:`quixstreams.models.topics.Topic`
        """
        return topic_manager.topic(self.name)

    def stop(self) -> None:
        """
        This method is triggered when the source needs to be stopped.
        """
        logger.info("stopping source %s", self)
        self.stopping.set()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.name})>"


class PollingSource(Source):
    """
    Base source implementation for polling sources.

    Implementations should override the `poll` method and return a :class:`quixstreams.models.messages.KafkaMessage` on every call.
    """

    def run(self) -> None:
        while not self.stopping.is_set():
            msg = self.poll()
            if msg is None:
                continue

            self.produce(
                key=msg.key,
                value=msg.value,
                headers=msg.headers,
                timestamp=msg.timestamp,
            )

    @abstractmethod
    def poll(self) -> Optional[KafkaMessage]:
        raise NotImplementedError(self.poll)


class SourceStoppingException(Exception):
    """
    Exception raised when a source is stopping
    """

    pass
