import logging
import os

from abc import ABC, abstractmethod

from quixstreams import Application
from quixstreams.kafka import Producer
from quixstreams.models.serializers import SerializerType
from quixstreams.models.topics import Topic
from quixstreams.models.types import MessageKey, MessageValue
from typing import Optional, Any
from typing_extensions import Self

# maybe rename Consumer to "Reader" to not confuse it with a kafka consumer?

logger = logging.getLogger(__name__)

__all__ = ("SourceConnector", "SourceProducer")


class MissingBrokerAddress(Exception):
    pass


class SourceProducer:
    """
    Responsible for producing a kafka-formatted message to a topic

    Generally will just be re-used everywhere, hence no protocol
    """

    def __init__(
        self,
        topic_name: str,
        broker_address: Optional[str] = None,
        key_serializer: SerializerType = "string",
        value_serializer: SerializerType = "json",
    ):
        """
        :param topic_name: name of topic
        :param broker_address: broker address (if Quix environment not defined)
        :param key_serializer: kafka key serializer; Default "string"
        :param value_serializer: kafka value serializer; Default "json"
        """

        if os.environ.get("Quix__Sdk__Token"):
            self._quix_app = Application.Quix("none")
        elif broker_address:
            self._quix_app = Application(broker_address, "none")
        else:
            raise MissingBrokerAddress("Missing broker address for producer.")
        self._topic: Topic = self._quix_app.topic(
            topic_name, key_serializer=key_serializer, value_serializer=value_serializer
        )
        self._producer: Optional[Producer] = None

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def produce(
        self,
        key: Optional[MessageKey] = None,
        value: Optional[MessageValue] = None,
    ):
        """
        :param key: message key
        :param value: message value
        """
        logger.debug(f"Producing {key}: {value}")
        self._producer.produce(key=key, value=value, topic=self._topic.name)

    def start(self):
        self._producer = self._quix_app.get_producer()
        self._producer.__enter__()

    def stop(self):
        self._producer.flush()


class SourceConnector(ABC):
    """
    Responsible for getting data from a source and producing it to kafka
    """

    @property
    @abstractmethod
    def _producer(self) -> SourceProducer:
        ...

    def __enter__(self) -> Self:
        """Does "_start" with context manager"""
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Does "_stop" with context manager"""
        self._stop()

    @abstractmethod
    def _key_extractor(self, data: Any) -> Optional[MessageKey]:
        """Generate a message key from the raw source data"""
        ...

    @abstractmethod
    def _value_extractor(self, data: Any) -> Optional[MessageValue]:
        """Generate a message value from the raw source data"""
        ...

    @abstractmethod
    def _start(self) -> Self:
        """Initialize any source resources (i.e. a session object, etc.)"""
        ...

    @abstractmethod
    def _stop(self):
        """Clean up/close any source resources (i.e. a session object, etc.)"""
        ...

    @abstractmethod
    def run(self):
        ...
