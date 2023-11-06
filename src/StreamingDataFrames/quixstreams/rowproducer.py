import logging
from typing import Optional, Any

from typing_extensions import Protocol

from .error_callbacks import ProducerErrorCallback, default_on_producer_error
from .kafka.producer import Producer, Partitioner
from .models import Topic, Row

logger = logging.getLogger(__name__)


class RowProducerProto(Protocol):
    def produce_row(
        self,
        row: Row,
        topic: Topic,
        key: Optional[Any] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
    ):
        ...


class RowProducer(Producer, RowProducerProto):
    """
    A producer class that is capable of serializing Rows to bytes and send them to Kafka.
    The serialization is performed according to the Topic serialization settings.

        It overrides `.subscribe()` method of Consumer class to accept `Topic`
        objects instead of strings.

        :param broker_address: Kafka broker host and port in format `<host>:<port>`.
            Passed as `bootstrap.servers` to `confluent_kafka.Producer`.
        :param partitioner: A function to be used to determine the outgoing message
            partition.
            Available values: "random", "consistent_random", "murmur2", "murmur2_random",
            "fnv1a", "fnv1a_random"
            Default - "murmur2".
        :param extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Producer` as is.
            Note: values passed as arguments override values in `extra_config`.
        :param on_error: a callback triggered when `RowProducer.produce_row()`
            or `RowProducer.poll()` fail`.
            If producer fails and the callback returns `True`, the exception
            will be logged but not propagated.
            The default callback logs an exception and returns `False`.
    """

    def __init__(
        self,
        broker_address: str,
        partitioner: Partitioner = "murmur2",
        extra_config: dict = None,
        on_error: Optional[ProducerErrorCallback] = None,
    ):
        super().__init__(
            broker_address=broker_address,
            partitioner=partitioner,
            extra_config=extra_config,
        )
        self._on_error: Optional[ProducerErrorCallback] = (
            on_error or default_on_producer_error
        )

    def produce_row(
        self,
        row: Row,
        topic: Topic,
        key: Optional[Any] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
    ):
        """
        Serialize Row to bytes according to the Topic serialization settings
        and produce it to Kafka

        If this method fails, it will trigger the provided "on_error" callback.

        :param row: Row object
        :param topic: Topic object
        :param key: message key, optional
        :param partition: partition number, optional
        :param timestamp: timestamp in milliseconds, optional
        """

        try:
            message = topic.row_serialize(row=row, key=key)
            self.produce(
                topic=topic.name,
                key=message.key,
                value=message.value,
                headers=message.headers,
                partition=partition,
                timestamp=timestamp,
            )
        except Exception as exc:
            to_suppress = self._on_error(exc, row, logger)
            if to_suppress:
                return
            raise

    def poll(self, timeout: float = None):
        """
        Polls the producer for events and calls `on_delivery` callbacks.

        If poll fails, it will trigger the provided "on_error" callback

        :param timeout: timeout in seconds
        """
        try:
            super().poll(timeout=timeout)
        except Exception as exc:
            to_suppress = self._on_error(exc, None, logger)
            if to_suppress:
                return
            raise
