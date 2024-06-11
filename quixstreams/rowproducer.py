from typing import Optional, Any, Union, Dict, Tuple, List, cast

from confluent_kafka import TopicPartition, KafkaException, KafkaError, Message
from confluent_kafka.admin import GroupMetadata

import logging

from quixstreams.exceptions import QuixException
from .error_callbacks import ProducerErrorCallback, default_on_producer_error
from .kafka.configuration import ConnectionConfig
from .kafka.exceptions import KafkaProducerDeliveryError
from .kafka.producer import Producer, TransactionalProducer
from .models import Topic, Row, Headers

logger = logging.getLogger(__name__)


_KEY_UNSET = object()


class KafkaProducerTransactionCommitFailed(QuixException): ...


class RowProducer:
    """
    A producer class that is capable of serializing Rows to bytes and send them to Kafka.
    The serialization is performed according to the Topic serialization settings.

    :param broker_address: Connection settings for Kafka.
        Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
        or a ConnectionConfig object if authentication is required.
    :param extra_config: A dictionary with additional options that
        will be passed to `confluent_kafka.Producer` as is.
        Note: values passed as arguments override values in `extra_config`.
    :param on_error: a callback triggered when `RowProducer.produce_row()`
        or `RowProducer.poll()` fail`.
        If producer fails and the callback returns `True`, the exception
        will be logged but not propagated.
        The default callback logs an exception and returns `False`.
    :param flush_timeout: The time the producer is waiting for all messages to be delivered.
    """

    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        extra_config: dict = None,
        on_error: Optional[ProducerErrorCallback] = None,
        flush_timeout: Optional[int] = None,
        transactional: bool = False,
    ):

        if transactional:
            self._producer = TransactionalProducer(
                broker_address=broker_address,
                extra_config=extra_config,
                flush_timeout=flush_timeout,
            )
        else:
            self._producer = Producer(
                broker_address=broker_address,
                extra_config=extra_config,
                flush_timeout=flush_timeout,
            )

        self._on_error: Optional[ProducerErrorCallback] = (
            on_error or default_on_producer_error
        )
        self._tp_offsets: Dict[Tuple[str, int], int] = {}
        self._error: Optional[KafkaError] = None
        self._transactional = transactional

    def produce_row(
        self,
        row: Row,
        topic: Topic,
        key: Optional[Any] = _KEY_UNSET,
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
            # Use existing key only if no other key is provided.
            # If key is provided - use it, even if it's None
            key = row.key if key is _KEY_UNSET else key
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

        If `poll()` fails, it will trigger the provided "on_error" callback

        :param timeout: timeout in seconds
        """
        try:
            self._producer.poll(timeout=timeout)
        except Exception as exc:
            to_suppress = self._on_error(exc, None, logger)
            if to_suppress:
                return
            raise

    def produce(
        self,
        topic: str,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        poll_timeout: float = 5.0,
        buffer_error_max_tries: int = 3,
    ):
        self._raise_for_error()

        return self._producer.produce(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp=timestamp,
            poll_timeout=poll_timeout,
            buffer_error_max_tries=buffer_error_max_tries,
            on_delivery=self._on_delivery,
        )

    def _on_delivery(self, err: Optional[KafkaError], msg: Message):
        if self._error is not None:
            # There's an error already set
            return

        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        if err is None:
            self._tp_offsets[(topic, partition)] = offset
        else:
            self._error = err

    def _raise_for_error(self):
        if self._error is not None:
            exc = KafkaProducerDeliveryError(self._error)
            self._error = None
            raise exc

    def flush(self, timeout: Optional[float] = None) -> int:
        result = self._producer.flush(timeout=timeout)
        self._raise_for_error()
        return result

    @property
    def offsets(self) -> Dict[Tuple[str, int], int]:
        return self._tp_offsets

    def begin_transaction(self):
        self._producer.begin_transaction()

    def abort_transaction(self, timeout: Optional[float] = None):
        # Skip abort if no active transaction since it throws an exception if at least
        # one transaction was successfully completed at some point.
        # This avoids polluting the stack trace in the case where a transaction was
        # not active as expected (because of some other exception already raised).
        if self._producer.active_transaction:
            self._producer.abort_transaction(timeout)
        else:
            logger.debug(
                "No Kafka transaction to abort, "
                "likely due to some other exception occurring"
            )

    def commit_transaction(
        self,
        positions: List[TopicPartition],
        group_metadata: GroupMetadata,
        timeout: Optional[float] = None,
    ):
        attempts_remaining = 3
        backoff_seconds = 1
        while attempts_remaining:
            try:
                self._producer.send_offsets_to_transaction(
                    positions, group_metadata, timeout=timeout
                )
                self._producer.commit_transaction(timeout=timeout)
                return
            # Errors do not manifest from these calls via producer error_cb.
            # NOTE: Manual flushing earlier keeps error handling here to a minimum.
            except KafkaException as e:
                error: KafkaError = e.args[0]
                if error.retriable():
                    attempts_remaining -= 1
                    logger.debug(
                        f"Kafka Transaction commit attempt failed, but is retriable; "
                        f"attempts remaining: {attempts_remaining}. "
                    )
                    if attempts_remaining:
                        logger.debug(
                            f"Sleeping for {backoff_seconds} seconds before retrying."
                        )
                        sleep(backoff_seconds)
                else:
                    # Just treat all errors besides retriable as fatal.
                    logger.error("Error while attempting to commit Kafka transaction.")
                    raise
        raise KafkaProducerTransactionCommitFailed(
            "All Kafka transaction commit attempts failed; "
            "aborting transaction and shutting down Application..."
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
