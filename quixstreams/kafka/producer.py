import logging
from typing import Union, Optional, Callable

from confluent_kafka import (
    Producer as ConfluentProducer,
    KafkaError,
    Message,
)
from typing_extensions import Literal

from .configuration import ConnectionConfig
from quixstreams.models.types import Headers

__all__ = (
    "Producer",
    "Partitioner",
)

Partitioner = Literal[
    "random", "consistent_random", "murmur2", "murmur2_random", "fnv1a", "fnv1a_random"
]

DeliveryCallback = Callable[[Optional[KafkaError], Message], None]

logger = logging.getLogger(__name__)

IGNORED_KAFKA_ERRORS = (
    # This error seems to be thrown despite brokers being available.
    # Seems linked to `connections.max.idle.ms`.
    KafkaError._ALL_BROKERS_DOWN,
    # Broker handle destroyed - common/typical behavior, often seen via AdminClient
    KafkaError._DESTROY,
)


def _default_error_cb(error: KafkaError):
    error_code = error.code()
    if error_code in IGNORED_KAFKA_ERRORS:
        logger.debug(error.str())
        return
    logger.error(f'Kafka producer error: {error.str()} code="{error_code}"')


class Producer:
    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        logger: logging.Logger = logger,
        error_callback: Callable[[KafkaError], None] = _default_error_cb,
        extra_config: Optional[dict] = None,
        flush_timeout: Optional[int] = None,
    ):
        """
        A wrapper around `confluent_kafka.Producer`.

        It initializes `confluent_kafka.Producer` on demand
        avoiding network calls during `__init__`, provides typing info for methods
        and some reasonable defaults.

        :param broker_address: Connection settings for Kafka.
            Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
            or a ConnectionConfig object if authentication is required.
        :param logger: a Logger instance to attach librdkafka logging to
        :param error_callback: callback used for producer errors
        :param extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Producer` as is.
            Note: values passed as arguments override values in `extra_config`.
        :param flush_timeout: The time the producer is waiting for all messages to be delivered.
        """
        if isinstance(broker_address, str):
            broker_address = ConnectionConfig(bootstrap_servers=broker_address)

        self._producer_config = {
            # previous Quix Streams defaults
            "partitioner": "murmur2",
            **(extra_config or {}),
            **broker_address.as_librdkafka_dict(),
            **{"logger": logger, "error_cb": error_callback},
        }
        self._inner_producer: Optional[ConfluentProducer] = None
        self._flush_timeout = flush_timeout or -1

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
        on_delivery: Optional[DeliveryCallback] = None,
    ):
        """
        Produce a message to a topic.

        It also polls Kafka for callbacks before producing to minimize
        the probability of `BufferError`.
        If `BufferError` still happens, the method will poll Kafka with timeout
        to free up the buffer and try again.

        :param topic: topic name
        :param value: message value
        :param key: message key
        :param headers: message headers
        :param partition: topic partition
        :param timestamp: message timestamp
        :param poll_timeout: timeout for `poll()` call in case of `BufferError`
        :param buffer_error_max_tries: max retries for `BufferError`.
            Pass `0` to not retry after `BufferError`.
        :param on_delivery: the delivery callback to be triggered on `poll()`
            for the produced message.

        """

        kwargs = {
            "partition": partition,
            "timestamp": timestamp,
            "headers": headers,
            "on_delivery": on_delivery,
        }

        # confluent_kafka doesn't like None for optional parameters
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        # Retry BufferError automatically 3 times before failing
        tried = 0
        while True:
            try:
                tried += 1
                self._producer.produce(topic, value, key, **kwargs)
                # Serve delivery callbacks after each produce to minimize a chance of
                # BufferError
                self._producer.poll(0)
                return
            except BufferError:
                if buffer_error_max_tries < 1 or tried == buffer_error_max_tries:
                    raise
                # The librdkafka buffer is full
                # Poll for delivery callbacks and try again
                self._producer.poll(timeout=poll_timeout)

    def poll(self, timeout: float = 0):
        """
        Polls the producer for events and calls `on_delivery` callbacks.
        :param timeout: poll timeout seconds; Default: 0 (unlike others)
            > NOTE: -1 will hang indefinitely if there are no messages to acknowledge
        """
        return self._producer.poll(timeout=timeout)

    def flush(self, timeout: Optional[float] = None) -> int:
        """
        Wait for all messages in the Producer queue to be delivered.

        :param float timeout: time to attempt flushing (seconds).
            None use producer default or -1 is infinite. Default: None

        :return: number of messages remaining to flush
        """
        return self._producer.flush(
            timeout=timeout if timeout is not None else self._flush_timeout
        )

    @property
    def _producer(self) -> ConfluentProducer:
        if not self._inner_producer:
            self._inner_producer = ConfluentProducer(self._producer_config)
        return self._inner_producer

    def __len__(self):
        return len(self._producer)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Flushing kafka producer")
        self.flush()
        logger.debug("Kafka producer flushed")
