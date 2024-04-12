import logging
from typing import Union, Optional, Callable

from confluent_kafka import (
    Producer as ConfluentProducer,
    KafkaError,
    Message,
)
from typing_extensions import Literal

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


def _default_error_cb(error: KafkaError):
    error_code = error.code()
    if str(error_code) == str(KafkaError._ALL_BROKERS_DOWN):
        logger.debug(error.str())
        return
    logger.error(
        f'Kafka producer error: {error.str()} code="{error_code}"',
    )


class Producer:
    def __init__(
        self,
        broker_address: str,
        partitioner: Partitioner = "murmur2",
        extra_config: Optional[dict] = None,
    ):
        """
        A wrapper around `confluent_kafka.Producer`.

        It initializes `confluent_kafka.Producer` on demand
        avoiding network calls during `__init__`, provides typing info for methods
        and some reasonable defaults.

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
        """
        config = dict(
            extra_config or {},
            **{
                "bootstrap.servers": broker_address,
                "partitioner": partitioner,
                "logger": logger,
                "error_cb": _default_error_cb,
            },
        )
        self._producer_config = config
        self._inner_producer: Optional[ConfluentProducer] = None

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
            None or -1 is infinite. Default: None

        :return: number of messages remaining to flush
        """
        return self._producer.flush(timeout=timeout if timeout is not None else -1)

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
