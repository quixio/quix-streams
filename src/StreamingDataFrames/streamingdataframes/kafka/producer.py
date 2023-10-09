import logging
from typing import List, Dict, Literal, Tuple
from typing import Union, Optional

from confluent_kafka import (
    Producer as ConfluentProducer,
    KafkaError,
    Message,
)

__all__ = (
    "Producer",
    "Partitioner",
)

Partitioner = Literal[
    "random", "consistent_random", "murmur2", "murmur2_random", "fnv1a", "fnv1a_random"
]
HeaderValue = Optional[Union[str, bytes]]
Headers = Union[
    List[Tuple[str, HeaderValue]],
    Dict[str, HeaderValue],
]

logger = logging.getLogger(__name__)
confluent_logger = logging.getLogger(__name__)


def _default_error_cb(error: KafkaError):
    logger.error(
        "Kafka producer error",
        extra={"error_code": error.code(), "error_desc": error.str()},
    )


def _on_delivery_cb(err: Optional[KafkaError], msg: Message):
    if err is not None:
        logger.error(
            "Failed to deliver a message",
            extra={
                "key": msg.key(),
                "topic": msg.topic(),
                "partition": msg.partition(),
                "error_code": err.code(),
                "error_desc": err.str(),
            },
        )
    else:
        logger.debug(
            "Successfully delivered a message",
            extra={
                "key": msg.key(),
                "topic": msg.topic(),
                "partition": msg.partition(),
            },
        )


class Producer:
    def __init__(
        self,
        broker_address: str,
        partitioner: Partitioner = "murmur2",
        extra_config: dict = None,
        debug: bool = False,
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
        :param debug: if True, the Producer will log the data from the delivery callbacks.
            Note: enabling the debug mode may reduce the throughput.
        """
        config = dict(
            **extra_config or {},
            **{
                "bootstrap.servers": broker_address,
                "partitioner": partitioner,
                "logger": confluent_logger,
                "error_cb": _default_error_cb,
            },
        )
        self._producer_config = config
        self._debug = debug
        self._inner_producer: Optional[ConfluentProducer] = None

    def produce(
        self,
        topic: str,
        value: Union[str, bytes],
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        poll_timeout: float = 5.0,
        buffer_error_max_tries: int = 3,
    ):
        """
        Produce message to topic.
        It also polls Kafka for callbacks before producing in order to minimize
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

        """

        kwargs = {
            "partition": partition,
            "timestamp": timestamp,
            "headers": headers,
        }
        if self._debug:
            # Trigger `on_delivery` callbacks only in "debug" mode, otherwise
            # it reduces throughput because of additional function calls
            kwargs["on_delivery"] = _on_delivery_cb

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

    def poll(self, timeout: float = None):
        """
        Polls the producer for events and calls `on_delivery` callbacks.
        :param timeout: poll timeout seconds
        """
        return self._producer.poll(timeout)

    def flush(self, timeout: float = None) -> int:
        """
        Wait for all messages in the Producer queue to be delivered.

        :param timeout: timeout is seconds
        :return: number of messages delivered
        """
        args_ = [arg for arg in (timeout,) if arg is not None]
        return self._producer.flush(*args_)

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
        logger.debug("Flushing Kafka Producer")
        self.flush()
