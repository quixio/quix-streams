import functools
import logging
import time
import uuid
from typing import Callable, List, Optional, Union

from confluent_kafka import (
    KafkaError,
    Message,
    TopicPartition,
)
from confluent_kafka import (
    Producer as ConfluentProducer,
)
from confluent_kafka.admin import GroupMetadata

from quixstreams.models.types import Headers

from .configuration import ConnectionConfig

__all__ = (
    "Producer",
    "PRODUCER_ON_ERROR_RETRIES",
    "PRODUCER_POLL_TIMEOUT",
)

from .exceptions import InvalidProducerConfigError, KafkaBrokerUnavailableError

DeliveryCallback = Callable[[Optional[KafkaError], Message], None]

logger = logging.getLogger(__name__)

_SILENTLY_IGNORED_KAFKA_ERRORS = (
    # Broker handle destroyed - common/typical behavior, often seen via AdminClient
    KafkaError._DESTROY,  # noqa: SLF001
)

PRODUCER_POLL_TIMEOUT = 30.0
PRODUCER_ON_ERROR_RETRIES = 10
_BROKER_PROBE_INTERVAL = 30.0


def _default_error_cb(error: KafkaError):
    error_code = error.code()
    if error_code in _SILENTLY_IGNORED_KAFKA_ERRORS:
        logger.debug(error.str())
        return
    if error_code == KafkaError._ALL_BROKERS_DOWN:  # noqa: SLF001
        logger.warning(error.str())
        return
    logger.error(f'Kafka producer error: {error.str()} code="{error_code}"')


def ensure_transactional(func):
    @functools.wraps(func)
    def wrapper(producer: "Producer", *args, **kwargs):
        if not producer.transactional:
            raise InvalidProducerConfigError(
                "Producer must be initialized"
                " with `transactional=True` to use Kafka Transactions API"
            )
        return func(producer, *args, **kwargs)

    return wrapper


class Producer:
    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        logger: logging.Logger = logger,
        error_callback: Callable[[KafkaError], None] = _default_error_cb,
        extra_config: Optional[dict] = None,
        flush_timeout: Optional[float] = None,
        transactional: bool = False,
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
        :param transactional: if `True`, the Producer will go into the transactional
            mode where you can use the Kafka Transactions API.
            By default, the "transactional.id" is generated randomly on init.
            To customize it, pass it via the `extra_config` parameter.
        """
        if isinstance(broker_address, str):
            broker_address = ConnectionConfig(bootstrap_servers=broker_address)

        self._broker_unavailable_since: Optional[float] = None
        self._last_broker_probe: Optional[float] = None

        # Wrap the user-provided (or default) error callback so that broker
        # availability tracking always runs, regardless of custom callbacks.
        self._user_error_cb = error_callback
        error_callback = self._error_cb

        self._producer_config = {
            # previous Quix Streams defaults
            "partitioner": "murmur2",
            **(extra_config or {}),
            **broker_address.as_librdkafka_dict(),
            **{"logger": logger, "error_cb": error_callback},
        }
        # Provide additional config if producer uses transactions
        if transactional:
            self._producer_config.update(
                {
                    "enable.idempotence": True,
                    # Respect the transactional.id if it's passed
                    "transactional.id": self._producer_config.get(
                        "transactional.id", str(uuid.uuid4())
                    ),
                }
            )
        self._inner_producer: Optional[ConfluentProducer] = None
        self._flush_timeout = flush_timeout or -1
        self._transactional = transactional

    def produce(
        self,
        topic: str,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        poll_timeout: float = PRODUCER_POLL_TIMEOUT,
        buffer_error_max_tries: int = PRODUCER_ON_ERROR_RETRIES,
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

    @property
    def transactional(self) -> bool:
        return self._transactional

    @ensure_transactional
    def begin_transaction(self):
        self._producer.begin_transaction()

    @ensure_transactional
    def send_offsets_to_transaction(
        self,
        positions: List[TopicPartition],
        group_metadata: GroupMetadata,
        timeout: Optional[float] = None,
    ):
        self._producer.send_offsets_to_transaction(
            positions, group_metadata, timeout if timeout is not None else -1
        )

    @ensure_transactional
    def abort_transaction(self, timeout: Optional[float] = None):
        self._producer.abort_transaction(timeout if timeout is not None else -1)

    @ensure_transactional
    def commit_transaction(self, timeout: Optional[float] = None):
        self._producer.commit_transaction(timeout if timeout is not None else -1)

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
            if self._transactional:
                self._inner_producer.init_transactions()
        return self._inner_producer

    def _error_cb(self, error: KafkaError):
        """Instance-level error callback that tracks broker availability
        and delegates to the user-provided (or default) error callback."""
        error_code = error.code()
        if error_code == KafkaError._ALL_BROKERS_DOWN:  # noqa: SLF001
            if self._broker_unavailable_since is None:
                self._broker_unavailable_since = time.monotonic()
        self._user_error_cb(error)

    def _broker_available(self):
        """Reset the broker unavailability tracker."""
        if self._broker_unavailable_since is not None:
            elapsed = time.monotonic() - self._broker_unavailable_since
            logger.info(
                "Kafka producer broker connectivity restored after %.1fs.",
                elapsed,
            )
            self._broker_unavailable_since = None
            self._last_broker_probe = None

    def raise_if_broker_unavailable(self, timeout: float):
        """Raise if all brokers have been unavailable for longer than ``timeout`` seconds.

        Periodically performs an active metadata probe to detect recovery
        even when no messages are flowing (idle applications).

        :param timeout: seconds of continuous unavailability before raising.
        :raises KafkaBrokerUnavailableError: if the timeout has been exceeded.
        """
        if self._broker_unavailable_since is None:
            return

        now = time.monotonic()
        elapsed = now - self._broker_unavailable_since

        # Periodically probe to detect recovery, even before the timeout.
        since_last_probe = (
            now - self._last_broker_probe
            if self._last_broker_probe is not None
            else float("inf")
        )
        if since_last_probe >= _BROKER_PROBE_INTERVAL:
            self._last_broker_probe = now
            try:
                self._producer.list_topics(timeout=5.0)
                # Probe succeeded — brokers are actually reachable.
                self._broker_available()
                return
            except Exception:
                logger.debug("Broker availability probe failed", exc_info=True)

        if elapsed >= timeout:
            raise KafkaBrokerUnavailableError(
                f"All Kafka brokers have been unavailable for "
                f"{elapsed:.0f}s (timeout={timeout:.0f}s). "
                f"The application cannot recover automatically; "
                f"restarting is required. "
                f"Adjust via Application(broker_availability_timeout=...) "
                f"or set to 0 to disable."
            )

    def __len__(self):
        return len(self._producer)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Flushing kafka producer")
        self.flush()
        logger.debug("Kafka producer flushed")
