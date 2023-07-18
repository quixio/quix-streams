import asyncio
import concurrent.futures
import logging
from typing import List, Dict, Literal, Tuple
from typing import Union, Optional, Callable

from confluent_kafka import KafkaException, Producer, KafkaError, Message

from .exceptions import AsyncProducerNotStartedError

__all__ = (
    "AsyncProducer",
    "Partitioner",
)


Partitioner = Literal[
    "random", "consistent_random", "murmur2", "murmur2_random", "fnv1a", "fnv1a_random"
]
HeaderValue = Union[str | bytes | None]
Headers = Union[
    List[Tuple[str, HeaderValue]],
    Dict[str, HeaderValue],
]

logger = logging.getLogger(__name__)
confluent_logger = logging.getLogger("confluent_kafka.producer")


def _default_error_cb(error: KafkaError):
    logger.error(
        "Kafka producer error",
        extra={"error_code": error.code(), "error_desc": error.str()},
    )


class AsyncProducer:
    """
    Asyncio-friendly wrapper around `confluent_kafka.Producer`.
    It replicates the same interface and delegates all IO operations to threads.

    AsyncProducer maintains one background thread to make `poll()` calls
    and receive `on_delivery()` callbacks.

    :param broker_address: Kafka broker address.
    :param partitioner: A function to be used to determine the outgoing message
        partition. Default - 'murmur2'.
        See `Partitioner` type for available options.
    :param loop: the instance of `asyncio.AbstractEventLoop` that will be used
        to schedule callbacks from threads.
        Default - the result of `asyncio.get_event_loop()`.
    :param extra_config: The dictionary with additional params for the underlying
        `confluent_kafka.Producer`. The values of the dictionary don't override
        the values passed in named parameters.
    """

    def __init__(
        self,
        broker_address: str,
        partitioner: Partitioner = "murmur2",
        extra_config: dict = None,
        loop=None,
        poll_timeout: float = 0.1,
    ):
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
        self._loop = loop or asyncio.get_event_loop()
        self._running = False
        self._sync_producer: Optional[Producer] = None
        self._poll_executor = concurrent.futures.ThreadPoolExecutor(1)
        self._poll_timeout = poll_timeout
        self._poll_task: Optional[asyncio.Task] = None

    async def produce(
        self,
        topic: str,
        value: Union[str, bytes],
        key: Union[str, bytes] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        blocking: bool = False,
    ):
        delivery = self._loop.create_future()

        def on_delivery(err: Optional[KafkaError], msg: Message):
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
                self._loop.call_soon_threadsafe(
                    delivery.set_exception, KafkaException(err)
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
                self._loop.call_soon_threadsafe(delivery.set_result, msg)

        kwargs = {
            "on_delivery": on_delivery,
            "partition": partition,
            "timestamp": timestamp,
            "headers": headers,
        }
        # confluent_kafka doesn't like None for optional parameters
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        self._producer.produce(topic, value, key, **kwargs)

        if blocking:
            await delivery

    async def poll(self, timeout: float = None):
        return await self._run_poll_executor(self._producer.poll, timeout)

    async def flush(self, timeout: float = None) -> int:
        args_ = [arg for arg in (timeout,) if arg is not None]
        return await self._run_poll_executor(self._producer.flush, *args_)

    async def start(self):
        # Create producer
        self._sync_producer = await self._create_producer()
        # Start polling to receive on_delivery callbacks
        self._running = True
        self._poll_task = asyncio.create_task(self._poll_forever())

    async def close(self):
        # Flush the messages to Kafka and handle delivery callbacks
        await self.flush()
        # Stop the poll loop
        self._running = False
        await self._poll_task

    @property
    def _producer(self) -> Producer:
        if not self._sync_producer:
            raise AsyncProducerNotStartedError("Producer is not started yet")
        return self._sync_producer

    def __len__(self):
        return len(self._producer)

    async def _create_producer(self) -> Producer:
        producer = await self._run_poll_executor(Producer, self._producer_config)
        return producer

    async def _poll_forever(self):
        while self._running:
            await self.poll(timeout=self._poll_timeout)

    async def _run_poll_executor(self, func: Callable, *args):
        return await self._loop.run_in_executor(self._poll_executor, func, *args)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        # Stop the thread pool. Warning: blocking call
        self._poll_executor.shutdown()
