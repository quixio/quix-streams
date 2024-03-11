import abc
import logging
import signal
from typing import Iterable, Union, Optional, List, Any

from confluent_kafka import TopicPartition

from quixstreams import Application
from quixstreams.kafka import AutoOffsetReset, Consumer
from quixstreams.rowconsumer import KafkaMessageError
from ..base.batching import BatchStore, Batch

logger = logging.getLogger("quixstreams")


class BaseSink(abc.ABC):
    def __init__(
        self,
        broker_address: str,
        topic: Union[str, Iterable[str]],
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset = "latest",
        consumer_extra_config: Optional[dict] = None,
    ):
        self._running = False
        self.broker_address = broker_address
        self.consumer_group = consumer_group
        self.app = Application(
            broker_address=self.broker_address,
            consumer_group=self.consumer_group,
            auto_offset_reset=auto_offset_reset,
            consumer_extra_config=consumer_extra_config,
            auto_commit_enable=False,
            auto_create_topics=False,
        )
        topic = [topic] if isinstance(topic, str) else topic
        self._validate_topics(topic)
        self.topics = topic

    @abc.abstractmethod
    def run(self):
        ...

    def start(self):
        self._running = True
        self._setup_signal_handlers()
        logger.info(
            f'Starting the sink class="{self.__class__}" '
            f'broker_address="{self.broker_address}" '
            f'topics="{", ".join(self.topics)}" '
            f'consumer_group="{self.consumer_group}"'
        )

    def stop(self):
        """
        Stop the internal poll loop and the message processing.

        Only necessary when manually managing the lifecycle of the `Application` (
        likely through some sort of threading).

        To otherwise stop an application, either send a `SIGTERM` to the process
        (like Kubernetes does) or perform a typical `KeyboardInterrupt` (`Ctrl+C`).
        """
        logger.info("Stopping the sink. All incomplete batches will not be uploaded")
        self._running = False

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._on_sigint)
        signal.signal(signal.SIGTERM, self._on_sigterm)

    def _on_sigint(self, *_):
        # Re-install the default SIGINT handler so doing Ctrl+C twice
        # raises KeyboardInterrupt
        signal.signal(signal.SIGINT, signal.default_int_handler)
        logger.debug(f"Received SIGINT, stopping the processing loop")
        self.stop()

    def _on_sigterm(self, *_):
        logger.debug(f"Received SIGTERM, stopping the processing loop")
        self.stop()

    def _validate_topics(self, topics: Iterable[str]):
        for topic in topics:
            if not isinstance(topic, str):
                raise ValueError(
                    f'Topic names must be strings, got "{type(topic)}": {topic}'
                )


class BaseBatchingSink(BaseSink):
    def __init__(
        self,
        batch_max_messages: int,
        batch_max_interval_seconds: float = 0.0,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._batch_max_messages = batch_max_messages
        self._batch_max_interval_seconds = batch_max_interval_seconds

        self._batch_store = BatchStore(
            batch_max_messages=self._batch_max_messages,
            batch_max_interval_seconds=self._batch_max_interval_seconds,
        )
        self._debug_logs_enabled = logger.isEnabledFor(logging.DEBUG)

    @abc.abstractmethod
    def deserialize_value(self, value: bytes) -> Any:
        ...

    @abc.abstractmethod
    def flush(self, batch: Batch):
        ...

    def run(self):
        """
        Run the Sink and start the consumer loop

        :return:
        """
        self.start()
        with self.app.get_consumer() as consumer:
            # Subscribe to the topics
            consumer.subscribe(
                topics=self.topics,
                on_revoke=self._consumer_on_revoke,
                on_lost=self._consumer_on_lost,
            )
            logger.info("Waiting for incoming messages")
            while self._running:
                # Check if any accumulated batches are ready to be uploaded
                self.maybe_flush_and_commit(consumer=consumer)

                # Consume messages from the assigned topic partitions
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    raise KafkaMessageError(error=msg.error())

                if self._debug_logs_enabled:
                    logger.debug(
                        f"Received a message "
                        f'partition="{msg.topic()}[{msg.partition()}]" '
                        f'key="{msg.key()}"'
                    )

                value = msg.value()  # noqa
                topic, partition, offset = (
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                if value is None:
                    logger.info(
                        f'Skipping an empty message from topic "{topic}[{partition}]"'
                    )
                    continue

                # Deserialize a message value into a Python object
                deserialized = self.deserialize_value(value)

                # Add an object to the batch according to its topic and partition
                batch = self._batch_store.get_batch(
                    topic=topic, partition=partition, offset=offset
                )
                batch.add(value=deserialized, offset=offset)

            logger.info("The sink is stopped")

    def maybe_flush_and_commit(self, consumer: Consumer):
        for batch in self._batch_store.list_batches():
            if batch.empty:
                continue

            if batch.full:
                logger.info(f"Batch {batch} is full and ready for upload")
            elif batch.expired:
                logger.info(f"Batch {batch} is expired and ready for upload")
            else:
                continue

            self.flush(batch=batch)
            consumer.commit(
                offsets=[
                    TopicPartition(
                        topic=batch.topic,
                        partition=batch.partition,
                        offset=batch.end_offset + 1,
                    )
                ],
                asynchronous=False,
            )
            self._batch_store.drop_batch(topic=batch.topic, partition=batch.partition)

    def _consumer_on_revoke(self, _, partitions: List[TopicPartition]):
        for tp in partitions:
            logger.debug(
                f'Partition "{tp.topic}[{tp.partition}]" is revoked, '
                f"dropping the accumulated batch."
            )
            self._batch_store.drop_batch(topic=tp.topic, partition=tp.partition)

    def _consumer_on_lost(self, _, partitions: List[TopicPartition]):
        for tp in partitions:
            logger.debug(
                f'Partition "{tp.topic}[{tp.partition}]" is lost, '
                f"dropping the accumulated batch."
            )
            self._batch_store.drop_batch(topic=tp.topic, partition=tp.partition)
