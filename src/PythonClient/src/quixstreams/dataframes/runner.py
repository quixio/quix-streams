import contextlib
import logging
from typing import Optional, Callable

from confluent_kafka import TopicPartition

from .dataframe import StreamingDataFrame
from .error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
    default_on_processing_error,
)
from .kafka import AutoOffsetReset, AssignmentStrategy, Partitioner
from .rowconsumer import RowConsumer
from .rowproducer import RowProducer
from .exceptions import QuixException


logger = logging.getLogger(__name__)

MessageProcessedCallback = Callable[[str, int, int], None]

__all__ = (
    "Runner",
    "RunnerNotStarted",
)


class RunnerNotStarted(QuixException):
    """
    Runner is not started yet
    """


class Runner:
    def __init__(
        self,
        broker_address: str,
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        assignment_strategy: AssignmentStrategy = "range",
        partitioner: Partitioner = "murmur2",
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
    ):
        """
        Class to run StreamingDataFrames.

        What it does:
            - Passes deserialized messages to StreamingDataFrame
            - Configures and maintains the lifetime of Consumer and Producer
            - Stores messages offsets after they're processed

        Example usage:
        ```
            runner = Runner(broker_address='localhost:9092', consumer_group='group')
            df = StreamingDataFrame()

            with runner:
                runner.run(dataframe=df)
        ```

        :param broker_address: Kafka broker host and port in format `<host>:<port>`.
            Passed as `bootstrap.servers` to `confluent_kafka.Consumer`.
        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`
        :param auto_offset_reset: Consumer `auto.offset.reset` setting
        :param auto_commit_enable: If true, periodically commit offset of
            the last message handed to the application. Default - `True`.
        :param assignment_strategy: The name of a partition assignment strategy.
        :param partitioner: A function to be used to determine the outgoing message
            partition.
        :param consumer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
        :param producer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Producer` as is.
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`. Default - 1.0s
        :param producer_poll_timeout: timeout for `RowProducer.poll()`. Default - 0s.
        :param on_message_processed: a callback triggered when message is successfully
            processed.

        To handle errors, `Runner` accepts callbacks triggered when exceptions occur
        on different stages of stream processing.
        If the callback returns `True`, the exception will be ignored. Otherwise, the
        exception will be propagated and the processing will eventually stop.

        :param on_consumer_error: triggered when internal `RowConsumer` fails
        to poll Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when RowProducer fails to serialize
            or to produce a message to Kafka.

        """
        self.consumer = RowConsumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            assignment_strategy=assignment_strategy,
            extra_config=consumer_extra_config,
            on_error=on_consumer_error,
        )
        self.producer = RowProducer(
            broker_address=broker_address,
            partitioner=partitioner,
            extra_config=producer_extra_config,
            on_error=on_producer_error,
        )
        self._consumer_poll_timeout = consumer_poll_timeout
        self._producer_poll_timeout = producer_poll_timeout
        self._running = False
        self._on_processing_error = on_processing_error or default_on_processing_error
        self._on_message_processed = on_message_processed
        self._exit_stack = contextlib.ExitStack()

    def start(self):
        self._exit_stack.enter_context(self.producer)
        self._exit_stack.enter_context(self.consumer)
        self._running = True

    def stop(self):
        self._running = False
        self._exit_stack.close()

    def run(
        self,
        dataframe: StreamingDataFrame,
    ):
        """
        Start processing data from Kafka using provided StreamingDataFrame

        :param dataframe: StreamingDataFrame instance
        """
        if not self._running:
            raise RunnerNotStarted("Runner is not yet started")

        logger.info("Start processing of the streaming dataframe")

        # Provide Consumer and Producer instances to StreamingDataFrame
        dataframe.producer = self.producer
        dataframe.consumer = self.consumer

        # Get a list of input topics for this Dataframe
        topics = list(dataframe.topics.values())
        # Subscribe to topics in Kafka and start polling
        self.consumer.subscribe(topics)
        # Start polling Kafka for messages and callbacks
        while self._running:
            # Serve producer callbacks
            self.producer.poll(self._producer_poll_timeout)
            rows = self.consumer.poll_row(timeout=self._consumer_poll_timeout)

            if rows is None:
                continue

            # Deserializer may return multiple rows for a single message
            rows = rows if isinstance(rows, list) else [rows]
            if not rows:
                continue

            first_row = rows[0]

            for row in rows:
                try:
                    dataframe.process(row=row)
                except Exception as exc:
                    # TODO: This callback might be triggered because of Producer
                    #  errors too because they happen within ".process()"
                    to_suppress = self._on_processing_error(exc, row, logger)
                    if not to_suppress:
                        raise

            topic_name, partition, offset = (
                first_row.topic,
                first_row.partition,
                first_row.offset,
            )
            # Store the message offset after it's successfully processed
            self.consumer.store_offsets(
                offsets=[
                    TopicPartition(
                        topic=topic_name,
                        partition=partition,
                        offset=offset,
                    )
                ]
            )

            if self._on_message_processed is not None:
                self._on_message_processed(topic_name, partition, offset)

        logger.info("Stop processing of the streaming dataframe")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
