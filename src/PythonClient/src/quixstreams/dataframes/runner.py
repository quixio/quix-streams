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

logger = logging.getLogger(__name__)

MessageProcessedCallback = Callable[[str, int, int], None]


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

        To handle errors, `Runner` accepts callbacks triggerred when exceptions occur
        on different stages of stream processing.
        If the callback returns `True`, the exception will be ignored. Otherwise, the
        exception will be propagated and the processing will eventually stop.

        :param on_consumer_error: triggerred when internal `RowConsumer` fails
        to poll Kafka or cannot deserialize a message.
        :param on_processing_error: triggerred when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when RowProducer fails to serialize
            or to produce a message to Kafka.

        """
        self._broker_address = broker_address
        self._consumer_group = consumer_group
        self._auto_offset_reset = auto_offset_reset
        self._auto_commit_enable = auto_commit_enable
        self._assignment_strategy = assignment_strategy
        self._partitioner = partitioner
        self._consumer_extra_config = consumer_extra_config
        self._producer_extra_config = producer_extra_config
        self._consumer_poll_timeout = consumer_poll_timeout
        self._producer_poll_timeout = producer_poll_timeout
        self._running = False
        self._on_consumer_error = on_consumer_error
        self._on_producer_error = on_producer_error
        self._on_processing_error = on_processing_error or default_on_processing_error
        self._on_message_processed = on_message_processed

    def stop(self):
        self._running = False

    def run(
        self,
        dataframe: StreamingDataFrame,
    ):
        """
        Start processing data from Kafka using provided StreamingDataFrame

        :param dataframe: StreamingDataFrame instance
        """
        logger.info("Start processing of the streaming dataframe")
        self._running = True
        # Initialize consumer and producer instances
        consumer = RowConsumer(
            broker_address=self._broker_address,
            consumer_group=self._consumer_group,
            auto_offset_reset=self._auto_offset_reset,
            auto_commit_enable=self._auto_commit_enable,
            assignment_strategy=self._assignment_strategy,
            extra_config=self._consumer_extra_config,
            on_error=self._on_consumer_error,
        )
        producer = RowProducer(
            broker_address=self._broker_address,
            partitioner=self._partitioner,
            extra_config=self._producer_extra_config,
            on_error=self._on_producer_error,
        )

        with producer, consumer:
            # Provide Consumer and Producer instances to StreamingDataFrame
            dataframe.producer = producer
            dataframe.consumer = consumer

            # Get a list of input topics for this Dataframe
            topics = list(dataframe.topics.values())
            # Subscribe to topics in Kafka and start polling
            consumer.subscribe(topics)
            # Start polling Kafka for messages and callbacks
            while self._running:
                # Serve producer callbacks
                producer.poll(self._producer_poll_timeout)
                rows = consumer.poll_row(timeout=self._consumer_poll_timeout)

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
                consumer.store_offsets(
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
