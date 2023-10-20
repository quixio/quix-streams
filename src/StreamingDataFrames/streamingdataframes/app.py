import contextlib
import logging
from itertools import chain

from confluent_kafka import TopicPartition
from typing import Optional, List, Callable
from typing_extensions import Self

from .dataframe import StreamingDataFrame
from .error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
    default_on_processing_error,
)
from .kafka import AutoOffsetReset, AssignmentStrategy, Partitioner
from .models import (
    Topic,
    TopicCreationConfigs,
    Deserializer,
    BytesDeserializer,
    Serializer,
    BytesSerializer,
)
from .platforms.quix import QuixKafkaConfigsBuilder
from .rowconsumer import RowConsumer
from .rowproducer import RowProducer
from .state import StateStoreManager
from .state.rocksdb import RocksDBOptionsType

__all__ = ("Application",)


logger = logging.getLogger(__name__)
MessageProcessedCallback = Callable[[str, int, int], None]


class Application:
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
        state_dir: Optional[str] = None,
        rocksdb_options: Optional[RocksDBOptionsType] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
    ):
        """
        The main Application class.

        What it does:
            - Initializes Topics and StreamingDataFrames
            - Facilitates the execution of StreamingDataFrames
            - Configures the app to work with Quix platform (if necessary)

        Example usage:
        ```
            from streamingdataframes import Application

            app = Application(broker_address='localhost:9092', consumer_group='group')
            topic = app.topic('test-topic')
            df = app.dataframe([topic)

            app.run(dataframe=df)
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
        :param state_dir: path to the application state directory, optional.
            It should be passed if the application uses stateful operations, otherwise
            the exception will be raised.
        :param rocksdb_options: RocksDB options.
            If `None`, the default options will be used.
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`. Default - 1.0s
        :param producer_poll_timeout: timeout for `RowProducer.poll()`. Default - 0s.
        :param on_message_processed: a callback triggered when message is successfully
            processed.

        To handle errors, `Application` accepts callbacks triggered when exceptions
        occur on different stages of stream processing.
        If the callback returns `True`, the exception will be ignored. Otherwise, the
        exception will be propagated and the processing will eventually stop.

        :param on_consumer_error: triggered when internal `RowConsumer` fails
        to poll Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when RowProducer fails to serialize
            or to produce a message to Kafka.

        """
        self._consumer = RowConsumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            assignment_strategy=assignment_strategy,
            extra_config=consumer_extra_config,
            on_error=on_consumer_error,
        )
        self._producer = RowProducer(
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
        self._quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None
        self._state_manager: Optional[StateStoreManager] = None
        if state_dir:
            self._state_manager = StateStoreManager(
                group_id=consumer_group,
                state_dir=state_dir,
                rocksdb_options=rocksdb_options,
            )

    def set_quix_config_builder(self, config_builder: QuixKafkaConfigsBuilder):
        self._quix_config_builder = config_builder

    @classmethod
    def Quix(
        cls,
        consumer_group: str,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        assignment_strategy: AssignmentStrategy = "range",
        partitioner: Partitioner = "murmur2",
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        state_dir: Optional[str] = None,
        rocksdb_options: Optional[RocksDBOptionsType] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
        auto_create_topics: bool = True,
    ) -> Self:
        """
        Initialize an Application to work with Quix platform,
        assuming environment is properly configured (by default in the platform).

        It takes the credentials from the environment and configures consumer and
        producer to properly connect to the Quix platform.

        .. note:: Quix platform requires `consumer_group` and topic names to be prefixed
            with workspace id.
            If the application is created via `Application.Quix()`, the real consumer
            group will be `<workspace_id>-<consumer_group>`,
            and the real topic names will be `<workspace_id>-<topic_name>`.

        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`.
            .. note:: The consumer group will be prefixed by Quix workspace id.

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
        :param state_dir: path to the application state directory, optional.
            It should be passed if the application uses stateful operations, otherwise
            the exception will be raised.
        :param rocksdb_options: RocksDB options.
            If `None`, the default options will be used.
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`. Default - 1.0s
        :param producer_poll_timeout: timeout for `RowProducer.poll()`. Default - 0s.
        :param on_message_processed: a callback triggered when message is successfully
            processed.

        To handle errors, `Application` accepts callbacks triggered when exceptions
        occur on different stages of stream processing.
        If the callback returns `True`, the exception will be ignored. Otherwise, the
        exception will be propagated and the processing will eventually stop.

        :param on_consumer_error: triggered when internal `RowConsumer` fails
        to poll Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when RowProducer fails to serialize
            or to produce a message to Kafka.

        :param quix_config_builder: instance of `QuixKafkaConfigsBuilder` to be used
            instead of the default one.
        :param auto_create_topics: Whether to auto-create any topics handed to a
            StreamingDataFrame instance (topics_in + topics_out).

        :return: `Application` object
        """
        quix_config_builder = quix_config_builder or QuixKafkaConfigsBuilder()
        quix_config_builder.app_auto_create_topics = auto_create_topics
        quix_configs = quix_config_builder.get_confluent_broker_config()
        broker_address = quix_configs.pop("bootstrap.servers")
        # Quix platform prefixes consumer group with workspace id
        consumer_group = quix_config_builder.append_workspace_id(consumer_group)
        consumer_extra_config = {**quix_configs, **(consumer_extra_config or {})}
        producer_extra_config = {**quix_configs, **(producer_extra_config or {})}
        app = cls(
            broker_address=broker_address,
            consumer_group=consumer_group,
            consumer_extra_config=consumer_extra_config,
            producer_extra_config=producer_extra_config,
            auto_offset_reset=auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            assignment_strategy=assignment_strategy,
            partitioner=partitioner,
            on_consumer_error=on_consumer_error,
            on_processing_error=on_processing_error,
            on_producer_error=on_producer_error,
            on_message_processed=on_message_processed,
            consumer_poll_timeout=consumer_poll_timeout,
            producer_poll_timeout=producer_poll_timeout,
            state_dir=state_dir,
            rocksdb_options=rocksdb_options,
        )
        # Inject Quix config builder to use it in other methods
        app.set_quix_config_builder(quix_config_builder)
        return app

    @property
    def is_quix_app(self):
        return self._quix_config_builder is not None

    def topic(
        self,
        name: str,
        value_deserializer: Optional[Deserializer] = None,
        key_deserializer: Optional[Deserializer] = BytesDeserializer(),
        value_serializer: Optional[Serializer] = None,
        key_serializer: Optional[Serializer] = BytesSerializer(),
        creation_configs: Optional[TopicCreationConfigs] = None,
    ) -> Topic:
        """
        Create a topic definition.

        :param name: topic name
            .. note:: If the application is created via `Quix.Application()`,
              the topic name will be prefixed by Quix workspace id, and it will
              be `<workspace_id>-<name>`
        :param value_deserializer: a deserializer for values
        :param key_deserializer: a deserializer for keys
        :param value_serializer: a serializer for values
        :param key_serializer: a serializer for keys
        :param creation_configs: settings for topic creation, if needed
        :return: `Topic` object
        """
        if self.is_quix_app:
            # This Application object was created via `.Quix()` method.
            # Quix platform's workspace id is used as topic prefix.
            name = self._quix_config_builder.append_workspace_id(name)
            if not creation_configs:
                creation_configs = TopicCreationConfigs(
                    num_partitions=2, replication_factor=2
                )
        return Topic(
            name=name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            creation_configs=creation_configs,
        )

    def dataframe(
        self,
        topics_in: List[Topic],
    ) -> StreamingDataFrame:
        """
        Create a StreamingDataFrame to define message processing pipeline.

        See :class:`streamingdataframes.dataframe.StreamingDataFrame` for more details

        :param topics_in: a list of :class:`streamingdataframes.models.Topic`
            to be used as input topics.
        :return: `StreamingDataFrame` object
        """
        sdf = StreamingDataFrame(topics_in=topics_in)
        sdf.consumer = self._consumer
        sdf.producer = self._producer
        return sdf

    def stop(self):
        """
        Stop the internal poll loop and the message processing.
        """
        self._running = False

    @property
    def is_stateful(self) -> bool:
        return bool(self._state_manager and self._state_manager.stores)

    def _quix_runtime_init(self, dataframe: StreamingDataFrame):
        """
        Do some runtime setup only applicable to an Application.Quix instance

        :param dataframe: instance of `StreamingDataFrame`
        """
        if self.is_quix_app:
            logger.debug("Performing Quix-Specific runtime setup...")
            topics = chain(dataframe.topics_in.values(), dataframe.topics_out.values())
            if self._quix_config_builder.app_auto_create_topics:
                self._quix_config_builder.create_topics(topics)
            else:
                self._quix_config_builder.confirm_topics_exist(topics)

    def run(
        self,
        dataframe: StreamingDataFrame,
    ):
        """
        Start processing data from Kafka using provided `StreamingDataFrame`

        :param dataframe: instance of `StreamingDataFrame`
        """
        logger.info("Start processing of the streaming dataframe")

        self._quix_runtime_init(dataframe)

        exit_stack = contextlib.ExitStack()
        exit_stack.enter_context(self._producer)
        exit_stack.enter_context(self._consumer)
        if self.is_stateful:
            exit_stack.enter_context(self._state_manager)

        exit_stack.callback(
            lambda *_: logger.debug("Closing Kafka consumers & producers")
        )
        exit_stack.callback(lambda *_: self.stop())

        with exit_stack:
            logger.info("Start processing of the streaming dataframe")

            # Subscribe to topics in Kafka and start polling
            self._consumer.subscribe(
                list(dataframe.topics_in.values()),
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
                on_lost=self._on_lost,
            )
            # Start polling Kafka for messages and callbacks
            self._running = True
            while self._running:
                # Serve producer callbacks
                self._producer.poll(self._producer_poll_timeout)
                rows = self._consumer.poll_row(timeout=self._consumer_poll_timeout)

                if rows is None:
                    continue

                # Deserializer may return multiple rows for a single message
                rows = rows if isinstance(rows, list) else [rows]
                if not rows:
                    continue

                first_row = rows[0]
                topic_name, partition, offset = (
                    first_row.topic,
                    first_row.partition,
                    first_row.offset,
                )

                if self.is_stateful:
                    # Store manager has stores registered, starting a transaction
                    state_transaction = self._state_manager.start_store_transaction(
                        topic=topic_name, partition=partition, offset=offset
                    )
                else:
                    # The application is stateless, use noop transaction
                    state_transaction = contextlib.nullcontext()

                with state_transaction:
                    for row in rows:
                        try:
                            dataframe.process(row=row)
                        except Exception as exc:
                            # TODO: This callback might be triggered because of Producer
                            #  errors too because they happen within ".process()"
                            to_suppress = self._on_processing_error(exc, row, logger)
                            if not to_suppress:
                                raise

                # Store the message offset after it's successfully processed
                self._consumer.store_offsets(
                    offsets=[
                        TopicPartition(
                            topic=topic_name,
                            partition=partition,
                            offset=offset + 1,
                        )
                    ]
                )

                if self._on_message_processed is not None:
                    self._on_message_processed(topic_name, partition, offset)

            logger.info("Stop processing of the streaming dataframe")

    def _on_assign(self, _, topic_partitions: List[TopicPartition]):
        """
        Assign new topic partitions to consumer and state.

        :param topic_partitions: list of `TopicPartition` from Kafka
        """
        if self.is_stateful:
            logger.info(f"Rebalancing: assigning state store partitions")
            for tp in topic_partitions:
                self._state_manager.on_partition_assign(tp)

    def _on_revoke(self, _, topic_partitions: List[TopicPartition]):
        """
        Revoke partitions from consumer and state
        """
        if self.is_stateful:
            logger.info(f"Rebalancing: revoking state store partitions")
            for tp in topic_partitions:
                self._state_manager.on_partition_revoke(tp)

    def _on_lost(self, _, topic_partitions: List[TopicPartition]):
        """
        Dropping lost partitions from consumer and state
        """
        if self.is_stateful:
            logger.info(f"Rebalancing: dropping lost state store partitions")
            for tp in topic_partitions:
                self._state_manager.on_partition_lost(tp)
