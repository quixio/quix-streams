import contextlib
import logging
import signal
from typing import Optional, List, Callable

from confluent_kafka import TopicPartition
from typing_extensions import Self

from .context import set_message_context, copy_context
from .core.stream import Filtered
from .dataframe import StreamingDataFrame
from .error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
    default_on_processing_error,
)
from .kafka import AutoOffsetReset, AssignmentStrategy, Partitioner, Producer, Consumer
from .logging import configure_logging, LogLevel
from .models import (
    Topic,
    SerializerType,
    DeserializerType,
    TimestampExtractor,
)
from .platforms.quix import (
    QuixKafkaConfigsBuilder,
    TopicCreationConfigs,
    check_state_dir,
    check_state_management_enabled,
)
from .rowconsumer import RowConsumer
from .rowproducer import RowProducer
from .state import StateStoreManager
from .state.rocksdb import RocksDBOptionsType

__all__ = ("Application",)

logger = logging.getLogger(__name__)
MessageProcessedCallback = Callable[[str, int, int], None]


class Application:
    """
    The main Application class.

    Typically, the primary object needed to get a kafka application up and running.

    Most functionality is explained the various methods, except for
    "column assignment".


    What it Does:

    - During user setup:
        - Provides defaults or helper methods for commonly needed objects
        - For Quix Platform Users: Configures the app for it
            (see `Application.Quix()`)
    - When executed via `.run()` (after setup):
        - Initializes Topics and StreamingDataFrames
        - Facilitates processing of Kafka messages with a `StreamingDataFrame`
        - Handles all Kafka client consumer/producer responsibilities.


    Example Snippet:

    ```python
    from quixstreams import Application

    # Set up an `app = Application` and  `sdf = StreamingDataFrame`;
    # add some operations to `sdf` and then run everything.

    app = Application(broker_address='localhost:9092', consumer_group='group')
    topic = app.topic('test-topic')
    df = app.dataframe(topic)
    df.apply(lambda value, context: print('New message', value)

    app.run(dataframe=df)
    ```
    """

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
        state_dir: str = "state",
        rocksdb_options: Optional[RocksDBOptionsType] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
        loglevel: Optional[LogLevel] = "INFO",
    ):
        """

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
        :param state_dir: path to the application state directory.
            Default - ".state".
        :param rocksdb_options: RocksDB options.
            If `None`, the default options will be used.
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`. Default - 1.0s
        :param producer_poll_timeout: timeout for `RowProducer.poll()`. Default - 0s.
        :param on_message_processed: a callback triggered when message is successfully
            processed.
        :param loglevel: a log level for "quixstreams" logger.
            Should be a string or None.
            If `None` is passed, no logging will be configured.
            You may pass `None` and configure "quixstreams" logger
            externally using `logging` library.
            Default - "INFO".

        ***Error Handlers***

        To handle errors, `Application` accepts callbacks triggered when
            exceptions occur on different stages of stream processing. If the callback
            returns `True`, the exception will be ignored. Otherwise, the exception
            will be propagated and the processing will eventually stop.
        :param on_consumer_error: triggered when internal `RowConsumer` fails
        to poll Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when RowProducer fails to serialize
            or to produce a message to Kafka.
        """
        configure_logging(loglevel=loglevel)
        self._broker_address = broker_address
        self._consumer_group = consumer_group
        self._auto_offset_reset = auto_offset_reset
        self._auto_commit_enable = auto_commit_enable
        self._assignment_strategy = assignment_strategy
        self._partitioner = partitioner
        self._producer_extra_config = producer_extra_config
        self._consumer_extra_config = consumer_extra_config

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
        self._state_manager = StateStoreManager(
            group_id=consumer_group,
            state_dir=state_dir,
            rocksdb_options=rocksdb_options,
        )

    def _set_quix_config_builder(self, config_builder: QuixKafkaConfigsBuilder):
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
        state_dir: str = "state",
        rocksdb_options: Optional[RocksDBOptionsType] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
        loglevel: Optional[LogLevel] = "INFO",
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
        auto_create_topics: bool = True,
    ) -> Self:
        """
        Initialize an Application to work with Quix platform,
        assuming environment is properly configured (by default in the platform).

        It takes the credentials from the environment and configures consumer and
        producer to properly connect to the Quix platform.

        >***NOTE:*** Quix platform requires `consumer_group` and topic names to be
            prefixed with workspace id.
            If the application is created via `Application.Quix()`, the real consumer
            group will be `<workspace_id>-<consumer_group>`,
            and the real topic names will be `<workspace_id>-<topic_name>`.



        Example Snippet:

        ```python
        from quixstreams import Application

        # Set up an `app = Application.Quix` and  `sdf = StreamingDataFrame`;
        # add some operations to `sdf` and then run everything. Also shows off how to
        # use the quix-specific serializers and deserializers.

        app = Application.Quix()
        input_topic = app.topic("topic-in", value_deserializer="quix")
        output_topic = app.topic("topic-out", value_serializer="quix_timeseries")
        df = app.dataframe(topic_in)
        df = df.to_topic(output_topic)

        app.run(dataframe=df)
        ```

        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`.
              >***NOTE:*** The consumer group will be prefixed by Quix workspace id.
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
        :param state_dir: path to the application state directory.
            Default - ".state".
        :param rocksdb_options: RocksDB options.
            If `None`, the default options will be used.
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`. Default - 1.0s
        :param producer_poll_timeout: timeout for `RowProducer.poll()`. Default - 0s.
        :param on_message_processed: a callback triggered when message is successfully
            processed.
        :param loglevel: a log level for "quixstreams" logger.
            Should be a string or None.
            If `None` is passed, no logging will be configured.
            You may pass `None` and configure "quixstreams" logger
            externally using `logging` library.
            Default - "INFO".

        ***Error Handlers***

        To handle errors, `Application` accepts callbacks triggered when
            exceptions occur on different stages of stream processing. If the callback
            returns `True`, the exception will be ignored. Otherwise, the exception
            will be propagated and the processing will eventually stop.
        :param on_consumer_error: triggered when internal `RowConsumer` fails to poll
            Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when RowProducer fails to serialize
            or to produce a message to Kafka.


        ***Quix-specific Parameters***

        :param quix_config_builder: instance of `QuixKafkaConfigsBuilder` to be used
            instead of the default one.
        :param auto_create_topics: Whether to auto-create any topics handed to a
            StreamingDataFrame instance (topics_in + topics_out).

        :return: `Application` object
        """
        configure_logging(loglevel=loglevel)
        quix_config_builder = quix_config_builder or QuixKafkaConfigsBuilder()
        quix_config_builder.app_auto_create_topics = auto_create_topics
        quix_configs = quix_config_builder.get_confluent_broker_config()

        # Check if the state dir points to the mounted PVC while running on Quix
        # Otherwise, the state won't be shared and replicas won't be able to
        # recover the same state.
        check_state_dir(state_dir=state_dir)

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
        app._set_quix_config_builder(quix_config_builder)
        return app

    @property
    def is_quix_app(self) -> bool:
        return self._quix_config_builder is not None

    def topic(
        self,
        name: str,
        value_deserializer: DeserializerType = "json",
        key_deserializer: DeserializerType = "bytes",
        value_serializer: SerializerType = "json",
        key_serializer: SerializerType = "bytes",
        creation_configs: Optional[TopicCreationConfigs] = None,
        timestamp_extractor: Optional[TimestampExtractor] = None,
    ) -> Topic:
        """
        Create a topic definition.

        Allows you to specify serialization that should be used when consuming/producing
        to the topic in the form of a string name (i.e. "json" for JSON) or a
        serialization class instance directly, like JSONSerializer().


        Example Snippet:

        ```python
        from quixstreams import Application

        # Specify an input and output topic for a `StreamingDataFrame` instance,
        # where the output topic requires adjusting the key serializer.

        app = Application()
        input_topic = app.topic("input-topic", value_deserializer="json")
        output_topic = app.topic(
            "output-topic", key_serializer="str", value_serializer=JSONSerializer()
        )
        sdf = app.dataframe(input_topic)
        sdf.to_topic(output_topic)
        ```

        :param name: topic name
            >***NOTE:*** If the application is created via `Quix.Application()`,
              the topic name will be prefixed by Quix workspace id, and it will
              be `<workspace_id>-<name>`
        :param value_deserializer: a deserializer type for values; default="json"
        :param key_deserializer: a deserializer type for keys; default="bytes"
        :param value_serializer: a serializer type for values; default="json"
        :param key_serializer: a serializer type for keys; default="bytes"
        :param creation_configs: settings for auto topic creation (Quix platform only)
            Its name will be overridden by this method's 'name' param.

        :param timestamp_extractor: a callable that returns a timestamp in
            milliseconds from a deserialized message. Default - `None`.

        Example Snippet:

        ```python
        app = Application(...)


        def custom_ts_extractor(
            value: Any,
            headers: Optional[List[Tuple[str, bytes]]],
            timestamp: float,
            timestamp_type: TimestampType,
        ) -> int:
            return value["timestamp"]

        topic = app.topic("input-topic", timestamp_extractor=custom_ts_extractor)
        ```


        :return: `Topic` object
        """

        if self.is_quix_app:
            name = self._quix_config_builder.append_workspace_id(name)
            if creation_configs:
                creation_configs.name = name
            else:
                creation_configs = TopicCreationConfigs(name=name)
            self._quix_config_builder.create_topic_configs[name] = creation_configs
        return Topic(
            name=name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            timestamp_extractor=timestamp_extractor,
        )

    def dataframe(
        self,
        topic: Topic,
    ) -> StreamingDataFrame:
        """
        A simple helper method that generates a `StreamingDataFrame`, which is used
        to define your message processing pipeline.

        See :class:`quixstreams.dataframe.StreamingDataFrame` for more details.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Set up an `app = Application` and  `sdf = StreamingDataFrame`;
        # add some operations to `sdf` and then run everything.

        app = Application(broker_address='localhost:9092', consumer_group='group')
        topic = app.topic('test-topic')
        df = app.dataframe(topic)
        df.apply(lambda value, context: print('New message', value)

        app.run(dataframe=df)
        ```


        :param topic: a `quixstreams.models.Topic` instance
            to be used as an input topic.
        :return: `StreamingDataFrame` object
        """
        sdf = StreamingDataFrame(topic=topic, state_manager=self._state_manager)
        sdf.producer = self._producer
        return sdf

    def stop(self):
        """
        Stop the internal poll loop and the message processing.

        Only necessary when manually managing the lifecycle of the `Application` (
        likely through some sort of threading).

        To otherwise stop an application, either send a `SIGTERM` to the process
        (like Kubernetes does) or perform a typical `KeyboardInterrupt` (`Ctrl+C`).
        """
        self._running = False

    def get_producer(self) -> Producer:
        """
        Create and return a pre-configured Producer instance.
        The Producer is initialized with params passed to Application.

        It's useful for producing data to Kafka outside the standard Application processing flow,
        (e.g. to produce test data into a topic).
        Using this within the StreamingDataFrame functions is not recommended, as it creates a new Producer
        instance each time, which is not optimized for repeated use in a streaming pipeline.

        Example Snippet:

        ```python
        from quixstreams import Application

        app = Application.Quix(...)
        topic = app.topic("input")

        with app.get_producer() as producer:
            for i in range(100):
                producer.produce(topic=topic.name, key=b"key", value=b"value")
        ```
        """
        if self.is_quix_app:
            topics = self._quix_config_builder.create_topic_configs.values()
            if self._quix_config_builder.app_auto_create_topics:
                self._quix_config_builder.create_topics(topics)
            else:
                self._quix_config_builder.confirm_topics_exist(topics)

        return Producer(
            broker_address=self._broker_address,
            partitioner=self._partitioner,
            extra_config=self._producer_extra_config,
        )

    def get_consumer(self) -> Consumer:
        """
        Create and return a pre-configured Consumer instance.
        The Consumer is initialized with params passed to Application.

        It's useful for consuming data from Kafka outside the standard Application processing flow.
        (e.g. to consume test data from a topic).
        Using it within the StreamingDataFrame functions is not recommended, as it creates a new Consumer instance
        each time, which is not optimized for repeated use in a streaming pipeline.

        Note: By default this consumer does not autocommit consumed offsets to allow exactly-once processing.
        To store the offset call store_offsets() after processing a message.
        If autocommit is necessary set `enable.auto.offset.store` to True in the consumer config when creating the app.

        Example Snippet:

        ```python
        from quixstreams import Application

        app = Application.Quix(...)
        topic = app.topic("input")

        with app.get_consumer() as consumer:
            consumer.subscribe([topic.name])
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is not None:
                    # Process message
                    # Optionally commit the offset
                    # consumer.store_offsets(msg)

        ```
        """
        if self.is_quix_app:
            topics = self._quix_config_builder.create_topic_configs.values()
            if self._quix_config_builder.app_auto_create_topics:
                self._quix_config_builder.create_topics(topics)
            else:
                self._quix_config_builder.confirm_topics_exist(topics)

        return Consumer(
            broker_address=self._broker_address,
            consumer_group=self._consumer_group,
            auto_offset_reset=self._auto_offset_reset,
            auto_commit_enable=self._auto_commit_enable,
            assignment_strategy=self._assignment_strategy,
            extra_config=self._consumer_extra_config,
        )

    def clear_state(self):
        """
        Clear the state of the application.
        """
        self._state_manager.clear_stores()

    def _quix_runtime_init(self):
        """
        Do a runtime setup only applicable to an Application.Quix instance

        In particular:
        - Create topics in Quix if `auto_create_topics` is True and ensure that all
          necessary topics are created
        - Ensure that "State management" flag is enabled for deployment if the app
          is stateful and is running on Quix platform
        """

        logger.debug("Ensure that all topics are present in Quix")
        topics = self._quix_config_builder.create_topic_configs.values()
        if self._quix_config_builder.app_auto_create_topics:
            self._quix_config_builder.create_topics(topics)
        else:
            self._quix_config_builder.confirm_topics_exist(topics)

        # Ensure that state management is enabled if application is stateful
        # and is running on Quix platform
        if self._state_manager.stores:
            check_state_management_enabled()

    def run(
        self,
        dataframe: StreamingDataFrame,
    ):
        """
        Start processing data from Kafka using provided `StreamingDataFrame`

        One started, can be safely terminated with a `SIGTERM` signal
        (like Kubernetes does) or a typical `KeyboardInterrupt` (`Ctrl+C`).


        Example Snippet:

        ```python
        from quixstreams import Application

        # Set up an `app = Application` and  `sdf = StreamingDataFrame`;
        # add some operations to `sdf` and then run everything.

        app = Application(broker_address='localhost:9092', consumer_group='group')
        topic = app.topic('test-topic')
        df = app.dataframe(topic)
        df.apply(lambda value, context: print('New message', value)

        app.run(dataframe=df)
        ```

        :param dataframe: instance of `StreamingDataFrame`
        """
        self._setup_signal_handlers()

        logger.info("Initializing processing of StreamingDataFrame")
        if self.is_quix_app:
            self._quix_runtime_init()

        exit_stack = contextlib.ExitStack()
        exit_stack.enter_context(self._producer)
        exit_stack.enter_context(self._consumer)
        exit_stack.enter_context(self._state_manager)

        exit_stack.callback(
            lambda *_: logger.debug("Closing Kafka consumers & producers")
        )
        exit_stack.callback(lambda *_: self.stop())

        if self._state_manager.stores:
            # Store manager has stores registered, use real state transactions
            # during processing
            start_state_transaction = self._state_manager.start_store_transaction
        else:
            # Application is stateless, use dummy state transactionss
            start_state_transaction = _dummy_state_transaction

        with exit_stack:
            # Subscribe to topics in Kafka and start polling
            self._consumer.subscribe(
                [dataframe.topic],
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
                on_lost=self._on_lost,
            )
            logger.info("Waiting for incoming messages")
            # Start polling Kafka for messages and callbacks
            self._running = True

            dataframe_composed = dataframe.compose()
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

                with start_state_transaction(
                    topic=topic_name, partition=partition, offset=offset
                ):
                    for row in rows:
                        context = copy_context()
                        context.run(set_message_context, row.context)
                        try:
                            # Execute StreamingDataFrame in a context
                            context.run(dataframe_composed, row.value)
                        except Filtered:
                            # The message was filtered by StreamingDataFrame
                            continue
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

            logger.info("Stop processing of StreamingDataFrame")

    def _on_assign(self, _, topic_partitions: List[TopicPartition]):
        """
        Assign new topic partitions to consumer and state.

        :param topic_partitions: list of `TopicPartition` from Kafka
        """
        if self._state_manager.stores:
            logger.debug(f"Rebalancing: assigning state store partitions")
            for tp in topic_partitions:
                # Assign store partitions
                store_partitions = self._state_manager.on_partition_assign(tp)

                # Check if the latest committed offset >= stored offset
                # Otherwise, the re-processed messages might use already updated
                # state, which can lead to inconsistent outputs
                stored_offsets = [
                    offset
                    for offset in (
                        store_tp.get_processed_offset() for store_tp in store_partitions
                    )
                    if offset is not None
                ]
                min_stored_offset = min(stored_offsets) + 1 if stored_offsets else None
                if min_stored_offset is not None:
                    tp_committed = self._consumer.committed([tp], timeout=30)[0]
                    if min_stored_offset > tp_committed.offset:
                        logger.warning(
                            f'Warning: offset "{tp_committed.offset}" '
                            f"for topic partition "
                            f'"{tp_committed.topic}[{tp_committed.partition}]" '
                            f'is behind the stored offset "{min_stored_offset}". '
                            f"It may lead to distortions in produced data."
                        )

    def _on_revoke(self, _, topic_partitions: List[TopicPartition]):
        """
        Revoke partitions from consumer and state
        """
        if self._state_manager.stores:
            logger.debug(f"Rebalancing: revoking state store partitions")
            for tp in topic_partitions:
                self._state_manager.on_partition_revoke(tp)

    def _on_lost(self, _, topic_partitions: List[TopicPartition]):
        """
        Dropping lost partitions from consumer and state
        """
        if self._state_manager.stores:
            logger.debug(f"Rebalancing: dropping lost state store partitions")
            for tp in topic_partitions:
                self._state_manager.on_partition_lost(tp)

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


_nullcontext = contextlib.nullcontext()


def _dummy_state_transaction(topic: str, partition: int, offset: int):
    return _nullcontext
