import contextlib
import functools
import logging
import os
import signal
import time
import warnings
from pathlib import Path
from typing import Callable, List, Literal, Optional, Tuple, Type, Union

from confluent_kafka import TopicPartition
from pydantic import AliasGenerator, Field
from pydantic_settings import PydanticBaseSettingsSource, SettingsConfigDict
from typing_extensions import Self

from .context import copy_context, set_message_context
from .dataframe import DataframeRegistry, StreamingDataFrame
from .error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
    default_on_processing_error,
)
from .kafka import AutoOffsetReset, ConnectionConfig, Consumer, Producer
from .logging import LogLevel, configure_logging
from .models import (
    DeserializerType,
    SerializerType,
    TimestampExtractor,
    Topic,
    TopicAdmin,
    TopicConfig,
    TopicManager,
)
from .platforms.quix import (
    QuixKafkaConfigsBuilder,
    QuixTopicManager,
    check_state_dir,
    check_state_management_enabled,
)
from .processing import PausingManager, ProcessingContext
from .rowconsumer import RowConsumer
from .rowproducer import RowProducer
from .sinks import SinkManager
from .sources import BaseSource, SourceException, SourceManager
from .state import StateStoreManager
from .state.recovery import RecoveryManager
from .state.rocksdb import RocksDBOptionsType
from .utils.settings import BaseSettings

__all__ = ("Application", "ApplicationConfig")

logger = logging.getLogger(__name__)
ProcessingGuarantee = Literal["at-least-once", "exactly-once"]
MessageProcessedCallback = Callable[[str, int, int], None]

# Enforce idempotent producing for the internal RowProducer
_default_producer_extra_config = {"enable.idempotence": True}

_default_max_poll_interval_ms = 300000


class Application:
    """
    The main Application class.

    Typically, the primary object needed to get a kafka application up and running.

    Most functionality is explained the various methods, except for
    "column assignment".


    What it Does:

    - On init:
        - Provides defaults or helper methods for commonly needed objects
        - If `quix_sdk_token` is passed, configures the app to use the Quix Cloud.
    - When executed via `.run()` (after setup):
        - Initializes Topics and StreamingDataFrames
        - Facilitates processing of Kafka messages with a `StreamingDataFrame`
        - Handles all Kafka client consumer/producer responsibilities.


    Example Snippet:

    ```python
    from quixstreams import Application

    # Set up an `app = Application` and `sdf = StreamingDataFrame`;
    # add some operations to `sdf` and then run everything.

    app = Application(broker_address='localhost:9092', consumer_group='group')
    topic = app.topic('test-topic')
    df = app.dataframe(topic)
    df.apply(lambda value, context: print('New message', value))

    app.run()
    ```
    """

    def __init__(
        self,
        broker_address: Optional[Union[str, ConnectionConfig]] = None,
        *,
        quix_sdk_token: Optional[str] = None,
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        commit_interval: float = 5.0,
        commit_every: int = 0,
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        state_dir: Union[str, Path] = Path("state"),
        rocksdb_options: Optional[RocksDBOptionsType] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
        loglevel: Optional[Union[int, LogLevel]] = "INFO",
        auto_create_topics: bool = True,
        use_changelog_topics: bool = True,
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
        topic_manager: Optional[TopicManager] = None,
        request_timeout: float = 30,
        topic_create_timeout: float = 60,
        processing_guarantee: ProcessingGuarantee = "at-least-once",
    ):
        """
        :param broker_address: Connection settings for Kafka.
            Used by Producer, Consumer, and Admin clients.
            Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
            or a ConnectionConfig object if authentication is required.
            Either this OR `quix_sdk_token` must be set to use `Application` (not both).
            Takes priority over quix auto-configuration.
            Linked Environment Variable: `Quix__Broker__Address`.
            Default: `None`
        :param quix_sdk_token: If using the Quix Cloud, the SDK token to connect with.
            Either this OR `broker_address` must be set to use Application (not both).
            Linked Environment Variable: `Quix__Sdk__Token`.
            Default: None (if not run on Quix Cloud)
              >***NOTE:*** the environment variable is set for you in the Quix Cloud
        :param consumer_group: Kafka consumer group.
            Passed as `group.id` to `confluent_kafka.Consumer`.
            Linked Environment Variable: `Quix__Consumer__Group`.
            Default - "quixstreams-default" (set during init)
              >***NOTE:*** Quix Applications will prefix it with the Quix workspace id.
        :param commit_interval: How often to commit the processed messages in seconds.
            Default - 5.0.
        :param commit_every: Commit the checkpoint after processing N messages.
            Use this parameter for more granular control of the commit schedule.
            If the value is > 0, the application will commit the checkpoint after
            processing the specified number of messages across all the assigned
            partitions.
            If the value is <= 0, only the `commit_interval` will be considered.
            Default - 0.
                >***NOTE:*** Only input offsets are counted, and the application
                > may produce more results than the number of incoming messages.
        :param auto_offset_reset: Consumer `auto.offset.reset` setting
        :param consumer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Consumer` as is.
        :param producer_extra_config: A dictionary with additional options that
            will be passed to `confluent_kafka.Producer` as is.
        :param state_dir: path to the application state directory.
            Default - `"state"`.
        :param rocksdb_options: RocksDB options.
            If `None`, the default options will be used.
        :param consumer_poll_timeout: timeout for `RowConsumer.poll()`. Default - `1.0`s
        :param producer_poll_timeout: timeout for `RowProducer.poll()`. Default - `0`s.
        :param on_message_processed: a callback triggered when message is successfully
            processed.
        :param loglevel: a log level for "quixstreams" logger.
            Should be a string or None.
            If `None` is passed, no logging will be configured.
            You may pass `None` and configure "quixstreams" logger
            externally using `logging` library.
            Default - `"INFO"`.
        :param auto_create_topics: Create all `Topic`s made via Application.topic()
            Default - `True`
        :param use_changelog_topics: Use changelog topics to back stateful operations
            Default - `True`
        :param topic_manager: A `TopicManager` instance
        :param request_timeout: timeout (seconds) for REST-based requests
        :param topic_create_timeout: timeout (seconds) for topic create finalization
        :param processing_guarantee: Use "exactly-once" or "at-least-once" processing.

        <br><br>***Error Handlers***<br>
        To handle errors, `Application` accepts callbacks triggered when
            exceptions occur on different stages of stream processing. If the callback
            returns `True`, the exception will be ignored. Otherwise, the exception
            will be propagated and the processing will eventually stop.
        :param on_consumer_error: triggered when internal `RowConsumer` fails
            to poll Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when `RowProducer` fails to serialize
            or to produce a message to Kafka.
        <br><br>***Quix Cloud Parameters***<br>
        :param quix_config_builder: instance of `QuixKafkaConfigsBuilder` to be used
            instead of the default one.
            > NOTE: It is recommended to just use `quix_sdk_token` instead.
        """
        configure_logging(loglevel=loglevel)

        producer_extra_config = producer_extra_config or {}
        consumer_extra_config = consumer_extra_config or {}

        # We can't use os.getenv as defaults (and have testing work nicely)
        # since it evaluates getenv when the function is defined.
        # In general this is just a most robust approach.
        broker_address = broker_address or os.getenv("Quix__Broker__Address")
        quix_sdk_token = quix_sdk_token or os.getenv("Quix__Sdk__Token")
        consumer_group = consumer_group or os.getenv(
            "Quix__Consumer_Group", "quixstreams-default"
        )

        if broker_address:
            # If broker_address is passed to the app it takes priority over any quix configuration
            self._is_quix_app = False
            topic_manager_factory = TopicManager
            if isinstance(broker_address, str):
                broker_address = ConnectionConfig(bootstrap_servers=broker_address)
        else:
            self._is_quix_app = True

            if quix_config_builder:
                quix_app_source = "Quix Config Builder"
                if quix_sdk_token:
                    warnings.warn(
                        "'quix_config_builder' is not necessary when an SDK token is defined; "
                        "we recommend letting the Application generate it automatically"
                    )
            elif quix_sdk_token:
                quix_app_source = "Quix SDK Token"
                quix_config_builder = QuixKafkaConfigsBuilder(
                    quix_sdk_token=quix_sdk_token
                )
            else:
                raise ValueError(
                    'Either "broker_address" or "quix_sdk_token" must be provided'
                )

            # SDK Token or QuixKafkaConfigsBuilder were provided
            logger.info(
                f"{quix_app_source} detected; "
                f"the application will connect to Quix Cloud brokers"
            )
            topic_manager_factory = functools.partial(
                QuixTopicManager, quix_config_builder=quix_config_builder
            )
            # Check if the state dir points to the mounted PVC while running on Quix
            state_dir = Path(state_dir)
            check_state_dir(state_dir=state_dir)
            quix_app_config = quix_config_builder.get_application_config(consumer_group)

            broker_address = quix_app_config.librdkafka_connection_config
            consumer_group = quix_app_config.consumer_group
            consumer_extra_config.update(quix_app_config.librdkafka_extra_config)
            producer_extra_config.update(quix_app_config.librdkafka_extra_config)

        self._config = ApplicationConfig(
            broker_address=broker_address,
            consumer_group=consumer_group,
            consumer_group_prefix=(
                quix_config_builder.workspace_id if quix_config_builder else ""
            ),
            auto_offset_reset=auto_offset_reset,
            commit_interval=commit_interval,
            commit_every=commit_every,
            # Add default values to the producer config, but allow them to be overwritten
            # by the provided producer_extra_config dict
            producer_extra_config={
                **_default_producer_extra_config,
                **producer_extra_config,
            },
            consumer_extra_config=consumer_extra_config,
            processing_guarantee=processing_guarantee,
            consumer_poll_timeout=consumer_poll_timeout,
            producer_poll_timeout=producer_poll_timeout,
            auto_create_topics=auto_create_topics,
            request_timeout=request_timeout,
            topic_create_timeout=topic_create_timeout,
            state_dir=state_dir,
            rocksdb_options=rocksdb_options,
            use_changelog_topics=use_changelog_topics,
        )

        self._on_message_processed = on_message_processed
        self._on_processing_error = on_processing_error or default_on_processing_error

        self._consumer = RowConsumer(
            broker_address=self._config.broker_address,
            consumer_group=self._config.consumer_group,
            auto_offset_reset=self._config.auto_offset_reset,
            auto_commit_enable=False,  # Disable auto commit and manage commits manually
            extra_config=self._config.consumer_extra_config,
            on_error=on_consumer_error,
        )
        self._producer = self._get_rowproducer(on_error=on_producer_error)
        self._running = False
        self._failed = False

        if not topic_manager:
            topic_manager = topic_manager_factory(
                topic_admin=TopicAdmin(
                    broker_address=self._config.broker_address,
                    extra_config=self._config.producer_extra_config,
                ),
                consumer_group=self._config.consumer_group,
                timeout=self._config.request_timeout,
                create_timeout=self._config.topic_create_timeout,
                auto_create_topics=self._config.auto_create_topics,
            )
        self._topic_manager = topic_manager

        producer = None
        recovery_manager = None
        if self._config.use_changelog_topics:
            producer = self._producer
            recovery_manager = RecoveryManager(
                consumer=self._consumer,
                topic_manager=self._topic_manager,
            )

        self._state_manager = StateStoreManager(
            group_id=self._config.consumer_group,
            state_dir=self._config.state_dir,
            rocksdb_options=self._config.rocksdb_options,
            producer=producer,
            recovery_manager=recovery_manager,
        )

        self._source_manager = SourceManager()
        self._sink_manager = SinkManager()
        self._pausing_manager = PausingManager(consumer=self._consumer)
        self._processing_context = ProcessingContext(
            commit_interval=self._config.commit_interval,
            commit_every=self._config.commit_every,
            producer=self._producer,
            consumer=self._consumer,
            state_manager=self._state_manager,
            exactly_once=self._config.exactly_once,
            sink_manager=self._sink_manager,
            pausing_manager=self._pausing_manager,
        )
        self._dataframe_registry = DataframeRegistry()

    @property
    def config(self) -> "ApplicationConfig":
        return self._config

    @property
    def is_quix_app(self) -> bool:
        return self._is_quix_app

    @classmethod
    def Quix(cls, *args, **kwargs):
        """
        RAISES EXCEPTION: DEPRECATED.

        use Application() with "quix_sdk_token" parameter or set the "Quix__Sdk__Token"
        environment variable.
        """

        raise AttributeError(
            "Application.Quix() is now deprecated; "
            "To connect to Quix Cloud, "
            'use Application() with "quix_sdk_token" parameter or set the '
            '"Quix__Sdk__Token" environment variable'
        )

    def topic(
        self,
        name: str,
        value_deserializer: DeserializerType = "json",
        key_deserializer: DeserializerType = "bytes",
        value_serializer: SerializerType = "json",
        key_serializer: SerializerType = "bytes",
        config: Optional[TopicConfig] = None,
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
        :param config: optional topic configurations (for creation/validation)
            >***NOTE:*** will not create without Application's auto_create_topics set
            to True (is True by default)

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
        return self._topic_manager.topic(
            name=name,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            config=config,
            timestamp_extractor=timestamp_extractor,
        )

    def dataframe(
        self,
        topic: Optional[Topic] = None,
        source: Optional[BaseSource] = None,
    ) -> StreamingDataFrame:
        """
        A simple helper method that generates a `StreamingDataFrame`, which is used
        to define your message processing pipeline.

        The topic is what the `StreamingDataFrame` will use as its input, unless
        a source is provided (`topic` is optional when using a `source`).

        If both `topic` AND `source` are provided, the source will write to that topic
        instead of its default topic (which the `StreamingDataFrame` then consumes).

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

        app.run()
        ```


        :param topic: a `quixstreams.models.Topic` instance
            to be used as an input topic.
        :param source: a `quixstreams.sources` "BaseSource" instance
        :return: `StreamingDataFrame` object
        """
        if not source and not topic:
            raise ValueError("one of `source` or `topic` is required")

        if source:
            topic = self.add_source(source, topic)

        sdf = StreamingDataFrame(
            topic=topic,
            topic_manager=self._topic_manager,
            processing_context=self._processing_context,
            registry=self._dataframe_registry,
        )
        self._dataframe_registry.register_root(sdf)

        return sdf

    def stop(self, fail: bool = False):
        """
        Stop the internal poll loop and the message processing.

        Only necessary when manually managing the lifecycle of the `Application` (
        likely through some sort of threading).

        To otherwise stop an application, either send a `SIGTERM` to the process
        (like Kubernetes does) or perform a typical `KeyboardInterrupt` (`Ctrl+C`).

        :param fail: if True, signals that application is stopped due
            to unhandled exception, and it shouldn't commit the current checkpoint.
        """

        self._running = False
        if fail:
            # Update "_failed" only when fail=True to prevent stop(failed=False) from
            # resetting it
            self._failed = True

        if self._state_manager.using_changelogs:
            self._state_manager.stop_recovery()

    def _get_rowproducer(
        self,
        on_error: Optional[ProducerErrorCallback] = None,
        transactional: Optional[bool] = None,
    ) -> RowProducer:
        """
        Create a RowProducer using the application config

        Used to create the application producer as well as the sources producers
        """

        if transactional is None:
            transactional = self._config.exactly_once

        return RowProducer(
            broker_address=self._config.broker_address,
            extra_config=self._config.producer_extra_config,
            flush_timeout=self._config.flush_timeout,
            on_error=on_error,
            transactional=transactional,
        )

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

        app = Application(...)
        topic = app.topic("input")

        with app.get_producer() as producer:
            for i in range(100):
                producer.produce(topic=topic.name, key=b"key", value=b"value")
        ```
        """
        self.setup_topics()

        return Producer(
            broker_address=self._config.broker_address,
            extra_config=self._config.producer_extra_config,
        )

    def get_consumer(self, auto_commit_enable: bool = True) -> Consumer:
        """
        Create and return a pre-configured Consumer instance.
        The Consumer is initialized with params passed to Application.

        It's useful for consuming data from Kafka outside the standard
        Application processing flow.
        (e.g., to consume test data from a topic).
        Using it within the StreamingDataFrame functions is not recommended, as it
        creates a new Consumer instance
        each time, which is not optimized for repeated use in a streaming pipeline.

        Note: By default, this consumer does not autocommit the consumed offsets to allow
        at-least-once processing.
        To store the offset call store_offsets() after processing a message.
        If autocommit is necessary set `enable.auto.offset.store` to True in
        the consumer config when creating the app.

        Example Snippet:

        ```python
        from quixstreams import Application

        app = Application(...)
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

        :param auto_commit_enable: Enable or disable auto commit
            Default - True
        """
        self.setup_topics()

        return Consumer(
            broker_address=self._config.broker_address,
            consumer_group=self._config.consumer_group,
            auto_offset_reset=self._config.auto_offset_reset,
            auto_commit_enable=auto_commit_enable,
            extra_config=self._config.consumer_extra_config,
        )

    def clear_state(self):
        """
        Clear the state of the application.
        """
        self._state_manager.clear_stores()

    def add_source(self, source: BaseSource, topic: Optional[Topic] = None) -> Topic:
        """
        Add a source to the application.

        Use when no transformations (which requires a `StreamingDataFrame`) are needed.

        See :class:`quixstreams.sources.base.BaseSource` for more details.

        :param source: a :class:`quixstreams.sources.BaseSource` instance
        :param topic: the :class:`quixstreams.models.Topic` instance the source will produce to
            Default: the source default
        """
        if not topic:
            topic = self._topic_manager.register(source.default_topic())

        producer = self._get_rowproducer(transactional=False)
        source.configure(topic, producer)
        self._source_manager.register(source)
        return topic

    def run(self, dataframe: Optional[StreamingDataFrame] = None):
        """
        Start processing data from Kafka using provided `StreamingDataFrame`

        Once started, it can be safely terminated with a `SIGTERM` signal
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

        app.run()
        ```
        """
        if dataframe is not None:
            warnings.warn(
                "Application.run() received a `dataframe` argument which is "
                "no longer used (StreamingDataFrames are now tracked automatically); "
                "the argument should be removed.",
                FutureWarning,
            )
        self._run()

    def _exception_handler(self, exc_type, exc_val, exc_tb):
        fail = False

        # Sources and the application are independent.
        # If a source fails, the application can shutdown gracefully.
        if exc_val is not None and exc_type is not SourceException:
            fail = True

        self.stop(fail=fail)

    def _run(self):
        self._setup_signal_handlers()

        logger.info(
            f"Starting the Application with the config: "
            f'broker_address="{self._config.broker_address}" '
            f'consumer_group="{self._config.consumer_group}" '
            f'auto_offset_reset="{self._config.auto_offset_reset}" '
            f"commit_interval={self._config.commit_interval}s "
            f"commit_every={self._config.commit_every} "
            f'processing_guarantee="{self._config.processing_guarantee}"'
        )
        if self.is_quix_app:
            self._quix_runtime_init()

        self.setup_topics()

        exit_stack = contextlib.ExitStack()
        exit_stack.enter_context(self._processing_context)
        exit_stack.enter_context(self._state_manager)
        exit_stack.enter_context(self._consumer)
        exit_stack.enter_context(self._source_manager)
        exit_stack.push(self._exception_handler)

        with exit_stack:
            # Subscribe to topics in Kafka and start polling
            if self._dataframe_registry.consumer_topics:
                self._run_dataframe()
            else:
                self._run_sources()

    def _run_dataframe(self):
        self._consumer.subscribe(
            self._dataframe_registry.consumer_topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        logger.info("Waiting for incoming messages")
        # Start polling Kafka for messages and callbacks
        self._running = True

        # Initialize the checkpoint
        self._processing_context.init_checkpoint()

        dataframes_composed = self._dataframe_registry.compose_all()

        while self._running:
            if self._state_manager.recovery_required:
                self._state_manager.do_recovery()
            else:
                self._process_message(dataframes_composed)
                self._processing_context.commit_checkpoint()
                self._processing_context.resume_ready_partitions()
                self._source_manager.raise_for_error()

        logger.info("Stop processing of StreamingDataFrame")

    def _run_sources(self):
        self._running = True
        self._source_manager.start_sources()
        while self._running:
            self._source_manager.raise_for_error()

            if not self._source_manager.is_alive():
                self.stop()

            time.sleep(1)

    def _quix_runtime_init(self):
        """
        Do a runtime setup only applicable to a Quix Application instance
        - Ensure that "State management" flag is enabled for deployment if the app
          is stateful and is running in Quix Cloud
        """
        # Ensure that state management is enabled if application is stateful
        if self._state_manager.stores:
            check_state_management_enabled()

    def setup_topics(self):
        """
        Validate and create the topics
        """

        topics_list = ", ".join(
            f'"{topic}"' for topic in self._topic_manager.all_topics
        )
        logger.info(f"Topics required for this application: {topics_list}")
        self._topic_manager.create_all_topics()
        self._topic_manager.validate_all_topics()

    def _process_message(self, dataframe_composed):
        # Serve producer callbacks
        self._producer.poll(self._config.producer_poll_timeout)
        rows = self._consumer.poll_row(timeout=self._config.consumer_poll_timeout)

        if rows is None:
            return

        # Deserializer may return multiple rows for a single message
        rows = rows if isinstance(rows, list) else [rows]
        if not rows:
            return

        first_row = rows[0]
        topic_name, partition, offset = (
            first_row.topic,
            first_row.partition,
            first_row.offset,
        )

        for row in rows:
            context = copy_context()
            context.run(set_message_context, row.context)
            try:
                # Execute StreamingDataFrame in a context
                context.run(
                    dataframe_composed[topic_name],
                    row.value,
                    row.key,
                    row.timestamp,
                    row.headers,
                )
            except Exception as exc:
                # TODO: This callback might be triggered because of Producer
                #  errors too because they happen within ".process()"
                to_suppress = self._on_processing_error(exc, row, logger)
                if not to_suppress:
                    raise

        # Store the message offset after it's successfully processed
        self._processing_context.store_offset(
            topic=topic_name, partition=partition, offset=offset
        )

        if self._on_message_processed is not None:
            self._on_message_processed(topic_name, partition, offset)

    def _on_assign(self, _, topic_partitions: List[TopicPartition]):
        """
        Assign new topic partitions to consumer and state.

        :param topic_partitions: list of `TopicPartition` from Kafka
        """
        # sometimes "empty" calls happen, probably updating the consumer epoch
        if not topic_partitions:
            return
        logger.debug("Rebalancing: assigning partitions")

        # Only start the sources once the consumer is assigned. Otherwise a source
        # can produce data before the consumer starts. If that happens on a new
        # consumer with `auto_offset_reset` set to `latest` the consumer will not
        # get the source data.
        self._source_manager.start_sources()

        # First commit everything processed so far because assignment can take a while
        # and fail
        self._processing_context.commit_checkpoint(force=True)

        # assigning manually here (instead of allowing it handle it automatically)
        # enables pausing them during recovery to work as expected
        self._consumer.incremental_assign(topic_partitions)

        if self._state_manager.stores:
            for tp in topic_partitions:
                # Get the latest committed offset for the assigned topic partition
                tp_committed = self._consumer.committed([tp], timeout=30)[0]
                # Assign store partitions
                store_partitions = self._state_manager.on_partition_assign(
                    topic=tp.topic,
                    partition=tp.partition,
                    committed_offset=tp_committed.offset,
                )

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
                if (
                    min_stored_offset is not None
                    and min_stored_offset > tp_committed.offset
                ):
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
        # Commit everything processed so far unless the application is closing
        # because of the unhandled exception.
        # In this case, we should drop the checkpoint and let another consumer
        # pick up from the latest one
        logger.debug("Rebalancing: revoking partitions")
        if self._failed:
            logger.warning(
                "Application is stopping due to failure, "
                "latest checkpoint will not be committed."
            )
        else:
            self._processing_context.commit_checkpoint(force=True)

        self._consumer.incremental_unassign(topic_partitions)
        for tp in topic_partitions:
            if self._state_manager.stores:
                self._state_manager.on_partition_revoke(
                    topic=tp.topic, partition=tp.partition
                )
            self._processing_context.on_partition_revoke(
                topic=tp.topic, partition=tp.partition
            )

    def _on_lost(self, _, topic_partitions: List[TopicPartition]):
        """
        Dropping lost partitions from consumer and state
        """
        logger.debug("Rebalancing: dropping lost partitions")
        for tp in topic_partitions:
            if self._state_manager.stores:
                self._state_manager.on_partition_revoke(
                    topic=tp.topic, partition=tp.partition
                )
            self._processing_context.on_partition_revoke(
                topic=tp.topic, partition=tp.partition
            )

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._on_sigint)
        signal.signal(signal.SIGTERM, self._on_sigterm)

    def _on_sigint(self, *_):
        # Re-install the default SIGINT handler so doing Ctrl+C twice
        # raises KeyboardInterrupt
        signal.signal(signal.SIGINT, signal.default_int_handler)
        logger.debug("Received SIGINT, stopping the processing loop")
        self.stop()

    def _on_sigterm(self, *_):
        logger.debug("Received SIGTERM, stopping the processing loop")
        self.stop()


class ApplicationConfig(BaseSettings):
    """
    Immutable object holding the application configuration

    For details see :class:`quixstreams.Application`
    """

    model_config = SettingsConfigDict(
        frozen=True,
        revalidate_instances="always",
        alias_generator=AliasGenerator(
            # used during model_dumps
            serialization_alias=lambda field_name: field_name.replace("_", "."),
        ),
    )

    broker_address: ConnectionConfig
    consumer_group: str
    consumer_group_prefix: str
    auto_offset_reset: AutoOffsetReset = "latest"
    commit_interval: float = 5.0
    commit_every: int = 0
    producer_extra_config: dict = Field(default_factory=dict)
    consumer_extra_config: dict = Field(default_factory=dict)
    processing_guarantee: ProcessingGuarantee = "at-least-once"
    consumer_poll_timeout: float = 1.0
    producer_poll_timeout: float = 0.0
    auto_create_topics: bool = True
    request_timeout: float = 30
    topic_create_timeout: float = 60
    state_dir: Path = Path("state")
    rocksdb_options: Optional[RocksDBOptionsType] = None
    use_changelog_topics: bool = True

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Included to ignore reading/setting values from the environment
        """
        return (init_settings,)

    def copy(self, **kwargs) -> Self:
        """
        Update the application config and return a copy
        """
        copy = self.model_copy(update=kwargs)
        copy.model_validate(copy, strict=True)
        return copy

    @property
    def flush_timeout(self) -> float:
        return (
            self.consumer_extra_config.get(
                "max.poll.interval.ms", _default_max_poll_interval_ms
            )
            / 1000
        )  # convert to seconds

    @property
    def exactly_once(self) -> bool:
        return self.processing_guarantee == "exactly-once"
