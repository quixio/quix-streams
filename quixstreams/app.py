import contextlib
import functools
import logging
import os
import signal
import time
import warnings
from collections import defaultdict
from pathlib import Path
from typing import (
    Callable,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Type,
    Union,
)

from confluent_kafka import TopicPartition
from pydantic import AliasGenerator, Field
from pydantic_settings import BaseSettings as PydanticBaseSettings
from pydantic_settings import PydanticBaseSettingsSource, SettingsConfigDict

from .context import copy_context, set_message_context
from .dataframe import DataFrameRegistry, StreamingDataFrame
from .error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
    default_on_processing_error,
)
from .internal_consumer import InternalConsumer
from .internal_producer import InternalProducer
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
    is_quix_deployment,
)
from .processing import ProcessingContext
from .runtracker import RunTracker
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

# Enforce idempotent producing for InternalProducer
_default_producer_extra_config = {"enable.idempotence": True}

# Default config for the internal consumer
_default_consumer_extra_config = {
    "fetch.queue.backoff.ms": 100,  # Make the consumer to fetch data more often
}

# Force assignment strategy to be "range" for co-partitioning in internal Consumers
consumer_extra_config_overrides = {"partition.assignment.strategy": "range"}

_default_max_poll_interval_ms = 300000


class TopicManagerFactory(Protocol):
    def __call__(
        self,
        topic_admin: TopicAdmin,
        consumer_group: str,
        timeout: float = 30,
        create_timeout: float = 60,
        auto_create_topics: bool = True,
    ) -> TopicManager: ...


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
        state_dir: Union[None, str, Path] = None,
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
        max_partition_buffer_size: int = 10000,
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
            Linked Environment Variable: `Quix__Consumer_Group`.
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
            Linked Environment Variable: `Quix__State__Dir`.
            Default - `"state"`.
        :param rocksdb_options: RocksDB options.
            If `None`, the default options will be used.
        :param consumer_poll_timeout: timeout for `InternalConsumer.poll()`. Default - `1.0`s
        :param producer_poll_timeout: timeout for `InternalProducer.poll()`. Default - `0`s.
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
        :param max_partition_buffer_size:  the maximum number of messages to buffer per topic partition to consider it full.
            The buffering is used to consume messages in-order between multiple partitions with the same number.
            It is a soft limit, and the actual number of buffered messages can be up to x2 higher.
            Lower value decreases the memory use, but increases the latency.
            Default - `10000`.

        <br><br>***Error Handlers***<br>
        To handle errors, `Application` accepts callbacks triggered when
            exceptions occur on different stages of stream processing. If the callback
            returns `True`, the exception will be ignored. Otherwise, the exception
            will be propagated and the processing will eventually stop.
        :param on_consumer_error: triggered when internal `InternalConsumer` fails
            to poll Kafka or cannot deserialize a message.
        :param on_processing_error: triggered when exception is raised within
            `StreamingDataFrame.process()`.
        :param on_producer_error: triggered when `InternalProducer` fails to serialize
            or to produce a message to Kafka.
        <br><br>***Quix Cloud Parameters***<br>
        :param quix_config_builder: instance of `QuixKafkaConfigsBuilder` to be used
            instead of the default one.
            > NOTE: It is recommended to just use `quix_sdk_token` instead.
        """
        configure_logging(loglevel=loglevel)

        producer_extra_config = producer_extra_config or {}
        consumer_extra_config = consumer_extra_config or {}

        if state_dir is None:
            state_dir = os.getenv(
                "Quix__State__Dir", "/app/state" if is_quix_deployment() else "state"
            )
        state_dir = Path(state_dir)

        # We can't use os.getenv as defaults (and have testing work nicely)
        # since it evaluates getenv when the function is defined.
        # In general this is just a most robust approach.
        broker_address = broker_address or os.getenv("Quix__Broker__Address")
        quix_sdk_token = quix_sdk_token or os.getenv("Quix__Sdk__Token")

        if not consumer_group:
            consumer_group = os.getenv("Quix__Consumer_Group", "quixstreams-default")

        if broker_address:
            # If broker_address is passed to the app it takes priority over any quix configuration
            self._is_quix_app = False
            self._topic_manager_factory: TopicManagerFactory = TopicManager
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
            self._topic_manager_factory = functools.partial(
                QuixTopicManager, quix_config_builder=quix_config_builder
            )
            # Check if the state dir points to the mounted PVC while running on Quix
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
            consumer_extra_config={
                **_default_consumer_extra_config,
                **consumer_extra_config,
            },
            processing_guarantee=processing_guarantee,
            consumer_poll_timeout=consumer_poll_timeout,
            producer_poll_timeout=producer_poll_timeout,
            auto_create_topics=auto_create_topics,
            request_timeout=request_timeout,
            topic_create_timeout=topic_create_timeout,
            state_dir=state_dir,
            rocksdb_options=rocksdb_options,
            use_changelog_topics=use_changelog_topics,
            max_partition_buffer_size=max_partition_buffer_size,
        )

        self._on_message_processed = on_message_processed
        self._on_processing_error = on_processing_error or default_on_processing_error

        self._consumer = self._get_internal_consumer(
            on_error=on_consumer_error,
            extra_config_overrides=consumer_extra_config_overrides,
        )
        self._producer = self._get_internal_producer(on_error=on_producer_error)
        self._running = False
        self._failed = False

        self._topic_manager = topic_manager or self._get_topic_manager()

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
        self._dataframe_registry = DataFrameRegistry()
        self._processing_context = ProcessingContext(
            commit_interval=self._config.commit_interval,
            commit_every=self._config.commit_every,
            producer=self._producer,
            consumer=self._consumer,
            state_manager=self._state_manager,
            exactly_once=self._config.exactly_once,
            sink_manager=self._sink_manager,
            dataframe_registry=self._dataframe_registry,
        )
        self._run_tracker = RunTracker(processing_context=self._processing_context)

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

    def _get_topic_manager(self) -> TopicManager:
        """
        Create a TopicAdmin using the application config

        Used to create the application topic admin as well as the sources topic admins
        """
        return self._topic_manager_factory(
            topic_admin=TopicAdmin(
                broker_address=self._config.broker_address,
                extra_config=self._config.producer_extra_config,
            ),
            consumer_group=self._config.consumer_group,
            timeout=self._config.request_timeout,
            create_timeout=self._config.topic_create_timeout,
            auto_create_topics=self._config.auto_create_topics,
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
            create_config=config,
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

        if source is not None:
            topic = self.add_source(source, topic)

        if topic is None:
            raise ValueError("one of `source` or `topic` is required")

        sdf = StreamingDataFrame(
            topic,
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

        self._run_tracker.stop_and_reset()
        if fail:
            # Update "_failed" only when fail=True to prevent stop(failed=False) from
            # resetting it
            self._failed = True

        if self._state_manager.using_changelogs:
            self._state_manager.stop_recovery()

    def _get_internal_producer(
        self,
        on_error: Optional[ProducerErrorCallback] = None,
        transactional: Optional[bool] = None,
    ) -> InternalProducer:
        """
        Create InternalProducer using the application config

        Used to create the application producer as well as the sources producers
        """

        if transactional is None:
            transactional = self._config.exactly_once

        return InternalProducer(
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

        return Producer(
            broker_address=self._config.broker_address,
            extra_config=self._config.producer_extra_config,
        )

    def _get_internal_consumer(
        self,
        on_error: Optional[ConsumerErrorCallback] = None,
        extra_config_overrides: Optional[dict] = None,
    ) -> InternalConsumer:
        """
        Create an InternalConsumer using the application config

        Used to create the application consumer as well as the sources consumers
        """
        extra_config_overrides = extra_config_overrides or {}
        # Override the existing "extra_config" with new values
        extra_config = {
            **self._config.consumer_extra_config,
            **extra_config_overrides,
        }
        return InternalConsumer(
            broker_address=self._config.broker_address,
            consumer_group=self._config.consumer_group,
            auto_offset_reset=self._config.auto_offset_reset,
            auto_commit_enable=False,  # Disable auto commit and manage commits manually
            max_partition_buffer_size=self._config.max_partition_buffer_size,
            extra_config=extra_config,
            on_error=on_error,
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
            Default - the topic generated by the `source.default_topic()` method.
            Note: the names of default topics are prefixed with "source__".
        """
        if not topic:
            # Prefix the name of the default topic generated by the Source
            # for visibility across all the topics.
            default_topic = source.default_topic()
            default_topic = default_topic.__clone__(
                name=f"source__{default_topic.name}"
            )
            topic = self._topic_manager.register(default_topic)

        self._source_manager.register(
            source,
            topic,
            self._get_internal_producer(transactional=False),
            self._get_internal_consumer(
                extra_config_overrides=consumer_extra_config_overrides
            ),
            self._get_topic_manager(),
        )
        return topic

    def run(
        self,
        dataframe: Optional[StreamingDataFrame] = None,
        timeout: float = 0.0,
        count: int = 0,
    ):
        """
        Start processing data from Kafka using provided `StreamingDataFrame`

        Once started, it can be safely terminated with a `SIGTERM` signal
        (like Kubernetes does) or a typical `KeyboardInterrupt` (`Ctrl+C`).

        Alternatively, stop conditions can be set (typically for debugging purposes);
            has the option of stopping after a number of messages, timeout, or both.

        Not setting a timeout or count limit will result in the Application running
          indefinitely (expected production behavior).


        Stop Condition Details:

        A timeout will immediately stop an Application once no new messages have
            been consumed after T seconds (after rebalance and recovery).

        A count will process N total records from ANY input/SDF topics (so
          multiple input topics will very likely differ in their consume total!) after
          an initial rebalance and recovery.
        THEN, any remaining processes from things such as groupby (which uses internal
          topics) will also be validated to ensure the results of said messages are
          fully processed (this does NOT count towards the process total).
        Note that without a timeout, the Application runs until the count is hit.

        If timeout and count are used together (which is the recommended pattern for
        debugging), either condition will trigger a stop.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Set up an `app = Application` and  `sdf = StreamingDataFrame`;
        # add some operations to `sdf` and then run everything.

        app = Application(broker_address='localhost:9092', consumer_group='group')
        topic = app.topic('test-topic')
        df = app.dataframe(topic)
        df.apply(lambda value, context: print('New message', value)

        app.run()  # could pass `timeout=5` here, for example
        ```
        :param dataframe: DEPRECATED - do not use; sdfs are now automatically tracked.
        :param timeout: maximum time to wait for a new message.
            Default = 0.0 (infinite)
        :param count: how many input topic messages to process before stopping.
            Default = 0 (infinite)
        """
        if dataframe is not None:
            warnings.warn(
                "Application.run() received a `dataframe` argument which is "
                "no longer used (StreamingDataFrames are now tracked automatically); "
                "the argument should be removed.",
                FutureWarning,
            )
        if not self._dataframe_registry.consumer_topics:
            # This is a plain source (no SDF), so a timeout is the only valid stopper.
            if count:
                raise ValueError(
                    "Can only provide a timeout to .run() when running "
                    "a plain Source (no StreamingDataFrame)."
                )
        self._run_tracker.set_topics(
            [t for t in self._topic_manager.topics],
            [t for t in self._topic_manager.repartition_topics],
        )
        self._run_tracker.set_stop_condition(timeout=timeout, count=count)
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
        changelog_topics = self._topic_manager.changelog_topics_list
        self._consumer.subscribe(
            topics=self._dataframe_registry.consumer_topics + changelog_topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )

        # set refs for performance improvements
        dataframes_composed = self._dataframe_registry.compose_all()
        state_manager = self._state_manager
        processing_context = self._processing_context
        source_manager = self._source_manager
        process_message = self._process_message
        printer = self._processing_context.printer
        run_tracker = self._run_tracker

        processing_context.init_checkpoint()
        run_tracker.set_as_running()
        logger.info("Waiting for incoming messages")
        # Start polling Kafka for messages and callbacks
        while run_tracker.running:
            if state_manager.recovery_required:
                state_manager.do_recovery()
                run_tracker.timeout_refresh()
            else:
                process_message(dataframes_composed)
                processing_context.commit_checkpoint()
                self._consumer.resume_backpressured()
                source_manager.raise_for_error()
                printer.print()
                run_tracker.update_status()

        logger.info("Stop processing of StreamingDataFrame")
        processing_context.commit_checkpoint(force=True)

    def _run_sources(self):
        run_tracker = self._run_tracker
        source_manager = self._source_manager

        run_tracker.set_as_running()
        source_manager.start_sources()
        while run_tracker.running and source_manager.is_alive():
            source_manager.raise_for_error()
            run_tracker.update_status()
            time.sleep(1)
        self.stop()

    def _quix_runtime_init(self):
        """
        Do a runtime setup only applicable to a Quix Application instance
        - Ensure that "State management" flag is enabled for deployment if the app
          is stateful and is running in Quix Cloud
        """
        # Ensure that state management is enabled if application is stateful
        if self._state_manager.stores:
            check_state_management_enabled()

    def _process_message(self, dataframe_composed):
        # Serve producer callbacks
        self._producer.poll(self._config.producer_poll_timeout)
        rows = self._consumer.poll_row(
            timeout=self._config.consumer_poll_timeout,
            buffered=self._dataframe_registry.requires_time_alignment,
        )

        if rows is None:
            self._run_tracker.set_current_message_tp(None)
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
        self._run_tracker.set_current_message_tp((topic_name, partition))

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

        # Assign partitions manually to pause the changelog topics
        self._consumer.assign(topic_partitions)
        # Pause changelog topic+partitions immediately after assignment
        non_changelog_topics = self._topic_manager.non_changelog_topics
        changelog_tps = [
            tp for tp in topic_partitions if tp.topic not in non_changelog_topics
        ]
        self._consumer.pause(changelog_tps)

        if self._state_manager.stores:
            non_changelog_tps = [
                tp for tp in topic_partitions if tp.topic in non_changelog_topics
            ]
            committed_tps = self._consumer.committed(
                partitions=non_changelog_tps, timeout=30
            )
            committed_offsets: dict[int, dict[str, int]] = defaultdict(dict)
            for tp in committed_tps:
                if tp.error:
                    raise RuntimeError(
                        f"Failed to get committed offsets for "
                        f'"{tp.topic}[{tp.partition}]" from the broker: {tp.error}'
                    )
                committed_offsets[tp.partition][tp.topic] = tp.offset

            # Match the assigned TP with a stream ID via DataFrameRegistry
            for tp in non_changelog_tps:
                stream_ids = self._dataframe_registry.get_stream_ids(
                    topic_name=tp.topic
                )
                # Assign store partitions for the given stream ids
                for stream_id in stream_ids:
                    self._state_manager.on_partition_assign(
                        stream_id=stream_id,
                        partition=tp.partition,
                        committed_offsets=committed_offsets[tp.partition],
                    )
        self._run_tracker.timeout_refresh()

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

        self._revoke_state_partitions(topic_partitions=topic_partitions)
        self._consumer.reset_backpressure()

    def _on_lost(self, _, topic_partitions: List[TopicPartition]):
        """
        Dropping lost partitions from consumer and state
        """
        logger.debug("Rebalancing: dropping lost partitions")

        self._revoke_state_partitions(topic_partitions=topic_partitions)
        self._consumer.reset_backpressure()

    def _revoke_state_partitions(self, topic_partitions: List[TopicPartition]):
        non_changelog_topics = self._topic_manager.non_changelog_topics
        non_changelog_tps = [
            tp for tp in topic_partitions if tp.topic in non_changelog_topics
        ]
        for tp in non_changelog_tps:
            if self._state_manager.stores:
                stream_ids = self._dataframe_registry.get_stream_ids(
                    topic_name=tp.topic
                )
                for stream_id in stream_ids:
                    self._state_manager.on_partition_revoke(
                        stream_id=stream_id, partition=tp.partition
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
    max_partition_buffer_size: int = 10000

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[PydanticBaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Included to ignore reading/setting values from the environment
        """
        return (init_settings,)

    def copy(self, **kwargs) -> "ApplicationConfig":
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
