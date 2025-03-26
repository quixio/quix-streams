import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional, Union
from unittest.mock import PropertyMock, create_autospec, patch

import pytest
from confluent_kafka.admin import (
    AdminClient,
    NewPartitions,
    NewTopic,
)

from quixstreams.app import Application, MessageProcessedCallback, ProcessingGuarantee
from quixstreams.error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
)
from quixstreams.kafka import (
    AutoOffsetReset,
    Consumer,
    Producer,
)
from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.models import MessageContext
from quixstreams.models.rows import Row
from quixstreams.models.serializers import (
    Deserializer,
    JSONDeserializer,
    JSONSerializer,
    Serializer,
)
from quixstreams.models.topics import (
    TimestampExtractor,
    Topic,
    TopicAdmin,
    TopicConfig,
    TopicManager,
)
from quixstreams.models.topics.exceptions import TopicNotFoundError
from quixstreams.platforms.quix import QuixTopicManager
from quixstreams.platforms.quix.config import (
    QuixApplicationConfig,
    QuixKafkaConfigsBuilder,
    prepend_workspace_id,
    strip_workspace_id_prefix,
)
from quixstreams.rowconsumer import RowConsumer
from quixstreams.rowproducer import RowProducer
from quixstreams.state import StateStoreManager
from quixstreams.state.manager import StoreTypes
from quixstreams.state.recovery import RecoveryManager


@pytest.fixture()
def kafka_admin_client(kafka_container) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_container.broker_address})


@pytest.fixture()
def random_consumer_group() -> str:
    return str(uuid.uuid4())


@pytest.fixture()
def consumer_factory(kafka_container, random_consumer_group):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
    ) -> Consumer:
        consumer_group = consumer_group or random_consumer_group
        extras = {
            # Make consumers to refresh cluster metadata often
            # to react on re-assignment changes faster
            "topic.metadata.refresh.interval.ms": 3000,
            # Keep rebalances as simple as possible for testing
            "partition.assignment.strategy": "range",
        }
        extras.update((extra_config or {}))

        return Consumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            auto_offset_reset=auto_offset_reset,
            extra_config=extras,
        )

    return factory


@pytest.fixture()
def consumer(consumer_factory) -> Consumer:
    return consumer_factory()


@pytest.fixture()
def producer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        extra_config: dict = None,
    ) -> Producer:
        extra_config = extra_config or {}

        return Producer(
            broker_address=broker_address,
            extra_config=extra_config,
        )

    return factory


@pytest.fixture()
def producer(producer_factory) -> Producer:
    return producer_factory()


@pytest.fixture()
def executor() -> ThreadPoolExecutor:
    executor = ThreadPoolExecutor(1)
    yield executor
    # Kill all the threads after leaving the test
    executor.shutdown(wait=False)


@pytest.fixture()
def topic_factory(kafka_admin_client):
    """
    For when you need to create a topic in Kafka.

    The factory will return the resulting topic name and partition count
    """

    def factory(
        topic: str = None, num_partitions: int = 1, timeout: float = 20.0
    ) -> (str, int):
        topic_name = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_topics(
            [NewTopic(topic=topic_name, num_partitions=num_partitions)]
        )
        futures[topic_name].result(timeout)
        return topic_name, num_partitions

    return factory


@pytest.fixture()
def topic_json_serdes_factory(topic_factory):
    """
    For when you need to create a topic in Kafka and want a `Topic` object afterward.
    Additionally, uses JSON serdes for message values by default.

    The factory will return the resulting Topic object.
    """

    def factory(
        topic: str = None,
        num_partitions: int = 1,
        timeout: float = 10.0,
        create_topic: bool = True,
    ):
        if create_topic:
            topic_name, _ = topic_factory(
                topic=topic, num_partitions=num_partitions, timeout=timeout
            )
        else:
            topic_name = uuid.uuid4()
        return Topic(
            name=topic or topic_name,
            create_config=TopicConfig(
                num_partitions=num_partitions, replication_factor=1
            ),
            value_deserializer=JSONDeserializer(),
            value_serializer=JSONSerializer(),
        )

    return factory


@pytest.fixture()
def set_topic_partitions(kafka_admin_client):
    def func(
        topic: str = None, num_partitions: int = 1, timeout: float = 10.0
    ) -> (str, int):
        topic = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_partitions(
            [NewPartitions(topic=topic, new_total_count=num_partitions)]
        )
        futures[topic].result(timeout)
        return topic, num_partitions

    return func


@pytest.fixture()
def row_consumer_factory(kafka_container, random_consumer_group):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
        on_error: Optional[ConsumerErrorCallback] = None,
    ) -> RowConsumer:
        extra_config = extra_config or {}
        consumer_group = consumer_group or random_consumer_group

        # Make consumers to refresh cluster metadata often
        # to react on re-assignment changes faster
        extra_config["topic.metadata.refresh.interval.ms"] = 3000
        return RowConsumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            auto_offset_reset=auto_offset_reset,
            extra_config=extra_config,
            on_error=on_error,
        )

    return factory


@pytest.fixture()
def row_producer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        extra_config: dict = None,
        on_error: Optional[ProducerErrorCallback] = None,
        transactional: bool = False,
    ) -> RowProducer:
        return RowProducer(
            broker_address=broker_address,
            extra_config=extra_config,
            on_error=on_error,
            transactional=transactional,
        )

    return factory


@pytest.fixture()
def row_producer(row_producer_factory):
    return row_producer_factory()


@pytest.fixture()
def transactional_row_producer(row_producer_factory):
    return row_producer_factory(transactional=True)


@pytest.fixture()
def row_factory():
    """
    This factory includes only the fields typically handed to a producer when
    producing a message; more generally, the fields you would likely
    need to validate upon producing/consuming.
    """

    def factory(
        value,
        topic="input-topic",
        key=b"key",
        timestamp: int = 0,
        headers=None,
        partition: int = 0,
        offset: int = 0,
    ) -> Row:
        context = MessageContext(
            topic=topic,
            partition=partition,
            offset=offset,
            size=0,
        )
        return Row(
            value=value, key=key, timestamp=timestamp, context=context, headers=headers
        )

    return factory


@pytest.fixture()
def app_factory(kafka_container, random_consumer_group, tmp_path, store_type):
    def factory(
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        commit_interval: float = 5.0,
        commit_every: int = 0,
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        consumer_poll_timeout: float = 1.0,
        producer_poll_timeout: float = 0.0,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        state_dir: Optional[str] = None,
        auto_create_topics: bool = True,
        use_changelog_topics: bool = True,
        topic_manager: Optional[TopicManager] = None,
        processing_guarantee: ProcessingGuarantee = "at-least-once",
        request_timeout: float = 30,
    ) -> Application:
        state_dir = state_dir or (tmp_path / "state").absolute()
        return Application(
            broker_address=kafka_container.broker_address,
            consumer_group=consumer_group or random_consumer_group,
            auto_offset_reset=auto_offset_reset,
            commit_interval=commit_interval,
            commit_every=commit_every,
            consumer_extra_config=consumer_extra_config,
            producer_extra_config=producer_extra_config,
            consumer_poll_timeout=consumer_poll_timeout,
            producer_poll_timeout=producer_poll_timeout,
            on_consumer_error=on_consumer_error,
            on_producer_error=on_producer_error,
            on_processing_error=on_processing_error,
            on_message_processed=on_message_processed,
            state_dir=state_dir,
            auto_create_topics=auto_create_topics,
            use_changelog_topics=use_changelog_topics,
            topic_manager=topic_manager,
            processing_guarantee=processing_guarantee,
            request_timeout=request_timeout,
        )

    with patch(
        "quixstreams.state.manager.StateStoreManager.default_store_type",
        new_callable=PropertyMock,
    ) as m:
        m.return_value = store_type
        yield factory


@pytest.fixture()
def state_manager_factory(store_type, tmp_path):
    def factory(
        group_id: Optional[str] = None,
        state_dir: Optional[str] = None,
        producer: Optional[RowProducer] = None,
        recovery_manager: Optional[RecoveryManager] = None,
        default_store_type: StoreTypes = store_type,
    ) -> StateStoreManager:
        group_id = group_id or str(uuid.uuid4())
        state_dir = state_dir or str(uuid.uuid4())
        return StateStoreManager(
            group_id=group_id,
            state_dir=str(tmp_path / state_dir),
            producer=producer,
            recovery_manager=recovery_manager,
            default_store_type=default_store_type,
        )

    return factory


@pytest.fixture()
def state_manager(state_manager_factory) -> StateStoreManager:
    manager = state_manager_factory()
    manager.init()
    yield manager
    manager.close()


@pytest.fixture()
def quix_mock_config_builder_factory(kafka_container):
    def factory(workspace_id: Optional[str] = None):
        if not workspace_id:
            workspace_id = "my_ws"
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder, spec_set=True)
        patch.object(
            cfg_builder,
            "workspace_id",
            new_callable=PropertyMock(return_value=workspace_id),
        ).start()

        # Slight change to ws stuff in case you pass a blank workspace (which makes
        #  some things easier
        cfg_builder.prepend_workspace_id.side_effect = lambda s: (
            prepend_workspace_id(workspace_id, s) if workspace_id else s
        )
        cfg_builder.strip_workspace_id_prefix.side_effect = lambda s: (
            strip_workspace_id_prefix(workspace_id, s) if workspace_id else s
        )

        cfg_builder.convert_topic_response = (
            QuixKafkaConfigsBuilder.convert_topic_response
        )

        # Mock the create API call and return this response.
        # Doing it this way keeps the old behavior where topics are only created
        # when the app is actually run (for tests, at least).
        # This does simulate an expected topic name with prepended WID which may not
        # always be true, but it's just to make testing easier.
        topic_response_mock = lambda topic, timeout=None: {
            "id": f"{workspace_id}-{topic.name}",
            "name": topic.name,
            "configuration": {
                "partitions": topic.create_config.num_partitions,
                "replicationFactor": topic.create_config.replication_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Delete",
            },
        }
        cfg_builder.get_or_create_topic.side_effect = topic_response_mock
        cfg_builder.create_topic.side_effect = topic_response_mock
        cfg_builder.get_topic.side_effect = topic_response_mock

        # Connect to local test container rather than Quix
        connection = ConnectionConfig(bootstrap_servers=kafka_container.broker_address)
        cfg_builder.librdkafka_connection_config = connection
        cfg_builder.get_application_config.side_effect = lambda cg: (
            QuixApplicationConfig(
                connection,
                {"connections.max.idle.ms": 60000},
                cfg_builder.prepend_workspace_id(cg),
            )
        )

        return cfg_builder

    return factory


@pytest.fixture()
def quix_topic_manager_factory(
    quix_mock_config_builder_factory,
    topic_admin,
    topic_manager_factory,
    random_consumer_group,
):
    """
    Allows for creating topics with a test cluster while keeping the workspace aspects
    """

    def factory(
        workspace_id: Optional[str] = None,
        consumer_group: str = random_consumer_group,
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
    ):
        if not quix_config_builder:
            quix_config_builder = quix_mock_config_builder_factory(
                workspace_id=workspace_id
            )
        quix_topic_manager = QuixTopicManager(
            topic_admin=topic_admin,
            consumer_group=consumer_group,
            quix_config_builder=quix_config_builder,
        )

        # Patch the instance of QuixTopicManager to use Kafka Admin API
        # to fetch topics instead of Quix Portal API
        def _mock_fetch_topic(topic: Topic):
            quix_topic_name = quix_config_builder.get_topic(topic=topic)["id"]

            actual_configs = topic_admin.inspect_topics([quix_topic_name])
            topic_config = actual_configs[quix_topic_name]
            if topic_config is None:
                raise TopicNotFoundError(
                    f'Topic "{quix_topic_name}" not found on the broker'
                )

            topic_config.extra_config["__quix_topic_name__"] = topic.name
            topic = topic.__clone__(name=quix_topic_name)
            topic.broker_config = topic_config
            return topic

        # Patch the instance of QuixTopicManager to use Kafka Admin API
        # create topics instead of Quix Portal API
        def _mock_create_topic(topic: Topic, timeout, create_timeout):
            # Get a topic "id" from the QuixKafkaConfigBuilder
            quix_response = quix_config_builder.get_topic(topic=topic)
            # Replace a topic name with "id" and create a topic in a local broker
            topic = topic.__clone__(name=quix_response["id"])
            topic_admin.create_topics(
                topics=[topic], timeout=timeout, finalize_timeout=create_timeout
            )

        patch.multiple(
            quix_topic_manager,
            default_num_partitions=1,
            default_replication_factor=1,
            _fetch_topic=_mock_fetch_topic,
            _create_topic=_mock_create_topic,
        ).start()
        return quix_topic_manager

    return factory


@pytest.fixture()
def quix_app_factory(
    random_consumer_group,
    kafka_container,
    tmp_path,
    topic_admin,
    quix_mock_config_builder_factory,
    quix_topic_manager_factory,
    store_type,
):
    """
    For doing testing with Quix Applications against a local cluster.

    Almost all behavior is standard, except the quix_config_builder is mocked out, and
    thus topic creation is handled with the TopicAdmin client.
    """

    def factory(
        auto_offset_reset: AutoOffsetReset = "latest",
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        state_dir: Optional[str] = None,
        auto_create_topics: bool = True,
        use_changelog_topics: bool = True,
        workspace_id: str = "my_ws",
        store_type: Optional[StoreTypes] = store_type,
        topic_manager: Optional[QuixTopicManager] = None,
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
    ) -> Application:
        state_dir = state_dir or (tmp_path / "state").absolute()
        if bool(topic_manager) ^ bool(quix_config_builder):
            raise ValueError(
                "Should provide both QuixTopicManager AND QuixKafkaConfigBuilder with "
                "corresponding workspace_id, or neither."
            )
        topic_manager = topic_manager or quix_topic_manager_factory(
            workspace_id=workspace_id, consumer_group=random_consumer_group
        )
        quix_config_builder = quix_config_builder or quix_mock_config_builder_factory(
            workspace_id=workspace_id
        )

        return Application(
            consumer_group=random_consumer_group,
            state_dir=state_dir,
            auto_offset_reset=auto_offset_reset,
            consumer_extra_config=consumer_extra_config,
            producer_extra_config=producer_extra_config,
            on_consumer_error=on_consumer_error,
            on_producer_error=on_producer_error,
            on_processing_error=on_processing_error,
            on_message_processed=on_message_processed,
            auto_create_topics=auto_create_topics,
            use_changelog_topics=use_changelog_topics,
            topic_manager=topic_manager,
            quix_config_builder=quix_config_builder,
        )

    with patch(
        "quixstreams.state.manager.StateStoreManager.default_store_type",
        new_callable=PropertyMock,
    ) as m:
        m.return_value = store_type
        yield factory


@pytest.fixture()
def message_context_factory():
    def factory(
        topic: str = "test",
    ) -> MessageContext:
        return MessageContext(
            topic=topic,
            partition=0,
            offset=0,
            size=0,
        )

    return factory


@pytest.fixture()
def topic_admin(kafka_container):
    t = TopicAdmin(broker_address=kafka_container.broker_address)
    t.admin_client  # init the underlying admin so mocks can be applied whenever
    return t


@pytest.fixture()
def topic_manager_factory(topic_admin, random_consumer_group):
    """
    TopicManager with option to add an TopicAdmin (which uses Kafka Broker)
    """

    def factory(
        topic_admin_: Optional[TopicAdmin] = None,
        consumer_group: str = random_consumer_group,
        timeout: float = 10,
        create_timeout: float = 20,
        auto_create_topics: bool = True,
    ) -> TopicManager:
        return TopicManager(
            topic_admin=topic_admin_ or topic_admin,
            consumer_group=consumer_group,
            timeout=timeout,
            create_timeout=create_timeout,
            auto_create_topics=auto_create_topics,
        )

    return factory


@pytest.fixture()
def topic_manager_topic_factory(topic_manager_factory):
    """
    Uses TopicManager to generate a Topic, create it, and return the Topic object
    """

    def factory(
        name: Optional[str] = None,
        partitions: int = 1,
        use_serdes_nones: bool = False,
        key_serializer: Optional[Union[Serializer, str]] = None,
        value_serializer: Optional[Union[Serializer, str]] = None,
        key_deserializer: Optional[Union[Deserializer, str]] = None,
        value_deserializer: Optional[Union[Deserializer, str]] = None,
        timestamp_extractor: Optional[TimestampExtractor] = None,
        topic_manager: Optional[TopicManager] = None,
    ) -> Topic:
        name = name or str(uuid.uuid4())
        topic_manager = topic_manager or topic_manager_factory()
        topic_args = {
            "key_serializer": key_serializer,
            "value_serializer": value_serializer,
            "key_deserializer": key_deserializer,
            "value_deserializer": value_deserializer,
            "create_config": topic_manager.topic_config(num_partitions=partitions),
            "timestamp_extractor": timestamp_extractor,
        }
        if not use_serdes_nones:
            # will use the topic manager serdes defaults rather than "Nones"
            topic_args = {k: v for k, v in topic_args.items() if v is not None}
        topic = topic_manager.topic(name, **topic_args)
        return topic

    return factory


@pytest.fixture
def get_output(capsys) -> Callable[[], str]:
    def _get_output() -> str:
        # Strip ANSI escape codes from the output
        output = capsys.readouterr().out
        return re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", output)

    return _get_output
