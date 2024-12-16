from typing import Optional
from unittest.mock import PropertyMock, create_autospec, patch

import pytest

from quixstreams.app import Application, MessageProcessedCallback
from quixstreams.error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
)
from quixstreams.kafka import AutoOffsetReset
from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.platforms.quix import QuixTopicManager
from quixstreams.platforms.quix.config import (
    QuixApplicationConfig,
    QuixKafkaConfigsBuilder,
    prepend_workspace_id,
    strip_workspace_id_prefix,
)
from quixstreams.state.manager import StoreTypes


@pytest.fixture()
def quix_mock_config_builder_factory(kafka_container):
    def factory(workspace_id: Optional[str] = None):
        if not workspace_id:
            workspace_id = "my_ws"
        cfg_builder = create_autospec(QuixKafkaConfigsBuilder)
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

        cfg_builder.convert_topic_response.side_effect = (
            lambda topic: QuixKafkaConfigsBuilder.convert_topic_response(topic)
        )

        # Mock the create API call and return this response.
        # Doing it this way keeps the old behavior where topics are only created
        # when the app is actually run (for tests, at least).
        # This does simulate an expected topic name with prepended WID which may not
        # always be true, but it's just to make testing easier.
        cfg_builder.get_or_create_topic.side_effect = lambda topic, timeout=None: {
            "id": f"{workspace_id}-{topic.name}",
            "name": topic.name,
            "configuration": {
                "partitions": topic.config.num_partitions,
                "replicationFactor": topic.config.replication_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Delete",
            },
        }

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
        topic_manager = topic_manager_factory(
            topic_admin_=topic_admin, consumer_group=consumer_group
        )

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
        # create topics instead of Quix Portal API
        patch.multiple(
            quix_topic_manager,
            default_num_partitions=1,
            default_replication_factor=1,
            _create_topics=topic_manager.create_topics,
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
            topic_manager=topic_manager
            or quix_topic_manager_factory(workspace_id=workspace_id),
            quix_config_builder=quix_config_builder
            or quix_mock_config_builder_factory(workspace_id=workspace_id),
        )

    with patch(
        "quixstreams.state.manager.StateStoreManager.default_store_type",
        new_callable=PropertyMock,
    ) as m:
        m.return_value = store_type
        yield factory
