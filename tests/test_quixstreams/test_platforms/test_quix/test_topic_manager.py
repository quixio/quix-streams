from unittest.mock import MagicMock

from quixstreams.models import TopicAdmin, TopicConfig
from quixstreams.platforms.quix import QuixTopicManager


class TestQuixTopicManager:
    def test_quix_topic_name_found(self, quix_mock_config_builder_factory):
        """
        Topic name should be "id" field from the Quix API get_topic result if found
        """
        topic_name = "my_topic"
        workspace_id = "my_wid"
        expected_name = "quix_topic_id"

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        config_builder.get_topic.side_effect = lambda _: {"id": expected_name}

        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group="test_group",
            quix_config_builder=config_builder,
        )
        # Check that topic name is prefixed by the workspace ID
        # when only the "name" part is provided
        assert topic_manager.topic(topic_name).name == expected_name

        topic = topic_manager.topic(expected_name)
        # Replication factor and num partitions should be None by default
        assert topic.config.replication_factor is None
        assert topic.config.num_partitions is None

    def test_quix_topic_name_not_found(self, quix_mock_config_builder_factory):
        """
        Workspace-appended name is returned when config builder returns None
        """
        topic_name = "my_topic"
        workspace_id = "my_wid"

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        config_builder.get_topic.side_effect = lambda _: None
        expected_name = config_builder.prepend_workspace_id(topic_name)

        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group="test_group",
            quix_config_builder=config_builder,
        )

        # Check that topic name is prefixed by the workspace ID
        # when only the "name" part is provided
        assert topic_manager.topic(topic_name).name == expected_name

        topic = topic_manager.topic(expected_name)
        # Replication factor and num partitions should be None by default
        assert topic.config.replication_factor is None
        assert topic.config.num_partitions is None

    def test_quix_topic_name(self, quix_mock_config_builder_factory):
        """
        Create a Topic object with same name regardless of workspace prefixes
        in the topic name
        """
        topic_name = "my_topic"
        workspace_id = "my_wid"
        expected_topic_name = f"{workspace_id}-{topic_name}"

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group="test_group",
            quix_config_builder=config_builder,
        )

        assert topic_manager.topic(topic_name).name == expected_topic_name
        assert topic_manager.topic(expected_topic_name).name == expected_topic_name

    def test_quix_changelog_topic(self, quix_mock_config_builder_factory):
        topic_name = "my_topic"
        workspace_id = "my_wid"
        consumer_group = "my_group"
        store_name = "default"
        expected = (
            f"{workspace_id}-changelog__{consumer_group}--{topic_name}--{store_name}"
        )

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group=consumer_group,
            quix_config_builder=config_builder,
        )
        topic = topic_manager.topic(topic_name)
        changelog = topic_manager.changelog_topic(
            topic_name=topic_name, store_name=store_name
        )

        assert changelog.name == expected
        assert topic_manager.changelog_topics[topic.name][store_name] == changelog

    def test_quix_changelog_topic_workspace_prepend(
        self, quix_mock_config_builder_factory
    ):
        """
        Changelog Topic name is the same regardless of workspace prefixes
        in the topic name and/or consumer group

        NOTE: the "topic_name" handed to TopicManager.changelog() should always contain
        the prefix based on where it will be called, but it can handle if it doesn't.
        """
        topic_name = "my_topic"
        workspace_id = "my_wid"
        appended_topic_name = f"{workspace_id}-{topic_name}"
        consumer_group = "my_group"
        store_name = "default"
        expected = (
            f"{workspace_id}-changelog__{consumer_group}--{topic_name}--{store_name}"
        )

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group=f"{workspace_id}-{consumer_group}",
            quix_config_builder=config_builder,
        )
        topic = topic_manager.topic(appended_topic_name)
        changelog = topic_manager.changelog_topic(
            topic_name=appended_topic_name, store_name=store_name
        )

        assert changelog.name == expected
        assert topic.name == appended_topic_name
        assert topic_manager.changelog_topics[topic.name][store_name] == changelog

    def test_quix_changelog_nested_internal_topic_naming(
        self, quix_mock_config_builder_factory
    ):
        """
        Confirm expected formatting for an internal topic that spawns another internal
        topic (changelog)
        """
        topic_name = "my_topic"
        workspace_id = "my_wid"
        store = "my_store"
        consumer_group = "my_consumer_group"
        operation = "my_op"
        expected_topic_name = (
            f"{workspace_id}-changelog__{consumer_group}--"
            f"repartition.{topic_name}.{operation}--{store}"
        )

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group=consumer_group,
            quix_config_builder=config_builder,
        )
        topic = topic_manager.topic(name=topic_name)
        repartition = topic_manager.repartition_topic(operation, topic.name)
        changelog = topic_manager.changelog_topic(repartition.name, store)

        assert changelog.name == expected_topic_name

    def test_quix_topic_custom_config(self, quix_mock_config_builder_factory):
        topic_name = "my_topic"
        workspace_id = "my_wid"

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group="test_group",
            quix_config_builder=config_builder,
        )

        config = TopicConfig(num_partitions=2, replication_factor=2)
        topic = topic_manager.topic(topic_name, config=config)

        assert topic.config.replication_factor == config.replication_factor
        assert topic.config.num_partitions == config.num_partitions
