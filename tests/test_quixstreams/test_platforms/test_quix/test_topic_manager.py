import pytest

from quixstreams.models import TopicConfig


class TestQuixTopicManager:
    def test_quix_topic(self, quix_topic_manager_factory):
        """
        Topic name should be "id" field from the Quix API get_topic result if found
        """
        topic_name = "my_topic"
        expected_topic_id = f"my_ws-{topic_name}"

        create_config = TopicConfig(num_partitions=5, replication_factor=1)
        topic_manager = quix_topic_manager_factory()
        topic = topic_manager.topic(topic_name, create_config=create_config)
        assert topic.name == expected_topic_id
        assert topic.quix_name == topic_name
        assert topic_manager.topics[topic.name] == topic
        assert topic.broker_config.num_partitions == create_config.num_partitions
        assert (
            topic.broker_config.replication_factor == create_config.replication_factor
        )

    def test_quix_internal_topic(self, quix_topic_manager_factory):
        consumer_group = "my_group"
        store_name = "default"
        topic_name = "my_topic"
        workspace_id = "workspace_id"

        expected_topic_id = f"{workspace_id}-{topic_name}"
        changelog_name = f"changelog__{consumer_group}--{topic_name}--{store_name}"
        expected_changelog_id = f"{workspace_id}-{changelog_name}"

        topic_manager = quix_topic_manager_factory(
            workspace_id=workspace_id, consumer_group=consumer_group
        )
        create_config = TopicConfig(num_partitions=5, replication_factor=1)
        topic = topic_manager.topic(topic_name, create_config=create_config)
        assert topic.name == expected_topic_id

        changelog = topic_manager.changelog_topic(
            stream_id=topic.name, store_name=store_name, config=topic.broker_config
        )
        assert changelog.name == expected_changelog_id
        assert topic_manager.changelog_topics[topic.name][store_name] == changelog

        assert (
            changelog.create_config.num_partitions == topic.broker_config.num_partitions
        )
        assert (
            changelog.create_config.replication_factor
            == topic.broker_config.replication_factor
        )
        assert (
            changelog.broker_config.num_partitions == topic.broker_config.num_partitions
        )
        assert (
            changelog.broker_config.replication_factor
            == topic.broker_config.replication_factor
        )

    def test_quix_changelog_nested_internal_topic_naming(
        self, quix_topic_manager_factory
    ):
        """
        Confirm expected formatting for an internal topic that spawns another internal
        topic (changelog)
        """

        workspace_id = "workspace_id"
        store_name = "my_store"
        consumer_group = "consumer_group"
        operation = "sum"
        topic_name = "topic"
        topic_id = f"{workspace_id}-{topic_name}"
        changelog_name = f"changelog__{consumer_group}--{topic_name}--{store_name}"
        repartition_name = f"repartition__{consumer_group}--{topic_name}--{operation}"
        repartition_id = f"{workspace_id}-{repartition_name}"
        changelog_name = f"changelog__{consumer_group}--repartition.{topic_name}.{operation}--{store_name}"
        changelog_topic_id = f"{workspace_id}-{changelog_name}"

        topic_manager = quix_topic_manager_factory(
            workspace_id=workspace_id, consumer_group=consumer_group
        )

        create_config = TopicConfig(num_partitions=5, replication_factor=1)
        topic = topic_manager.topic(topic_name, create_config=create_config)
        assert topic.name == topic_id

        repartition = topic_manager.repartition_topic(
            operation=operation, stream_id=topic.name, config=topic.broker_config
        )
        assert repartition.name == repartition_id
        assert topic_manager.repartition_topics[repartition.name] == repartition

        changelog = topic_manager.changelog_topic(
            stream_id=repartition.name,
            store_name=store_name,
            config=repartition.broker_config,
        )
        assert changelog.name == changelog_topic_id
        assert topic_manager.changelog_topics[repartition.name][store_name] == changelog

        assert (
            changelog.create_config.num_partitions
            == repartition.broker_config.num_partitions
        )
        assert (
            changelog.create_config.replication_factor
            == repartition.broker_config.replication_factor
        )
        assert (
            changelog.broker_config.num_partitions
            == repartition.broker_config.num_partitions
        )
        assert (
            changelog.broker_config.replication_factor
            == repartition.broker_config.replication_factor
        )

    def test_stream_id_from_topics_multiple_topics_success(
        self, quix_topic_manager_factory
    ):
        topic_manager = quix_topic_manager_factory(workspace_id="workspace_id")
        topic1 = topic_manager.topic("test1")
        topic2 = topic_manager.topic("test2")
        stream_id = topic_manager.stream_id_from_topics([topic1, topic2])

        assert stream_id == "test1--test2"

    def test_stream_id_from_topics_single_topic_prefixed_with_workspace(
        self, quix_topic_manager_factory
    ):
        """
        Test that stream_id is prefixed with workspace_id if the single topic is passed
        for the backwards compatibility.
        """
        topic_manager = quix_topic_manager_factory(workspace_id="workspace_id")
        topic1 = topic_manager.topic("test1")
        stream_id = topic_manager.stream_id_from_topics([topic1])

        assert stream_id == "workspace_id-test1"

    def test_stream_id_from_topics_no_topics_fails(self, quix_topic_manager_factory):
        topic_manager = quix_topic_manager_factory()
        with pytest.raises(ValueError):
            topic_manager.stream_id_from_topics([])
