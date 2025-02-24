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
        assert topic_manager.topics[topic.name] == topic
        assert topic.real_config.num_partitions == create_config.num_partitions
        assert topic.real_config.replication_factor == create_config.replication_factor

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
            topic_name=topic.name, store_name=store_name
        )
        assert changelog.name == expected_changelog_id
        assert topic_manager.changelog_topics[topic.name][store_name] == changelog

        assert (
            changelog.create_config.num_partitions == topic.real_config.num_partitions
        )
        assert (
            changelog.create_config.replication_factor
            == topic.real_config.replication_factor
        )
        assert changelog.real_config.num_partitions == topic.real_config.num_partitions
        assert (
            changelog.real_config.replication_factor
            == topic.real_config.replication_factor
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

        repartition = topic_manager.repartition_topic(operation, topic.name)
        assert repartition.name == repartition_id
        assert topic_manager.repartition_topics[repartition.name] == repartition

        changelog = topic_manager.changelog_topic(repartition.name, store_name)
        assert changelog.name == changelog_topic_id
        assert topic_manager.changelog_topics[repartition.name][store_name] == changelog

        assert (
            changelog.create_config.num_partitions
            == repartition.real_config.num_partitions
        )
        assert (
            changelog.create_config.replication_factor
            == repartition.real_config.replication_factor
        )
        assert (
            changelog.real_config.num_partitions
            == repartition.real_config.num_partitions
        )
        assert (
            changelog.real_config.replication_factor
            == repartition.real_config.replication_factor
        )
