from quixstreams.models import TopicConfig


class TestQuixTopicManager:
    def test_quix_topic_name_found(
        self, quix_topic_manager_factory, quix_mock_config_builder_factory
    ):
        """
        Topic name should be "id" field from the Quix API get_topic result if found
        """
        topic_name = "my_topic"
        workspace_id = "my_wid"
        expected_name = "get_topic_result_id"

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        config_builder.get_topic.side_effect = lambda topic: {"id": expected_name}
        topic_manager = quix_topic_manager_factory(
            workspace_id=workspace_id, quix_config_builder=config_builder
        )

        assert topic_manager.topic(topic_name).name == expected_name
        # Replication factor should be None by default
        assert topic_manager.topic(expected_name).config.replication_factor is None

    def test_quix_topic_name_not_found(
        self, quix_topic_manager_factory, quix_mock_config_builder_factory
    ):
        """
        Workspace-appended name is returned when config builder returns None
        """
        topic_name = "my_topic"
        workspace_id = "my_wid"

        config_builder = quix_mock_config_builder_factory(workspace_id=workspace_id)
        config_builder.get_topic.side_effect = lambda topic: None
        expected_name = config_builder.prepend_workspace_id(topic_name)
        topic_manager = quix_topic_manager_factory(
            workspace_id=workspace_id, quix_config_builder=config_builder
        )

        assert topic_manager.topic(topic_name).name == expected_name
        # Replication factor should be None by default
        assert topic_manager.topic(expected_name).config.replication_factor is None

    def test_quix_changelog_topic(self, quix_topic_manager_factory):
        """
        Create a changelog Topic object with same name regardless of workspace prefixes
        in the topic name or consumer group

        NOTE: the "topic_name" handed to TopicManager.changelog() should always contain
        the prefix based on where it will be called, but it can handle if it doesn't.
        """
        topic_name = "my_topic"
        workspace_id = "my_wid"
        consumer_id = "my_group"
        store_name = "default"
        expected = (
            f"{workspace_id}-changelog__{consumer_id}--{topic_name}--{store_name}"
        )
        topic_manager = quix_topic_manager_factory(workspace_id=workspace_id)
        topic = topic_manager.topic(topic_name)

        assert (
            topic_manager.changelog_topic(
                topic_name=topic_name, store_name=store_name, consumer_group=consumer_id
            ).name
            == expected
        )

        # also works with WID's appended in
        changelog = topic_manager.changelog_topic(
            topic_name=topic.name,
            store_name=store_name,
            consumer_group=f"{workspace_id}-{consumer_id}",
        )
        assert changelog.name == expected

        assert topic_manager.changelog_topics[topic.name][store_name] == changelog

    def test_quix_topic_custom_config(self, quix_topic_manager_factory):
        topic_name = "my_topic"
        workspace_id = "my_wid"
        topic_manager = quix_topic_manager_factory(workspace_id=workspace_id)

        config = TopicConfig(num_partitions=2, replication_factor=2)

        topic = topic_manager.topic(topic_name, config=config)
        assert topic.config.replication_factor == config.replication_factor
        assert topic.config.num_partitions == config.num_partitions
