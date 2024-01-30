class TestQuixTopicManager:
    def test_quix_topic_name(self, quix_topic_manager_factory):
        topic_name = "my_topic"
        workspace_id = "my_wid"
        expected = f"{workspace_id}-{topic_name}"
        topic_manager = quix_topic_manager_factory(workspace_id=workspace_id)

        # should be the same regardless of workspace_id being included
        assert topic_manager.topic(topic_name).name == expected
        assert topic_manager.topic(expected).name == expected

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
                topic_name=topic_name, suffix=store_name, consumer_group=consumer_id
            ).name
            == expected
        )

        # also works with WID's appended in
        changelog = topic_manager.changelog_topic(
            topic_name=topic.name,
            suffix=store_name,
            consumer_group=f"{workspace_id}-{consumer_id}",
        )
        assert changelog.name == expected

        assert topic_manager.changelog_topics[topic.name][store_name] == changelog
