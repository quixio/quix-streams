from unittest.mock import create_autospec

import pytest

from quixstreams.kafka.admin import Admin
from quixstreams.topic_manager import TopicConfig
from quixstreams.models.serializers import BytesSerializer, BytesDeserializer


class TestTopicManager:
    def test_topic_config(self, topic_manager_factory):
        """
        `TopicConfig` is created with expected defaults where necessary.
        """
        topic_manager = topic_manager_factory()
        topic_manager._topic_extra_config_defaults = {
            "a.config": "a_default",
            "another.config": "value",
        }
        topic_configs = topic_manager.topic_config(
            num_partitions=5, extra_config={"a.config": "woo"}
        )

        assert topic_configs.num_partitions == 5
        assert topic_configs.replication_factor == 1
        assert topic_configs.extra_config == {
            "a.config": "woo",
            "another.config": "value",
        }

    def test_topic_with_config(self, topic_manager_factory):
        """
        `Topic` is created with expected passed `TopicConfig` and added to the list
        of topics stored on the `TopicManager`.
        """
        topic_manager = topic_manager_factory()
        topic_name = "my_topic"
        extras = {"a_config": "woo"}
        topic_partitions = 5
        topic_replication = 5
        topic = topic_manager.topic(
            name=topic_name,
            config=TopicConfig(
                num_partitions=topic_partitions,
                replication_factor=topic_replication,
                extra_config=extras,
            ),
        )

        assert topic_manager.topics[topic_name] == topic

        assert topic.name == topic_name
        assert topic.config.num_partitions == topic_partitions
        assert topic.config.replication_factor == topic_replication
        assert topic.config.extra_config == extras

    def test_topic_no_config(self, topic_manager_factory):
        """
        `Topic` is created with expected passed config.
        """
        topic_manager = topic_manager_factory()
        topic_name = "my_topic"
        topic = topic_manager.topic(name=topic_name)

        assert topic.name == topic_name
        assert topic.config.num_partitions == topic_manager._topic_partitions
        assert topic.config.replication_factor == topic_manager._topic_replication

    def test_topic_no_auto_create_config(self, topic_manager_factory):
        """
        `Topic` is created with expected passed config.
        """
        topic_manager = topic_manager_factory()
        topic_name = "my_topic"
        topic = topic_manager.topic(name=topic_name, auto_create_config=False)

        assert topic.name == topic_name
        assert topic.config is None

    def test_changelog_topic(self, topic_manager_factory):
        """
        A changelog `Topic` is created with settings that match the source `Topic`
        and is added to the changelog topic list stored on the `TopicManager`.
        """

        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name="my_topic",
            config=topic_manager.topic_config(num_partitions=5),
        )

        suffix = "default"
        group = "my_consumer_group"
        changelog = topic_manager.changelog_topic(
            source_topic_name=topic.name,
            suffix=suffix,
            consumer_group=group,
        )

        assert topic_manager.changelog_topics[topic.name][suffix] == changelog

        assert changelog.name == topic_manager._format_changelog_name(
            group, topic.name, suffix
        )
        for attr in [
            "_key_serializer",
            "_value_serializer",
        ]:
            assert isinstance(getattr(changelog, attr), BytesSerializer)
        for attr in ["_key_deserializer", "_value_deserializer"]:
            assert isinstance(getattr(changelog, attr), BytesDeserializer)
        assert changelog.config.num_partitions == topic.config.num_partitions
        assert changelog.config.replication_factor == topic.config.replication_factor
        assert changelog.config.extra_config["cleanup.policy"] == "compact"

    def test_changelog_topic_settings_import(self, topic_manager_factory):
        """
        A changelog `Topic` only imports specified extra_configs from source `Topic`.
        """

        topic_manager = topic_manager_factory()
        topic_manager._changelog_extra_config_imports_defaults = {"import.this"}
        topic = topic_manager.topic(
            name="my_topic",
            config=topic_manager.topic_config(
                extra_config={"import.this": "different", "ignore.this": "woo"}
            ),
        )
        changelog = topic_manager.changelog_topic(
            source_topic_name=topic.name,
            suffix="default",
            consumer_group="my_consumer_group",
        )

        assert "import.this" in changelog.config.extra_config
        assert "ignore.this" not in changelog.config.extra_config

    def test_changelog_topic_source_exists_in_cluster(
        self, topic_manager_factory, admin, topic_factory
    ):
        """
        `TopicConfig` is inferred from the cluster topic metadata rather than the
        source `Topic` object if the topic already exists AND an `Admin` is provided.
        """

        topic_manager = topic_manager_factory(admin=admin)
        topic_manager._changelog_extra_config_imports_defaults = {"ignore.this"}
        topic_name, partitions = topic_factory(num_partitions=5, timeout=15)

        topic = topic_manager.topic(
            name=topic_name,
            config=topic_manager.topic_config(
                num_partitions=1,
                extra_config={"ignore.this": "not.set.on.cluster.topic.so.ignore"},
            ),
        )
        changelog = topic_manager.changelog_topic(
            source_topic_name=topic.name,
            suffix="default",
            consumer_group="my_consumer_group",
        )

        assert changelog.config.num_partitions == partitions == 5
        assert "ignore.this" not in changelog.config.extra_config

    def test_create_topics(self, topic_manager_factory):
        admin = create_autospec(Admin)
        topic_manager = topic_manager_factory(admin=admin)
        topics = [topic_manager.topic(name=n) for n in ["topic1", "topic2"]]
        topic_manager.create_topics(topics)

        admin.create_topics.assert_called_with(
            topics, timeout=topic_manager._create_timeout
        )

    def test_create_topics_invalid_config(self, topic_manager_factory):
        admin = create_autospec(Admin)
        topic_manager = topic_manager_factory(admin=admin)
        topics = [topic_manager.topic(name="topic1", auto_create_config=False)]

        with pytest.raises(ValueError):
            topic_manager.create_topics(topics)
        admin.create_topics.assert_not_called()

    def test_validate_topics_exists(self, topic_manager_factory):
        admin = create_autospec(Admin)
        topic_manager = topic_manager_factory(admin=admin)
        topics = [
            topic_manager.topic(
                name=n,
                config=topic_manager.topic_config(
                    num_partitions=2, extra_config={"my.setting": "woo"}
                ),
            )
            for n in ["topic1", "topic2", "topic3"]
        ]
        admin.inspect_topics.return_value = {
            "topic1": topic_manager.topic_config(
                num_partitions=5, extra_config=topics[0].config.extra_config
            ),
            "topic2": topic_manager.topic_config(
                num_partitions=5, extra_config={"my.setting": "derp"}
            ),
            "topic3": topics[2].config,
        }

        topic_manager.validate_topics(topics=topics, validation_level="exists")

    def test_validate_topics_exists_fails(self, topic_manager_factory):
        admin = create_autospec(Admin)
        topic_manager = topic_manager_factory(admin=admin)
        topics = [
            topic_manager.topic(
                name=n,
                config=topic_manager.topic_config(
                    num_partitions=2, extra_config={"my.setting": "woo"}
                ),
            )
            for n in ["topic1", "topic2", "topic3"]
        ]
        admin.inspect_topics.return_value = {
            "topic1": None,
            "topic2": topic_manager.topic_config(
                num_partitions=5, extra_config={"my.setting": "derp"}
            ),
            "topic3": topics[2].config,
        }

        with pytest.raises(topic_manager.TopicValidationError) as e:
            topic_manager.validate_topics(topics=topics, validation_level="exists")
        assert "topic1" in e.value.args[0]

    def test_validate_topics_required(self, topic_manager_factory):
        admin = create_autospec(Admin)
        topic_manager = topic_manager_factory(admin=admin)
        topics = [
            topic_manager.topic(
                name=n,
                config=topic_manager.topic_config(extra_config={"my.setting": "woo"}),
            )
            for n in ["topic1", "topic2", "topic3"]
        ]
        admin.inspect_topics.return_value = {
            "topic1": topics[0].config,
            "topic2": topic_manager.topic_config(extra_config={"my.setting": "derp"}),
            "topic3": topics[2].config,
        }
        topic_manager.validate_topics(topics=topics, validation_level="required")

    def test_validate_topics_required_fails(self, topic_manager_factory):
        admin = create_autospec(Admin)
        topic_manager = topic_manager_factory(admin=admin)
        topics = [
            topic_manager.topic(
                name=n,
                config=topic_manager.topic_config(
                    num_partitions=2, extra_config={"my.setting": "woo"}
                ),
            )
            for n in ["topic1", "topic2", "topic3"]
        ]
        admin.inspect_topics.return_value = {
            "topic1": topic_manager.topic_config(
                num_partitions=5, extra_config=topics[0].config.extra_config
            ),
            "topic2": topic_manager.topic_config(
                num_partitions=5, extra_config={"my.setting": "derp"}
            ),
            "topic3": topics[2].config,
        }

        with pytest.raises(topic_manager.TopicValidationError) as e:
            topic_manager.validate_topics(topics=topics, validation_level="required")
        for topic in ["topic1", "topic2"]:
            assert topic in e.value.args[0]

    def test_validate_topics_all(self, topic_manager_factory):
        admin = create_autospec(Admin)
        topic_manager = topic_manager_factory(admin=admin)
        topics = [
            topic_manager.topic(
                name=n,
                config=topic_manager.topic_config(extra_config={"my.setting": "woo"}),
            )
            for n in ["topic1", "topic2", "topic3"]
        ]
        admin.inspect_topics.return_value = topics
        topic_manager.validate_topics(topics=topics, validation_level="all")

    def test_validate_topics_all_fails(self, topic_manager_factory):
        admin = create_autospec(Admin)
        topic_manager = topic_manager_factory(admin=admin)
        topics = [
            topic_manager.topic(
                name=n,
                config=topic_manager.topic_config(
                    num_partitions=2, extra_config={"my.setting": "woo"}
                ),
            )
            for n in ["topic1", "topic2", "topic3"]
        ]
        admin.inspect_topics.return_value = {
            "topic1": topic_manager.topic_config(
                num_partitions=5, extra_config=topics[0].config.extra_config
            ),
            "topic2": topic_manager.topic_config(
                num_partitions=topics[2].config.num_partitions,
                extra_config={"my.setting": "derp"},
            ),
            "topic3": topics[2].config,
        }

        with pytest.raises(topic_manager.TopicValidationError) as e:
            topic_manager.validate_topics(topics=topics, validation_level="all")
        for topic in ["topic1", "topic2"]:
            assert topic in e.value.args[0]
