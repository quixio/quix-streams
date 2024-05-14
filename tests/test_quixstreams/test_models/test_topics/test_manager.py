from unittest.mock import create_autospec

import pytest

from quixstreams.models.serializers import BytesSerializer, BytesDeserializer
from quixstreams.models.topics import TopicConfig, TopicAdmin
from quixstreams.models.topics.exceptions import (
    TopicNameLengthExceeded,
    TopicNotFoundError,
    TopicConfigurationMismatch,
)


@pytest.fixture()
def topic_admin_mock():
    return create_autospec(TopicAdmin)


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
        assert topic_configs.extra_config == {"a.config": "woo"}

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

    def test_changelog_topic(self, topic_manager_factory):
        """
        A changelog `Topic` is created with settings that match the source `Topic`
        and is added to the changelog topic list stored on the `TopicManager`.
        """

        group = "my_consumer_group"
        topic_manager = topic_manager_factory(consumer_group=group)
        topic = topic_manager.topic(
            name="my_topic",
            config=topic_manager.topic_config(num_partitions=5),
        )

        store_name = "default"
        changelog = topic_manager.changelog_topic(
            topic_name=topic.name,
            store_name=store_name,
        )

        assert topic_manager.changelog_topics[topic.name][store_name] == changelog

        assert changelog.name == f"changelog__{group}--{topic.name}--{store_name}"

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
            topic_name=topic.name,
            store_name="default",
        )

        assert "import.this" in changelog.config.extra_config
        assert "ignore.this" not in changelog.config.extra_config

    def test_changelog_topic_source_exists_in_cluster(
        self, topic_manager_factory, topic_factory
    ):
        """
        `TopicConfig` is inferred from the cluster topic metadata rather than the
        source `Topic` object if the topic already exists AND an `TopicAdmin` is provided.
        """

        topic_manager = topic_manager_factory()
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
            topic_name=topic.name,
            store_name="default",
        )

        assert changelog.config.num_partitions == partitions == 5
        assert "ignore.this" not in changelog.config.extra_config

    def test_create_all_topics(self, topic_manager_factory, topic_admin_mock):
        topic_manager = topic_manager_factory(topic_admin_mock)
        topics = [topic_manager.topic(name=n) for n in ["topic1", "topic2"]]
        topic_manager.create_topics(topics)

        topic_admin_mock.create_topics.assert_called_with(
            topics, timeout=topic_manager._create_timeout
        )

    def test_validate_all_topics(self, topic_manager_factory, topic_admin_mock):
        """
        Validation succeeds, even when a source topic's Topic.config or extra_config
        differs from its kafka topic settings.
        """
        topic_manager = topic_manager_factory(topic_admin_mock)
        topics = [
            topic_manager.topic(
                name=f"topic{n}",
                config=topic_manager.topic_config(
                    num_partitions=n, extra_config={"my.setting": "woo"}
                ),
            )
            for n in range(3)
        ]
        changelogs = [
            topic_manager.changelog_topic(topic_name=topic_name, store_name="default")
            for topic_name in topic_manager.topics
        ]
        topic_admin_mock.inspect_topics.return_value = {
            topics[0].name: topics[0].config,
            topics[1].name: topics[0].config,
            topics[2].name: topic_manager.topic_config(
                extra_config={"my.setting": "derp"}
            ),
            **{changelog.name: changelog.config for changelog in changelogs},
        }
        topic_manager.validate_all_topics()

    def test_validate_all_topics_topic_not_found(
        self, topic_manager_factory, topic_admin_mock
    ):
        """
        Source topics and changelogs fail validation when missing.
        """
        topic_manager = topic_manager_factory(topic_admin_mock)
        topic = topic_manager.topic(
            name="topic",
            config=topic_manager.topic_config(),
        )
        topic_admin_mock.inspect_topics.return_value = {
            topic.name: None,
        }

        with pytest.raises(TopicNotFoundError):
            topic_manager.validate_all_topics()

    def test_validate_all_topics_changelog_partition_count_mismatch(
        self, topic_manager_factory, topic_admin_mock
    ):
        """
        Changelog topic validation must fail if it has a different number of partitions
        than the source topic.
        """
        source_topic_config = TopicConfig(num_partitions=2, replication_factor=1)
        topic_admin_mock.inspect_topics.return_value = {
            "topic": source_topic_config,
        }
        topic_manager = topic_manager_factory(topic_admin_mock)
        source_topic = topic_manager.topic(
            name="topic",
            config=source_topic_config,
        )

        changelog_topic = topic_manager.changelog_topic(
            topic_name=source_topic.name, store_name="default"
        )
        topic_admin_mock.inspect_topics.return_value = {
            source_topic.name: source_topic_config,
            changelog_topic.name: topic_manager.topic_config(
                num_partitions=500,
            ),
        }

        with pytest.raises(TopicConfigurationMismatch, match="Invalid partition count"):
            topic_manager.validate_all_topics()

    def test_validate_all_topics_changelog_replication_factor_mismatch(
        self, topic_manager_factory, topic_admin_mock
    ):
        """
        Changelog topic validation must fail if it has a different replication factor
        than the source topic.
        """
        source_topic_config = TopicConfig(num_partitions=2, replication_factor=2)
        topic_admin_mock.inspect_topics.return_value = {
            "topic": source_topic_config,
        }
        topic_manager = topic_manager_factory(topic_admin_mock)
        source_topic = topic_manager.topic(
            name="topic",
            config=source_topic_config,
        )

        changelog_topic = topic_manager.changelog_topic(
            topic_name=source_topic.name,
            store_name="default",
        )

        topic_admin_mock.inspect_topics.return_value = {
            source_topic.name: source_topic.config,
            changelog_topic.name: topic_manager.topic_config(
                num_partitions=2, replication_factor=1
            ),
        }

        with pytest.raises(
            TopicConfigurationMismatch, match="Invalid replication factor"
        ):
            topic_manager.validate_all_topics()

    def test_topic_name_len_exceeded(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        bad_name = "a" * 300

        with pytest.raises(TopicNameLengthExceeded):
            topic_manager.topic(bad_name)

    def test_changelog_name_len_exceeded(self, topic_manager_factory):
        topic_manager = topic_manager_factory()

        topic = topic_manager.topic("good_name")
        with pytest.raises(TopicNameLengthExceeded):
            topic_manager.changelog_topic(
                topic_name=topic.name, store_name="store" * 100
            )

    def test_repartition_topic(self, topic_manager_factory):
        """
        A repartition `Topic` is created with settings that match the source `Topic`
        and is added to the repartition topic list stored on the `TopicManager`.
        """
        group = "my_consumer_group"
        topic_manager = topic_manager_factory(consumer_group=group)
        topic = topic_manager.topic(
            name="my_topic",
            config=topic_manager.topic_config(num_partitions=5),
        )

        operation = "my_op"
        repartition = topic_manager.repartition_topic(
            operation=operation,
            topic_name=topic.name,
            key_serializer="bytes",
            value_serializer="bytes",
        )

        assert topic_manager.repartition_topics[repartition.name] == repartition
        assert repartition.name == f"repartition__{group}--{topic.name}--{operation}"
        assert repartition.config.num_partitions == topic.config.num_partitions
        assert repartition.config.replication_factor == topic.config.replication_factor

    def test_changelog_nested_internal_topic_naming(self, topic_manager_factory):
        """
        Confirm expected formatting for an internal topic that spawns another internal
        topic (changelog)
        """
        store = "my_store"
        group = "my_consumer_group"
        topic_manager = topic_manager_factory(consumer_group=group)
        topic = topic_manager.topic(
            name="my_topic",
            config=topic_manager.topic_config(num_partitions=5),
        )

        operation = "my_op"
        repartition = topic_manager.repartition_topic(
            operation=operation,
            topic_name=topic.name,
            key_serializer="bytes",
            value_serializer="bytes",
        )
        changelog = topic_manager.changelog_topic(repartition.name, store)

        assert (
            changelog.name
            == f"changelog__{group}--repartition.{topic.name}.{operation}--{store}"
        )
