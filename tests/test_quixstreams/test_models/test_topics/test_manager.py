import uuid

import pytest
from confluent_kafka.cimpl import NewPartitions

from quixstreams.models.serializers import BytesDeserializer, BytesSerializer
from quixstreams.models.topics import TopicConfig
from quixstreams.models.topics.exceptions import (
    TopicConfigurationMismatch,
    TopicNameLengthExceeded,
    TopicNotFoundError,
)


class TestTopicManager:
    def test_topic_config(self, topic_manager_factory):
        """
        `TopicConfig` is created with expected defaults where necessary.
        """
        topic_manager = topic_manager_factory()
        topic_manager.default_extra_config = {
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
        topic_name = str(uuid.uuid4())
        create_config = TopicConfig(
            num_partitions=5,
            replication_factor=1,
            extra_config={"retention.ms": "60"},
        )
        topic = topic_manager.topic(
            name=topic_name,
            create_config=create_config,
        )

        assert topic_manager.topics[topic_name] == topic

        assert topic.name == topic_name
        assert topic.create_config.num_partitions == create_config.num_partitions
        assert (
            topic.create_config.replication_factor == create_config.replication_factor
        )
        assert topic.create_config.extra_config == create_config.extra_config
        assert topic.broker_config.num_partitions == create_config.num_partitions
        assert (
            topic.broker_config.replication_factor == create_config.replication_factor
        )

    def test_topic_no_config(self, topic_manager_factory):
        """
        `Topic` is created with expected passed config.
        """
        topic_manager = topic_manager_factory()
        topic_name = str(uuid.uuid4())
        topic = topic_manager.topic(name=topic_name)

        assert topic.name == topic_name
        assert (
            topic.create_config.num_partitions == topic_manager.default_num_partitions
        )
        assert (
            topic.create_config.replication_factor
            == topic_manager.default_replication_factor
        )
        assert (
            topic.broker_config.num_partitions == topic_manager.default_num_partitions
        )
        assert (
            topic.broker_config.replication_factor
            == topic_manager.default_replication_factor
        )

    def test_topic_not_found(self, topic_manager_factory):
        """
        Source topics fail validation when missing.
        """
        topic_manager = topic_manager_factory(auto_create_topics=False)
        with pytest.raises(TopicNotFoundError):
            topic_manager.topic(
                name=str(uuid.uuid4()),
                create_config=topic_manager.topic_config(),
            )

    def test_changelog_topic(self, topic_manager_factory):
        """
        A changelog `Topic` is created with settings that match the source `Topic`
        and is added to the changelog topic list stored on the `TopicManager`.
        """

        group = "my_consumer_group"
        topic_manager = topic_manager_factory(consumer_group=group)
        topic = topic_manager.topic(
            name="my_topic",
            create_config=topic_manager.topic_config(num_partitions=5),
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
        assert (
            changelog.create_config.num_partitions == topic.create_config.num_partitions
        )
        assert (
            changelog.create_config.replication_factor
            == topic.create_config.replication_factor
        )

        assert topic.create_config.extra_config.get("cleanup.policy") != "compact"
        assert changelog.create_config.extra_config["cleanup.policy"] == "compact"

    def test_changelog_topic_settings_import(self, topic_manager_factory):
        """
        A changelog `Topic` only imports specified extra_configs from source `Topic`.
        """

        topic_manager = topic_manager_factory()
        extra_config = {
            "segment.ms": 1000,  # this must be ignored
            "retention.bytes": 10,  # this must be imported
        }

        topic = topic_manager.topic(
            name="my_topic",
            create_config=topic_manager.topic_config(extra_config=extra_config),
        )
        changelog = topic_manager.changelog_topic(
            topic_name=topic.name,
            store_name="default",
        )

        assert "retention.bytes" in changelog.create_config.extra_config
        assert "segment.ms" not in changelog.create_config.extra_config

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
            create_config=topic_manager.topic_config(
                num_partitions=1,
                extra_config={"segment.ms": "1000"},
            ),
        )
        changelog = topic_manager.changelog_topic(
            topic_name=topic.name,
            store_name="default",
        )

        assert changelog.create_config.num_partitions == partitions == 5
        assert "segment.ms" not in changelog.create_config.extra_config

    def test_validate_all_topics(self, topic_manager_factory):
        """
        Validation succeeds, regardless of whether a Topic.config matches its actual
        kafka settings.

        Tests a "happy path" fresh creation + validation, followed by a config update.
        """

        # happy path with new topics
        _topic_manager = topic_manager_factory()
        topic_config = _topic_manager.topic_config(
            num_partitions=5, extra_config={"max.message.bytes": "1234567"}
        )
        topic = _topic_manager.topic(name=str(uuid.uuid4()), create_config=topic_config)
        _topic_manager.changelog_topic(topic_name=topic.name, store_name="default")
        _topic_manager.validate_all_topics()

        # assume attempt to recreate topics with altered configs (it ignores them).
        topic_manager = topic_manager_factory()
        topic_updated = topic_manager.topic(
            name=topic.name,
            create_config=topic_manager.topic_config(num_partitions=20),
        )
        topic_manager.changelog_topic(
            topic_name=topic_updated.name, store_name="default"
        )

        topic_manager.validate_all_topics()

    def test_validate_all_topics_changelog_partition_count_mismatch(
        self, topic_manager_factory, kafka_admin_client
    ):
        """
        Changelog topic validation must fail if it has a different number of partitions
        than the source topic.
        """
        source_topic_config = TopicConfig(num_partitions=2, replication_factor=1)
        topic_manager = topic_manager_factory()

        # Create a source topic and a corresponding changelog topic
        source_topic = topic_manager.topic(
            name="topic",
            create_config=source_topic_config,
        )
        topic_manager.changelog_topic(
            topic_name=source_topic.name, store_name="default"
        )

        # Increase a number of source partitions
        fut = kafka_admin_client.create_partitions(
            [NewPartitions(topic=source_topic.name, new_total_count=3)]
        )
        fut[source_topic.name].result(timeout=5)

        # Re-define the topics
        source_topic = topic_manager.topic(
            name="topic",
            create_config=source_topic_config,
        )
        topic_manager.changelog_topic(
            topic_name=source_topic.name, store_name="default"
        )

        # Validation must fail because the changelog topic already exists
        # and has only 2 partitions, while the source topic was changed
        with pytest.raises(TopicConfigurationMismatch, match="partition count"):
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
            create_config=topic_manager.topic_config(num_partitions=5),
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
        assert (
            repartition.create_config.num_partitions
            == topic.create_config.num_partitions
        )
        assert (
            repartition.create_config.replication_factor
            == topic.create_config.replication_factor
        )

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
            create_config=topic_manager.topic_config(num_partitions=5),
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

    def test_non_changelog_topics(self, topic_manager_factory):
        group = "my_consumer_group"
        topic_manager = topic_manager_factory(consumer_group=group)
        data_topic = topic_manager.topic(
            name="my_topic",
            create_config=topic_manager.topic_config(num_partitions=5),
        )

        operation = "my_op"
        repartition_topic = topic_manager.repartition_topic(
            operation=operation,
            topic_name=data_topic.name,
            key_serializer="bytes",
            value_serializer="bytes",
        )

        changelog_topic = topic_manager.changelog_topic(
            topic_name=data_topic.name, store_name="default"
        )

        assert data_topic.name in topic_manager.non_changelog_topics
        assert repartition_topic.name in topic_manager.non_changelog_topics
        assert changelog_topic.name not in topic_manager.non_changelog_topics
