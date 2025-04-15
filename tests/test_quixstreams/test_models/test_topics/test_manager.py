import uuid

import pytest

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
        assert topic.quix_name == topic_name
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
        assert "retention.ms" in topic.broker_config.extra_config
        assert "retention.bytes" in topic.broker_config.extra_config
        assert "cleanup.policy" in topic.broker_config.extra_config

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

    def test_derive_topic_config_success(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(
                num_partitions=2,
                replication_factor=1,
                extra_config={"retention.bytes": "10000", "retention.ms": "10000"},
            ),
        )
        topic2 = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(
                num_partitions=1,
                replication_factor=1,
                extra_config={"retention.bytes": "9999", "retention.ms": "10001"},
            ),
        )

        config = topic_manager.derive_topic_config([topic1, topic2])
        assert config.num_partitions == 2
        assert config.replication_factor == 1
        assert config.extra_config == {
            "retention.bytes": "10000",
            "retention.ms": "10001",
        }

    def test_derive_topic_config_no_topics(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        with pytest.raises(ValueError, match="At least one Topic must be passed"):
            topic_manager.derive_topic_config([])

    def test_derive_topic_config_max_retention(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(
                num_partitions=2,
                replication_factor=1,
                extra_config={"retention.bytes": "-1", "retention.ms": "-1"},
            ),
        )
        topic2 = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(
                num_partitions=1,
                replication_factor=1,
                extra_config={"retention.bytes": "9999", "retention.ms": "10001"},
            ),
        )

        config = topic_manager.derive_topic_config([topic1, topic2])
        assert config.num_partitions == 2
        assert config.replication_factor == 1
        assert config.extra_config == {
            "retention.bytes": "-1",
            "retention.ms": "-1",
        }

    def test_changelog_topic(self, topic_manager_factory):
        """
        A changelog `Topic` is created with settings that match the source `Topic`
        and is added to the changelog topic list stored on the `TopicManager`.
        """

        group = "my_consumer_group"
        topic_manager = topic_manager_factory(consumer_group=group)
        store_name = "default"
        state_id = str(uuid.uuid4())
        changelog = topic_manager.changelog_topic(
            state_id=state_id,
            store_name=store_name,
            config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        assert topic_manager.changelog_topics[state_id][store_name] == changelog

        assert changelog.name == f"changelog__{group}--{state_id}--{store_name}"

        for attr in [
            "_key_serializer",
            "_value_serializer",
        ]:
            assert isinstance(getattr(changelog, attr), BytesSerializer)
        for attr in ["_key_deserializer", "_value_deserializer"]:
            assert isinstance(getattr(changelog, attr), BytesDeserializer)
        assert changelog.broker_config.num_partitions == 1
        assert changelog.broker_config.replication_factor == 1
        assert changelog.broker_config.extra_config["cleanup.policy"] == "compact"

    def test_changelog_topic_partition_count_mismatch(
        self, topic_manager_factory, kafka_admin_client
    ):
        """
        Changelog topic validation must fail if it has a different number of partitions
        than expected.

        """
        topic_manager = topic_manager_factory()

        state_id = str(uuid.uuid4())
        # Create a new changelog topic with 1 partition
        topic_manager.changelog_topic(
            state_id=state_id,
            store_name="store",
            config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        # Re-create the same changelog topic but with 2 partitions.
        # The validation must fail because the changelog topic already exists
        # and has only 1 partition
        with pytest.raises(TopicConfigurationMismatch, match="partition count"):
            topic_manager.changelog_topic(
                state_id=state_id,
                store_name="store",
                config=TopicConfig(num_partitions=2, replication_factor=1),
            )

    def test_topic_name_len_exceeded(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        bad_name = "a" * 300

        with pytest.raises(TopicNameLengthExceeded):
            topic_manager.topic(bad_name)

    def test_changelog_name_len_exceeded(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        with pytest.raises(TopicNameLengthExceeded):
            topic_manager.changelog_topic(
                state_id=str(uuid.uuid4()),
                store_name="store" * 100,
                config=TopicConfig(num_partitions=1, replication_factor=1),
            )

    def test_repartition_topic(self, topic_manager_factory):
        """
        A repartition `Topic` is created with settings that match the source `Topic`
        and is added to the repartition topic list stored on the `TopicManager`.
        """
        group = "my_consumer_group"
        topic_manager = topic_manager_factory(consumer_group=group)

        operation = "my_op"
        state_id = str(uuid.uuid4())
        repartition = topic_manager.repartition_topic(
            operation=operation,
            state_id=state_id,
            key_serializer="bytes",
            value_serializer="bytes",
            config=TopicConfig(
                num_partitions=1,
                replication_factor=1,
                extra_config={"retention.ms": "1000", "retention.bytes": "1000"},
            ),
        )

        assert topic_manager.repartition_topics[repartition.name] == repartition
        assert repartition.name == f"repartition__{group}--{state_id}--{operation}"
        assert repartition.broker_config.num_partitions == 1
        assert repartition.broker_config.replication_factor == 1
        assert repartition.broker_config.extra_config["retention.ms"] == "1000"
        assert repartition.broker_config.extra_config["retention.bytes"] == "1000"

    def test_changelog_nested_internal_topic_naming(self, topic_manager_factory):
        """
        Confirm expected formatting for an internal topic that spawns another internal
        topic (changelog)
        """
        store_name = "my_store"
        group = "my_consumer_group"
        topic_manager = topic_manager_factory(consumer_group=group)

        state_id = str(uuid.uuid4())
        operation = "my_op"
        repartition_topic = topic_manager.repartition_topic(
            operation=operation,
            state_id=state_id,
            key_serializer="bytes",
            value_serializer="bytes",
            config=TopicConfig(
                num_partitions=1,
                replication_factor=1,
            ),
        )
        changelog = topic_manager.changelog_topic(
            state_id=repartition_topic.name,
            store_name=store_name,
            config=TopicConfig(
                num_partitions=1,
                replication_factor=1,
            ),
        )

        assert (
            changelog.name
            == f"changelog__{group}--repartition.{state_id}.{operation}--{store_name}"
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
            state_id=data_topic.name,
            key_serializer="bytes",
            value_serializer="bytes",
            config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        changelog_topic = topic_manager.changelog_topic(
            state_id=data_topic.name,
            store_name="default",
            config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        assert data_topic.name in topic_manager.non_changelog_topics
        assert repartition_topic.name in topic_manager.non_changelog_topics
        assert changelog_topic.name not in topic_manager.non_changelog_topics

    def test_state_id_from_topics_success(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic("test1")
        topic2 = topic_manager.topic("test2")
        state_id = topic_manager.state_id_from_topics([topic1, topic2])

        assert state_id == "test1--test2"

    def test_state_id_from_topics_sorted(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic1 = topic_manager.topic("test1")
        topic2 = topic_manager.topic("test2")

        assert topic_manager.state_id_from_topics(
            [topic1, topic2]
        ) == topic_manager.state_id_from_topics([topic2, topic1])

    def test_state_id_from_topics_no_topics_fails(self, topic_manager_factory):
        topic_manager = topic_manager_factory()
        with pytest.raises(ValueError):
            topic_manager.state_id_from_topics([])
