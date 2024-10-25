from unittest.mock import MagicMock

from quixstreams.models import TopicAdmin
from quixstreams.platforms.quix import QuixTopicManager


class TestQuixTopicManager:
    def test_quix_topic(self, quix_mock_config_builder_factory):
        """
        Topic name should be "id" field from the Quix API get_topic result if found
        """
        num_partitions = 5
        rep_factor = 5
        topic_name = "my_topic"
        topic_id = "quix_topic_id"

        config_builder = quix_mock_config_builder_factory()
        config_builder.get_or_create_topic.side_effect = lambda _topic: {
            "id": topic_id,
            "name": _topic.name,
            "configuration": {
                "partitions": num_partitions,
                "replicationFactor": rep_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Delete",
            },
        }

        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group="test_group",
            quix_config_builder=config_builder,
        )
        topic = topic_manager.topic(topic_name)
        assert topic.name == topic_id
        assert topic_manager.topics[topic.name] == topic
        assert (
            config_builder.get_or_create_topic.call_args_list[0].args[0].name
            == topic_name
        )
        assert topic.config.num_partitions == num_partitions
        assert topic.config.replication_factor == rep_factor

    def test_quix_internal_topic(self, quix_mock_config_builder_factory):
        num_partitions = 5
        rep_factor = 5
        quix_api_prepend = "whatever"
        consumer_group = "my_group"
        store_name = "default"
        topic_name = "my_topic"
        topic_id = f"{quix_api_prepend}-{topic_name}"
        changelog_name = f"changelog__{consumer_group}--{topic_name}--{store_name}"
        changelog_id = f"{quix_api_prepend}-{changelog_name}"
        config_builder = quix_mock_config_builder_factory()
        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group=consumer_group,
            quix_config_builder=config_builder,
        )

        config_builder.get_or_create_topic.side_effect = lambda _topic: {
            "id": topic_id,
            "name": _topic.name,
            "configuration": {
                "partitions": num_partitions,
                "replicationFactor": rep_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Delete",
            },
        }
        topic = topic_manager.topic(topic_name)
        assert topic.name == topic_id

        config_builder.get_or_create_topic.side_effect = lambda _topic: {
            "id": changelog_id,
            "name": _topic.name,
            "configuration": {
                "partitions": topic.config.num_partitions,
                "replicationFactor": topic.config.replication_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Compact",
            },
        }
        changelog = topic_manager.changelog_topic(
            topic_name=topic.name, store_name=store_name
        )
        assert changelog.name == changelog_id
        assert topic_manager.changelog_topics[topic.name][store_name] == changelog
        assert (
            config_builder.get_or_create_topic.call_args_list[1].args[0].name
            == changelog_name
        )
        assert changelog.config.num_partitions == num_partitions
        assert changelog.config.replication_factor == rep_factor

        config_builder.get_or_create_topic.side_effect = lambda _topic: {
            "id": topic_id,
            "name": _topic.name,
            "configuration": {
                "partitions": num_partitions,
                "replicationFactor": rep_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Delete",
            },
        }
        topic = topic_manager.topic(topic_name)
        assert topic.name == topic_id

        config_builder.get_or_create_topic.side_effect = lambda _topic: {
            "id": changelog_id,
            "name": _topic.name,
            "configuration": {
                "partitions": topic.config.num_partitions,
                "replicationFactor": topic.config.replication_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Compact",
            },
        }
        changelog = topic_manager.changelog_topic(
            topic_name=topic.name, store_name=store_name
        )
        assert changelog.name == changelog_id
        assert topic_manager.changelog_topics[topic.name][store_name] == changelog
        assert (
            config_builder.get_or_create_topic.call_args_list[1].args[0].name
            == changelog_name
        )
        assert changelog.config.num_partitions == num_partitions
        assert changelog.config.replication_factor == rep_factor

    def test_quix_changelog_nested_internal_topic_naming(
        self, quix_mock_config_builder_factory
    ):
        """
        Confirm expected formatting for an internal topic that spawns another internal
        topic (changelog)
        """
        num_partitions = 5
        rep_factor = 5
        quix_api_prepend = "whatever"
        store_name = "my_store"
        consumer_group = "my_consumer_group"
        operation = "my_op"
        topic_name = "my_topic"
        topic_id = f"{quix_api_prepend}-{topic_name}"
        changelog_name = f"changelog__{consumer_group}--{topic_name}--{store_name}"
        changelog_topic_id = f"{quix_api_prepend}-{changelog_name}"
        repartition_name = f"repartition__{consumer_group}--{topic_name}--{operation}"
        repartition_id = f"{quix_api_prepend}-{repartition_name}"
        changelog_name = f"changelog__{consumer_group}--repartition.{topic_name}.{operation}--{store_name}"
        changelog_topic_id = f"{quix_api_prepend}-{changelog_name}"

        config_builder = quix_mock_config_builder_factory()
        topic_manager = QuixTopicManager(
            topic_admin=MagicMock(spec_set=TopicAdmin),
            consumer_group=consumer_group,
            quix_config_builder=config_builder,
        )

        config_builder.get_or_create_topic.side_effect = lambda _topic: {
            "id": topic_id,
            "name": _topic.name,
            "configuration": {
                "partitions": num_partitions,
                "replicationFactor": rep_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Delete",
            },
        }
        topic = topic_manager.topic(topic_name)
        assert topic.name == topic_id

        config_builder.get_or_create_topic.side_effect = lambda _topic: {
            "id": repartition_id,
            "name": _topic.name,
            "configuration": {
                "partitions": topic.config.num_partitions,
                "replicationFactor": topic.config.replication_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Compact",
            },
        }
        repartition = topic_manager.repartition_topic(operation, topic.name)
        assert repartition.name == repartition_id
        assert topic_manager.repartition_topics[repartition.name] == repartition

        config_builder.get_or_create_topic.side_effect = lambda _topic: {
            "id": changelog_topic_id,
            "name": _topic.name,
            "configuration": {
                "partitions": topic.config.num_partitions,
                "replicationFactor": topic.config.replication_factor,
                "retentionInMinutes": 1,
                "retentionInBytes": 1,
                "cleanupPolicy": "Compact",
            },
        }
        changelog = topic_manager.changelog_topic(repartition.name, store_name)
        assert changelog.name == changelog_topic_id
        assert topic_manager.changelog_topics[repartition.name][store_name] == changelog
        assert (
            config_builder.get_or_create_topic.call_args_list[2].args[0].name
            == changelog_name
        )
        assert changelog.config.num_partitions == num_partitions
        assert changelog.config.replication_factor == rep_factor
