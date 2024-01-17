from unittest.mock import patch

import uuid
from quixstreams.state.changelog import (
    ChangelogManager,
    ChangelogWriter,
)
from quixstreams.topic_manager import BytesTopic


class TestChangelogWriter:
    def test_produce(
        self, topic_manager_admin_factory, row_producer_factory, consumer_factory
    ):
        p_num = 2
        cf_header = "my_cf_header"
        cf = "my_cf"
        expected = {
            "key": b"my_key",
            "value": b"10",
            "headers": [(cf_header, cf.encode())],
            "partition": p_num,
        }
        topic_manager = topic_manager_admin_factory()
        topic = BytesTopic(
            name=str(uuid.uuid4()), config=topic_manager.topic_config(num_partitions=3)
        )
        topic_manager.create_topics([topic])

        writer = ChangelogWriter(
            topic=topic, partition_num=p_num, producer=row_producer_factory()
        )
        writer.produce(
            **{k: v for k, v in expected.items() if k in ["key", "value"]},
            headers={cf_header: cf},
        )
        writer._producer.flush(5)

        consumer = consumer_factory(auto_offset_reset="earliest")
        consumer.subscribe([topic.name])
        message = consumer.poll(10)

        for k in expected:
            assert getattr(message, k)() == expected[k]


class TestChangelogManager:
    def test_add_changelog(self, topic_manager_factory, row_producer_factory):
        topic_manager = topic_manager_factory()
        changelog_manager = ChangelogManager(
            topic_manager=topic_manager, producer=row_producer_factory()
        )
        kwargs = dict(
            source_topic_name="my_source_topic",
            suffix="my_suffix",
            consumer_group="my_group",
        )
        with patch.object(topic_manager, "changelog_topic") as make_changelog:
            changelog_manager.add_changelog(**kwargs)
        make_changelog.assert_called_with(**kwargs)

    def test_get_writer(self, topic_manager_factory, row_producer_factory):
        topic_manager = topic_manager_factory()
        producer = row_producer_factory()
        changelog_manager = ChangelogManager(
            topic_manager=topic_manager, producer=producer
        )

        topic_name = "my_topic"
        suffix = "my_suffix"
        p_num = 1
        kwargs = dict(source_topic_name=topic_name, suffix=suffix)
        topic_manager.topic(topic_name)  # changelogs depend on topic objects existing
        changelog_topic = topic_manager.changelog_topic(
            **kwargs, consumer_group="group"
        )

        writer = changelog_manager.get_writer(**kwargs, partition_num=p_num)
        assert writer._topic == changelog_topic
        assert writer._partition_num == p_num
        assert writer._producer == producer
