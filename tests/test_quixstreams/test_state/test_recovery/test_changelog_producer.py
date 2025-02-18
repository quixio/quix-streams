import uuid

from quixstreams.state import ChangelogProducer, ChangelogProducerFactory


class TestChangelogProducer:
    def test_produce(
        self, topic_manager_factory, row_producer_factory, consumer_factory
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
        topic_manager = topic_manager_factory()
        changelog = topic_manager.topic(
            name=str(uuid.uuid4()),
            key_serializer="bytes",
            value_serializer="bytes",
            create_config=topic_manager.topic_config(num_partitions=3),
        )
        topic_manager.create_topics([changelog])

        producer = ChangelogProducer(
            changelog_name=changelog.name,
            partition=p_num,
            producer=row_producer_factory(),
        )
        producer.produce(
            **{k: v for k, v in expected.items() if k in ["key", "value"]},
            headers={cf_header: cf},
        )
        producer.flush()

        consumer = consumer_factory(auto_offset_reset="earliest")
        consumer.subscribe([changelog.name])
        message = consumer.poll(10)

        for k in expected:
            assert getattr(message, k)() == expected[k]


class TestChangelogProducerFactory:
    def test_get_partition_producer(self, row_producer_factory):
        changelog_name = "changelog__topic"
        producer = row_producer_factory()

        p_num = 1

        changelog_producer = ChangelogProducerFactory(
            changelog_name=changelog_name,
            producer=producer,
        ).get_partition_producer(partition_num=p_num)
        assert changelog_producer.changelog_name == changelog_name
        assert changelog_producer.partition == p_num
