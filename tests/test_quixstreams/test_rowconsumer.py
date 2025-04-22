import uuid
from unittest.mock import patch
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError, TopicPartition

from quixstreams.exceptions import PartitionAssignmentError
from quixstreams.kafka.exceptions import KafkaConsumerException
from quixstreams.models import (
    Deserializer,
    IgnoreMessage,
    SerializationError,
    Topic,
    TopicConfig,
)
from quixstreams.rowconsumer import RowConsumer
from tests.utils import Timeout


class TestRowConsumer:
    def test_consumer_reuse(self, row_consumer_factory):
        row_consumer = row_consumer_factory()
        # just repeat the same thing twice, the consumer should be reusable
        for i in range(2):
            assert not row_consumer.consumer_exists
            with row_consumer as consumer:
                metadata = consumer.list_topics()
                assert consumer.consumer_exists
                assert metadata
                broker_meta = metadata.brokers[0]
                broker_address = f"{broker_meta.host}:{broker_meta.port}"
                assert broker_address

    def test_poll_row_success(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()
        with (
            row_consumer_factory(
                auto_offset_reset="earliest",
            ) as consumer,
            producer,
        ):
            producer.produce(topic=topic.name, key=b"key", value=b'{"field":"value"}')
            producer.flush()
            consumer.subscribe([topic])

            while Timeout():
                row = consumer.poll_row(0.1)
                if row is not None:
                    assert row
                    assert row.key == b"key"
                    assert row.value == {"field": "value"}
                    return

    def test_poll_row_multiple_topics(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topics = [topic_json_serdes_factory(), topic_json_serdes_factory()]
        with (
            row_consumer_factory(
                auto_offset_reset="earliest",
            ) as consumer,
            producer,
        ):
            for topic in topics:
                producer.produce(
                    topic=topic.name, key=b"key", value=b'{"field":"value"}'
                )
            producer.flush()
            consumer.subscribe(topics)

            rows = []
            while Timeout():
                row = consumer.poll_row(0.1)
                if row is not None:
                    rows.append(row)
                    if len(rows) == 2:
                        return

    def test_poll_row_kafka_error(self, row_consumer_factory, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic = Topic(name=str(uuid4()), create_config=topic_manager.topic_config())

        with row_consumer_factory(
            auto_offset_reset="earliest",
        ) as consumer:
            consumer.subscribe([topic])
            with pytest.raises(KafkaConsumerException) as raised:
                consumer.poll_row(10.0)
        exc = raised.value
        assert exc.code == KafkaError.UNKNOWN_TOPIC_OR_PART

    def test_poll_row_ignore_message(
        self, row_consumer_factory, topic_manager_topic_factory, producer
    ):
        class _Deserializer(Deserializer):
            def __call__(self, *args, **kwargs):
                raise IgnoreMessage()

        topic = topic_manager_topic_factory(value_deserializer=_Deserializer())
        with (
            row_consumer_factory(
                auto_offset_reset="earliest",
            ) as consumer,
            producer,
        ):
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            row = consumer.poll_row(10.0)
            assert row is None

            low, high = consumer.get_watermark_offsets(
                partition=TopicPartition(topic=topic.name, partition=0)
            )
        assert low == 0
        assert high == 1

    def test_poll_row_deserialization_error_raise(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()
        with (
            row_consumer_factory(
                auto_offset_reset="earliest",
            ) as consumer,
            producer,
        ):
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            with pytest.raises(SerializationError):
                consumer.poll_row(10.0)

    def test_poll_row_kafka_error_raise(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()
        with (
            row_consumer_factory(
                auto_offset_reset="error",
            ) as consumer,
            producer,
        ):
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            with pytest.raises(KafkaConsumerException):
                consumer.poll_row(10.0)

    def test_poll_row_deserialization_error_suppress(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()

        suppressed = False

        def on_error(exc, *args):
            assert isinstance(exc, SerializationError)
            nonlocal suppressed
            suppressed = True
            return True

        with (
            row_consumer_factory(
                auto_offset_reset="earliest",
                on_error=on_error,
            ) as consumer,
            producer,
        ):
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            row = consumer.poll_row(10.0)
            assert row is None
            assert suppressed

    def test_poll_row_kafka_error_suppress(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()

        suppressed = False

        def on_error(exc, *args):
            assert isinstance(exc, KafkaConsumerException)
            nonlocal suppressed
            suppressed = True
            return True

        with (
            row_consumer_factory(
                auto_offset_reset="error",
                on_error=on_error,
            ) as consumer,
            producer,
        ):
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            row = consumer.poll_row(10.0)
            assert row is None
            assert suppressed

    def test_poll_row_kafka_error_suppress_except_partition_assignment(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()

        def on_error(*_):
            return True

        def on_assign(*_):
            raise ValueError("Test")

        with (
            row_consumer_factory(
                auto_offset_reset="error",
                on_error=on_error,
            ) as consumer,
            producer,
        ):
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic], on_assign=on_assign)
            with pytest.raises(PartitionAssignmentError):
                consumer.poll_row(10.0)

    def test_trigger_backpressure(self, topic_manager_factory, row_consumer):
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=1, replication_factor=1),
        )
        # Create a changelog topic
        changelog = topic_manager.changelog_topic(
            stream_id=topic.name, store_name="default", config=topic.broker_config
        )
        offset_to_seek = 999

        row_consumer.subscribe([topic, changelog])
        while not row_consumer.assignment():
            row_consumer.poll(0.1)

        with (
            patch.object(RowConsumer, "pause") as pause_mock,
            patch.object(RowConsumer, "seek") as seek_mock,
        ):
            row_consumer.trigger_backpressure(
                resume_after=1,
                offsets_to_seek={(topic.name, 0): offset_to_seek},
            )

        assert (
            TopicPartition(topic=topic.name, partition=0)
            in row_consumer.backpressured_tps
        )
        pause_mock.assert_called_once_with(
            partitions=[TopicPartition(topic=topic.name, partition=0, offset=-1000)]
        )
        seek_mock.assert_called_once_with(
            partition=TopicPartition(
                topic=topic.name, partition=0, offset=offset_to_seek
            )
        )

    def test_resume_backpressured_nothing_paused(
        self, row_consumer, topic_manager_factory
    ):
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        row_consumer.subscribe([topic])
        while not row_consumer.assignment():
            row_consumer.poll(0.1)

        with patch.object(RowConsumer, "resume") as resume_mock:
            row_consumer.resume_backpressured()
        assert not resume_mock.called

    def test_resume_backpressured(self, row_consumer, topic_manager_factory):
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=2, replication_factor=1),
        )

        # Create a changelog topic
        changelog = topic_manager.changelog_topic(
            stream_id=topic.name, store_name="default", config=topic.broker_config
        )

        offset_to_seek = 999

        row_consumer.subscribe([topic, changelog])
        while not row_consumer.assignment():
            row_consumer.poll(0.1)

        # Pause one partition that is ready to be resumed right now
        row_consumer.trigger_backpressure(
            resume_after=0,
            offsets_to_seek={
                (topic.name, 0): offset_to_seek,
                (topic.name, 1): offset_to_seek,
            },
        )
        assert row_consumer.backpressured_tps

        with patch.object(RowConsumer, "resume") as resume_mock:
            # Resume partitions
            row_consumer.resume_backpressured()

        assert not row_consumer.backpressured_tps

        # Ensure that only previously backpressured partitions are resumed and
        # not changelog partitions
        assert resume_mock.call_count == 2
        resume_mock.assert_any_call(
            partitions=[TopicPartition(topic=topic.name, partition=0)]
        )
        resume_mock.assert_any_call(
            partitions=[TopicPartition(topic=topic.name, partition=1)]
        )
