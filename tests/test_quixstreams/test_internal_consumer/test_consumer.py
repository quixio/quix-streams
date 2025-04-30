import uuid
from collections import namedtuple
from unittest.mock import patch
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError, TopicPartition

from quixstreams.exceptions import PartitionAssignmentError
from quixstreams.internal_consumer import InternalConsumer
from quixstreams.kafka.exceptions import KafkaConsumerException
from quixstreams.models import (
    Deserializer,
    IgnoreMessage,
    SerializationError,
    Topic,
    TopicConfig,
)
from tests.utils import Timeout


class TestInternalConsumer:
    def test_consumer_reuse(self, internal_consumer_factory):
        internal_consumer = internal_consumer_factory()
        # just repeat the same thing twice, the consumer should be reusable
        for i in range(2):
            assert not internal_consumer.consumer_exists
            with internal_consumer as consumer:
                metadata = consumer.list_topics()
                assert consumer.consumer_exists
                assert metadata
                broker_meta = metadata.brokers[0]
                broker_address = f"{broker_meta.host}:{broker_meta.port}"
                assert broker_address

    def test_poll_row_success(
        self, internal_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()
        with (
            internal_consumer_factory(
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
                    break

    def test_poll_row_multiple_topics(
        self, internal_consumer_factory, topic_json_serdes_factory, producer
    ):
        topics = [topic_json_serdes_factory(), topic_json_serdes_factory()]
        with (
            internal_consumer_factory(
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
                        break

    def test_poll_row_kafka_error(
        self, internal_consumer_factory, topic_manager_factory
    ):
        topic_manager = topic_manager_factory()
        topic = Topic(name=str(uuid4()), create_config=topic_manager.topic_config())

        with internal_consumer_factory(
            auto_offset_reset="earliest",
        ) as consumer:
            consumer.subscribe([topic])
            with pytest.raises(KafkaConsumerException) as raised:
                consumer.poll_row(10.0)
        exc = raised.value
        assert exc.code == KafkaError.UNKNOWN_TOPIC_OR_PART

    def test_poll_row_ignore_message(
        self, internal_consumer_factory, topic_manager_topic_factory, producer
    ):
        class _Deserializer(Deserializer):
            def __call__(self, *args, **kwargs):
                raise IgnoreMessage()

        topic = topic_manager_topic_factory(value_deserializer=_Deserializer())
        with (
            internal_consumer_factory(
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
        self, internal_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()
        with (
            internal_consumer_factory(
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
        self, internal_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()
        with (
            internal_consumer_factory(
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
        self, internal_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()

        suppressed = False

        def on_error(exc, *args):
            assert isinstance(exc, SerializationError)
            nonlocal suppressed
            suppressed = True
            return True

        with (
            internal_consumer_factory(
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
        self, internal_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()

        suppressed = False

        def on_error(exc, *args):
            assert isinstance(exc, KafkaConsumerException)
            nonlocal suppressed
            suppressed = True
            return True

        with (
            internal_consumer_factory(
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
        self, internal_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()

        def on_error(*_):
            return True

        def on_assign(*_):
            raise ValueError("Test")

        with (
            internal_consumer_factory(
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

    def test_trigger_backpressure(self, topic_manager_factory, internal_consumer):
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

        internal_consumer.subscribe([topic, changelog])
        while not internal_consumer.assignment():
            internal_consumer.poll(0.1)

        with (
            patch.object(InternalConsumer, "pause") as pause_mock,
            patch.object(InternalConsumer, "seek") as seek_mock,
        ):
            internal_consumer.trigger_backpressure(
                resume_after=1,
                offsets_to_seek={(topic.name, 0): offset_to_seek},
            )

        assert (
            TopicPartition(topic=topic.name, partition=0)
            in internal_consumer.backpressured_tps
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
        self, internal_consumer, topic_manager_factory
    ):
        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name=str(uuid.uuid4()),
            create_config=TopicConfig(num_partitions=1, replication_factor=1),
        )

        internal_consumer.subscribe([topic])
        while not internal_consumer.assignment():
            internal_consumer.poll(0.1)

        with patch.object(InternalConsumer, "resume") as resume_mock:
            internal_consumer.resume_backpressured()
        assert not resume_mock.called

    def test_resume_backpressured(self, internal_consumer, topic_manager_factory):
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

        internal_consumer.subscribe([topic, changelog])
        while not internal_consumer.assignment():
            internal_consumer.poll(0.1)

        # Pause one partition that is ready to be resumed right now
        internal_consumer.trigger_backpressure(
            resume_after=0,
            offsets_to_seek={
                (topic.name, 0): offset_to_seek,
                (topic.name, 1): offset_to_seek,
            },
        )
        assert internal_consumer.backpressured_tps

        with patch.object(InternalConsumer, "resume") as resume_mock:
            # Resume partitions
            internal_consumer.resume_backpressured()

        assert not internal_consumer.backpressured_tps

        # Ensure that only previously backpressured partitions are resumed and
        # not changelog partitions
        assert resume_mock.call_count == 2
        resume_mock.assert_any_call(
            partitions=[TopicPartition(topic=topic.name, partition=0)]
        )
        resume_mock.assert_any_call(
            partitions=[TopicPartition(topic=topic.name, partition=1)]
        )

    def test_poll_row_buffered_multiple_topics_in_order(
        self, internal_consumer_factory, topic_json_serdes_factory, producer
    ):
        """
        Test that "poll_row()" consumes messages in timestamp order
        from two co-partitioned topics
        """
        topic1 = topic_json_serdes_factory()
        topic2 = topic_json_serdes_factory()

        Message = namedtuple("Message", ("topic", "timestamp"))
        messages = [
            Message(topic=topic1.name, timestamp=10),
            Message(topic=topic1.name, timestamp=13),
            Message(topic=topic2.name, timestamp=11),
            Message(topic=topic2.name, timestamp=12),
        ]

        with producer:
            for message in messages:
                producer.produce(
                    topic=message.topic,
                    key=b"key",
                    value=None,
                    timestamp=message.timestamp,
                )

        with internal_consumer_factory(
            auto_offset_reset="earliest",
        ) as consumer:
            consumer.subscribe([topic1, topic2])

            rows = []
            while Timeout():
                row = consumer.poll_row(0.1, buffered=True)
                if row is not None:
                    rows.append(row)
                    if len(rows) == len(messages):
                        break

        assert rows[0].timestamp == messages[0].timestamp
        assert rows[1].timestamp == messages[2].timestamp
        assert rows[2].timestamp == messages[3].timestamp
        assert rows[3].timestamp == messages[1].timestamp
