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
)
from tests.utils import Timeout


class TestRowConsumer:
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
