from concurrent.futures import Future

import pytest
from confluent_kafka import KafkaError, TopicPartition

from src.quixstreams.dataframes import Topic
from src.quixstreams.dataframes.models import (
    IgnoreMessage,
    Deserializer,
    SerializationError,
)
from src.quixstreams.dataframes.rowconsumer import KafkaMessageError


class TestRowConsumer:
    def test_poll_row_success(
        self, row_consumer_factory, topic_json_serdes_factory, producer, executor
    ):
        topic = topic_json_serdes_factory()
        with row_consumer_factory(
            auto_offset_reset="earliest",
        ) as consumer, producer:
            producer.produce(topic=topic.name, key=b"key", value=b'{"field":"value"}')
            producer.flush()
            consumer.subscribe([topic])
            polled = Future()

            def poll():
                while True:
                    row = consumer.poll_row(0.1)
                    if row is not None:
                        polled.set_result(row)
                        return

            executor.submit(poll)
            row = polled.result(10.0)

        assert row
        assert row.key == b"key"
        assert row.value == {"field": "value"}

    def test_poll_row_multiple_topics(
        self, row_consumer_factory, topic_json_serdes_factory, producer, executor
    ):
        topics = [topic_json_serdes_factory(), topic_json_serdes_factory()]
        with row_consumer_factory(
            auto_offset_reset="earliest",
        ) as consumer, producer:
            for topic in topics:
                producer.produce(
                    topic=topic.name, key=b"key", value=b'{"field":"value"}'
                )
            producer.flush()
            consumer.subscribe(topics)
            rows = Future()

            def poll():
                _rows = []
                while True:
                    row = consumer.poll_row(0.1)
                    if row is not None:
                        _rows.append(_rows)
                        if len(_rows) == 2:
                            rows.set_result(_rows)

            executor.submit(poll)
            rows = rows.result(10.0)

        assert len(rows) == 2

    def test_poll_row_kafka_error(self, row_consumer_factory):
        topic = Topic("123")
        with row_consumer_factory(
            auto_offset_reset="earliest",
        ) as consumer:
            consumer.subscribe([topic])
            with pytest.raises(KafkaMessageError) as raised:
                consumer.poll_row(10.0)
        exc = raised.value
        assert exc.code == KafkaError.UNKNOWN_TOPIC_OR_PART

    def test_poll_row_ignore_message(
        self, row_consumer_factory, topic_factory, producer
    ):
        topic_name, _ = topic_factory()

        class _Deserializer(Deserializer):
            def __call__(self, *args, **kwargs):
                raise IgnoreMessage()

        topic = Topic(topic_name, value_deserializer=_Deserializer())
        with row_consumer_factory(
            auto_offset_reset="earliest",
        ) as consumer, producer:
            producer.produce(topic_name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            row = consumer.poll_row(10.0)
            assert row is None

            low, high = consumer.get_watermark_offsets(
                partition=TopicPartition(topic=topic_name, partition=0)
            )
        assert low == 0
        assert high == 1

    def test_poll_row_deserialization_error_raise(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()
        with row_consumer_factory(
            auto_offset_reset="earliest",
        ) as consumer, producer:
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            with pytest.raises(SerializationError):
                consumer.poll_row(10.0)

    def test_poll_row_kafka_error_raise(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()
        with row_consumer_factory(
            auto_offset_reset="error",
        ) as consumer, producer:
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            with pytest.raises(KafkaMessageError):
                consumer.poll_row(10.0)

    def test_poll_row_deserialization_error_suppress(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()

        suppressed = Future()

        def on_error(exc, *args):
            assert isinstance(exc, SerializationError)
            suppressed.set_result(True)
            return True

        with row_consumer_factory(
            auto_offset_reset="earliest",
            on_error=on_error,
        ) as consumer, producer:
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            row = consumer.poll_row(10.0)
            assert row is None
            assert suppressed.result(10.0)

    def test_poll_row_kafka_error_suppress(
        self, row_consumer_factory, topic_json_serdes_factory, producer
    ):
        topic = topic_json_serdes_factory()

        suppressed = Future()

        def on_error(exc, *args):
            assert isinstance(exc, KafkaMessageError)
            suppressed.set_result(True)
            return True

        with row_consumer_factory(
            auto_offset_reset="error",
            on_error=on_error,
        ) as consumer, producer:
            producer.produce(topic.name, key=b"key", value=b"value")
            producer.flush()
            consumer.subscribe([topic])
            row = consumer.poll_row(10.0)
            assert row is None
            assert suppressed.result(10.0)
