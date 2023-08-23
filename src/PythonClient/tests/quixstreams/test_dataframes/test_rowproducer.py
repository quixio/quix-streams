from concurrent.futures import Future

import pytest
from confluent_kafka import KafkaException

from src.quixstreams.dataframes.models import (
    Row,
    MessageTimestamp,
    TimestampType,
    Topic,
    JSONSerializer,
    JSONDeserializer,
    SerializationError,
)


def row_factory(
    topic: str,
    value,
    key=b"key",
    headers=None,
) -> Row:
    headers = headers or {}
    return Row(
        key=key,
        value=value,
        headers=headers,
        topic=topic,
        partition=0,
        offset=0,
        size=0,
        timestamp=MessageTimestamp(0, TimestampType.TIMESTAMP_NOT_AVAILABLE),
    )


class TestRowProducer:
    def test_produce_row_success(
        self, row_consumer_factory, row_producer_factory, topic_factory
    ):
        topic_name, _ = topic_factory()
        topic = Topic(
            topic_name,
            value_deserializer=JSONDeserializer(),
            value_serializer=JSONSerializer(),
        )

        key = b"key"
        value = {"field": "value"}
        headers = [("header1", b"1")]

        with row_consumer_factory(
            auto_offset_reset="earliest"
        ) as consumer, row_producer_factory() as producer:
            row = row_factory(
                topic=topic_name,
                value=value,
                key=key,
                headers=headers,
            )
            producer.produce_row(topic=topic, row=row)
            consumer.subscribe([topic])
            row = consumer.poll_row(timeout=5.0)
        assert row
        assert row.key == key
        assert row.value == value
        assert row.headers == headers

    def test_produce_row_serialization_error_raise(self, row_producer_factory):
        topic = Topic(
            "test",
            value_serializer=JSONSerializer(),
        )

        with row_producer_factory() as producer:
            row = row_factory(
                topic=topic.name,
                value=object(),
            )
            with pytest.raises(SerializationError):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_produce_error_raise(self, row_producer_factory):
        topic = Topic(
            "test",
            value_serializer=JSONSerializer(),
        )

        with row_producer_factory(extra_config={"message.max.bytes": 1000}) as producer:
            row = row_factory(
                topic=topic.name,
                value={"field": 1001 * "a"},
            )
            with pytest.raises(KafkaException):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_serialization_error_suppress(
        self, row_consumer_factory, row_producer_factory, topic_factory
    ):
        topic = Topic(
            "test",
            value_serializer=JSONSerializer(),
        )

        suppressed = Future()

        def on_error(exc, *args):
            assert isinstance(exc, SerializationError)
            suppressed.set_result(True)
            return True

        with row_producer_factory(on_error=on_error) as producer:
            row = row_factory(
                topic=topic.name,
                value=object(),
            )
            producer.produce_row(topic=topic, row=row)
