from concurrent.futures import Future

import pytest
from confluent_kafka import KafkaException

from src.quixstreams.dataframes.models import (
    Topic,
    JSONSerializer,
    SerializationError,
)


class TestRowProducer:
    def test_produce_row_success(
        self, row_consumer_factory, row_producer_factory,
        topic_json_serdes_factory, producable_row_factory
    ):
        topic = topic_json_serdes_factory()

        key = b"key"
        value = {"field": "value"}
        headers = [("header1", b"1")]

        with row_consumer_factory(
            auto_offset_reset="earliest"
        ) as consumer, row_producer_factory() as producer:
            row = producable_row_factory(
                topic=topic.name,
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

    def test_produce_row_serialization_error_raise(
            self, row_producer_factory, producable_row_factory
    ):
        topic = Topic(
            "test",
            value_serializer=JSONSerializer(),
        )

        with row_producer_factory() as producer:
            row = producable_row_factory(
                topic=topic.name,
                value=object(),
            )
            with pytest.raises(SerializationError):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_produce_error_raise(
            self, row_producer_factory, producable_row_factory
    ):
        topic = Topic(
            "test",
            value_serializer=JSONSerializer(),
        )

        with row_producer_factory(extra_config={"message.max.bytes": 1000}) as producer:
            row = producable_row_factory(
                topic=topic.name,
                value={"field": 1001 * "a"},
            )
            with pytest.raises(KafkaException):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_serialization_error_suppress(
        self, row_consumer_factory, row_producer_factory, producable_row_factory
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
            row = producable_row_factory(
                topic=topic.name,
                value=object(),
            )
            producer.produce_row(topic=topic, row=row)
