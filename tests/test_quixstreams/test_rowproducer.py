from concurrent.futures import Future

import pytest
from confluent_kafka import KafkaException as ConfluentKafkaException

from quixstreams.kafka.exceptions import KafkaProducerDeliveryError
from quixstreams.models import (
    JSONSerializer,
    SerializationError,
)


class TestRowProducer:
    def test_produce_row_success(
        self,
        row_consumer_factory,
        row_producer_factory,
        topic_json_serdes_factory,
        row_factory,
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = {"field": "value"}
        headers = [("header1", b"1")]

        with row_producer_factory() as producer:
            row = row_factory(
                topic=topic.name,
                value=value,
                key=key,
                headers=headers,
            )
            producer.produce_row(topic=topic, row=row)

        with row_consumer_factory(auto_offset_reset="earliest") as consumer:
            consumer.subscribe([topic])
            row = consumer.poll_row(timeout=5.0)

        assert producer.offsets
        assert producer.offsets.get((topic.name, 0)) is not None

        assert row
        assert row.key == key
        assert row.value == value
        assert row.headers == headers

    @pytest.mark.parametrize(
        "init_key, new_key, expected_key",
        [
            (b"key", b"new_key", b"new_key"),
            (b"key", b"", b""),
            (b"key", None, None),
        ],
    )
    def test_produce_row_custom_key(
        self,
        init_key,
        new_key,
        expected_key,
        row_consumer_factory,
        row_producer_factory,
        topic_json_serdes_factory,
        row_factory,
    ):
        topic = topic_json_serdes_factory()

        value = {"field": "value"}
        headers = [("header1", b"1")]

        with row_consumer_factory(
            auto_offset_reset="earliest"
        ) as consumer, row_producer_factory() as producer:
            row = row_factory(
                topic=topic.name,
                value=value,
                key=init_key,
                headers=headers,
            )
            producer.produce_row(topic=topic, row=row, key=new_key)
            consumer.subscribe([topic])
            row = consumer.poll_row(timeout=5.0)

        assert row
        assert row.key == expected_key
        assert row.value == value
        assert row.headers == headers

    def test_produce_row_serialization_error_raise(
        self, row_producer_factory, row_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

        with row_producer_factory() as producer:
            row = row_factory(
                topic=topic.name,
                value=object(),
            )
            with pytest.raises(SerializationError):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_produce_error_raise(
        self, row_producer_factory, row_factory, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

        with row_producer_factory(extra_config={"message.max.bytes": 1000}) as producer:
            row = row_factory(
                topic=topic.name,
                value={"field": 1001 * "a"},
            )
            with pytest.raises(ConfluentKafkaException):
                producer.produce_row(topic=topic, row=row)

    def test_produce_row_serialization_error_suppress(
        self,
        row_consumer_factory,
        row_producer_factory,
        row_factory,
        topic_manager_topic_factory,
    ):
        topic = topic_manager_topic_factory(value_serializer=JSONSerializer())

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

    def test_produce_delivery_error_raised_on_produce(
        self, row_producer_factory, topic_json_serdes_factory
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = b"value"

        producer = row_producer_factory()

        # Send message to a non-existing partition to simulate error
        # in the delivery callback
        producer.produce(topic=topic.name, key=key, value=value, partition=3)
        # Poll for delivery callbacks
        producer.poll(5)
        # The next produce should fail after
        with pytest.raises(KafkaProducerDeliveryError):
            producer.produce(topic=topic.name, key=key, value=value)

    def test_produce_delivery_error_raised_on_flush(
        self, row_producer_factory, topic_json_serdes_factory
    ):
        topic = topic_json_serdes_factory(num_partitions=1)
        key = b"key"
        value = b"value"

        producer = row_producer_factory()

        # Send message to a non-existing partition to simulate error
        # in the delivery callback
        producer.produce(topic=topic.name, key=key, value=value, partition=3)
        # The flush should fail after that
        with pytest.raises(KafkaProducerDeliveryError):
            producer.flush()
