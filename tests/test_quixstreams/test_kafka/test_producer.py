import pytest
from confluent_kafka import TopicPartition

from quixstreams.kafka.exceptions import InvalidProducerConfigError


class TestProducer:
    def test_producer_start_close(self, producer):
        with producer:
            ...

    def test_produce(self, producer, topic_factory):
        topic_name, _ = topic_factory()
        with producer:
            producer.produce(
                topic=topic_name,
                key="test",
                value=b"test",
            )
            producer.poll(1.0)

    def test_produce_on_delivery_callback(self, producer, topic_factory):
        topic_name, _ = topic_factory()

        offsets = []
        with producer:
            producer.produce(
                topic=topic_name,
                key="test",
                value=b"test",
                on_delivery=lambda error, msg: offsets.append(msg.offset()),
            )
        assert len(offsets) == 1

    def test_produce_failure_no_error(self, producer_factory, topic_factory):
        topic_name, _ = topic_factory()
        extra_config = {
            # Set impossible message timeout to simulate a failure
            "linger.ms": 1,
            "message.timeout.ms": 2,
        }
        value = b"1" * 1001

        with producer_factory(extra_config=extra_config) as producer:
            producer.produce(topic=topic_name, key="test", value=value)

    def test_produce_flush_success(self, producer_factory, topic_factory):
        topic_name, _ = topic_factory()

        # Set larger buffer timeout to accumulate pending messages
        extra_config = {"linger.ms": 30000}
        with producer_factory(extra_config=extra_config) as producer:
            producer.produce(
                topic=topic_name,
                key="test",
                value=b"test",
            )
            producer.produce(
                topic=topic_name,
                key="test",
                value=b"test",
            )
            assert len(producer) > 0
            pending_count = producer.flush()
            assert pending_count == 0

    def test_produce_flush_failed(self, producer_factory, topic_factory):
        topic_name, _ = topic_factory()

        extra_config = {
            # Set impossible message timeout to simulate a failure
            "linger.ms": 1,
            "message.timeout.ms": 2,
        }
        value = b"1" * 1001
        with producer_factory(extra_config=extra_config) as producer:
            producer.produce(
                topic=topic_name,
                key="test",
                value=value,
            )
            producer.produce(
                topic=topic_name,
                key="test",
                value=value,
            )
            assert len(producer) > 0
            pending_count = producer.flush()
            assert pending_count == 0

    def test_produce_retries_buffererror(self, producer_factory, topic_factory):
        topic_name, _ = topic_factory()

        # Set very small buffer size to simulate BufferError
        extra_config = {"queue.buffering.max.messages": 1}

        value = b"1" * 1001
        total_messages = 10
        # It shouldn't fail
        with producer_factory(extra_config=extra_config) as producer:
            for i in range(total_messages):
                producer.produce(
                    topic=topic_name,
                    key="test",
                    value=value,
                )

    def test_produce_raises_buffererror_if_retries_exceeded(
        self, producer_factory, topic_factory
    ):
        topic_name, _ = topic_factory()

        # Set very small buffer size to simulate BufferError
        extra_config = {"queue.buffering.max.messages": 1}

        value = b"1" * 1001
        total_messages = 10
        with producer_factory(extra_config=extra_config) as producer:
            with pytest.raises(BufferError):
                for i in range(total_messages):
                    producer.produce(
                        topic=topic_name,
                        key="test",
                        value=value,
                        buffer_error_max_tries=0,
                    )

    def test_non_transactional_producer_use_transactional_api_fails(
        self, producer_factory, consumer
    ):
        producer = producer_factory(transactional=False)

        with pytest.raises(InvalidProducerConfigError):
            producer.begin_transaction()

        with pytest.raises(InvalidProducerConfigError):
            producer.abort_transaction(timeout=1)

        with pytest.raises(InvalidProducerConfigError):
            producer.commit_transaction(timeout=1)

        with pytest.raises(InvalidProducerConfigError):
            producer.send_offsets_to_transaction(
                positions=[TopicPartition(topic="test", partition=0, offset=0)],
                group_metadata=consumer.consumer_group_metadata(),
                timeout=1,
            )
