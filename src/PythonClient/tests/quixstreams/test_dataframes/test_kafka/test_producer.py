import confluent_kafka
import pytest


async def test_producer_start_close(producer):
    async with producer:
        ...


class TestAsyncProducer:
    async def test_producer_produce_non_blocking(self, producer, topic_factory):
        topic_name, _ = await topic_factory()
        async with producer:
            await producer.produce(
                topic=topic_name,
                key="test",
                value=b"test",
            )

    async def test_producer_produce_blocking_success(self, producer, topic_factory):
        topic_name, _ = await topic_factory()
        async with producer:
            await producer.produce(
                topic=topic_name, key="test", value=b"test", blocking=True
            )

    async def test_producer_produce_blocking_failure_raises_error(
        self, producer_factory, topic_factory
    ):
        topic_name, _ = await topic_factory()
        extra_config = {
            # Set impossible message timeout to simulate a failure
            "linger.ms": 1,
            "message.timeout.ms": 2,
        }
        value = b"1" * 1001

        async with producer_factory(extra_config=extra_config) as producer:
            with pytest.raises(confluent_kafka.KafkaException) as raised:
                await producer.produce(
                    topic=topic_name, key="test", value=value, blocking=True
                )

        kafka_err: confluent_kafka.KafkaError = raised.value.args[0]
        assert kafka_err.code() == confluent_kafka.KafkaError._MSG_TIMED_OUT

    async def test_producer_produce_non_blocking_failure_no_error(
        self, producer_factory, topic_factory
    ):
        topic_name, _ = await topic_factory()
        extra_config = {
            # Set impossible message timeout to simulate a failure
            "linger.ms": 1,
            "message.timeout.ms": 2,
        }
        value = b"1" * 1001

        async with producer_factory(extra_config=extra_config) as producer:
            await producer.produce(topic=topic_name, key="test", value=value)

    async def test_producer_produce_flush_success(
        self, producer_factory, topic_factory
    ):
        topic_name, _ = await topic_factory()

        # Set larger buffer timeout to accumulate pending messages
        extra_config = {"linger.ms": 30000}
        async with producer_factory(extra_config=extra_config) as producer:
            await producer.produce(
                topic=topic_name,
                key="test",
                value=b"test",
            )
            await producer.produce(
                topic=topic_name,
                key="test",
                value=b"test",
            )
            assert len(producer) > 0
            pending_count = await producer.flush()
            assert pending_count == 0

    async def test_producer_produce_flush_failed(self, producer_factory, topic_factory):
        topic_name, _ = await topic_factory()

        extra_config = {
            # Set impossible message timeout to simulate a failure
            "linger.ms": 1,
            "message.timeout.ms": 2,
        }
        value = b"1" * 1001
        async with producer_factory(extra_config=extra_config) as producer:
            await producer.produce(
                topic=topic_name,
                key="test",
                value=value,
            )
            await producer.produce(
                topic=topic_name,
                key="test",
                value=value,
            )
            assert len(producer) > 0
            pending_count = await producer.flush()
            assert pending_count == 0
