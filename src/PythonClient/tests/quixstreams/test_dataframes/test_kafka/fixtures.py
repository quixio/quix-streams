import asyncio
import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

from src.quixstreams.dataframes.kafka import AsyncProducer
from src.quixstreams.dataframes.kafka.consumer import AsyncConsumer, AutoOffsetReset


@pytest.fixture()
def consumer_factory(kafka_container, event_loop):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: str = "tests",
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
    ) -> AsyncConsumer:
        extra_config = extra_config or {}

        # Make consumers to refresh cluster metadata often
        # to react on re-assignment changes faster
        extra_config["topic.metadata.refresh.interval.ms"] = 3000

        return AsyncConsumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            auto_offset_reset=auto_offset_reset,
            extra_config=extra_config,
            loop=event_loop,
        )

    return factory


@pytest.fixture()
def consumer(consumer_factory) -> AsyncConsumer:
    return consumer_factory()


@pytest.fixture()
def producer_factory(kafka_container, event_loop):
    def factory(
        broker_address: str = kafka_container.broker_address,
        extra_config: dict = None,
    ) -> AsyncProducer:
        extra_config = extra_config or {}

        return AsyncProducer(
            broker_address=broker_address,
            extra_config=extra_config,
            loop=event_loop,
        )

    return factory


@pytest.fixture()
def producer(producer_factory) -> AsyncProducer:
    return producer_factory()


@pytest.fixture()
def kafka_admin_client(kafka_container) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_container.broker_address})


@pytest.fixture()
def topic_factory(kafka_admin_client, event_loop):
    async def func(topic: str = None, num_partitions: int = 1) -> (str, int):
        topic = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_topics(
            [NewTopic(topic=topic, num_partitions=num_partitions)]
        )
        await asyncio.wrap_future(futures[topic], loop=event_loop)
        return topic, num_partitions

    return func


@pytest.fixture()
def set_topic_partitions(kafka_admin_client, event_loop):
    async def func(topic: str = None, num_partitions: int = 1) -> (str, int):
        topic = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_partitions(
            [NewPartitions(topic=topic, new_total_count=num_partitions)]
        )
        await asyncio.wrap_future(futures[topic], loop=event_loop)
        return topic, num_partitions

    return func
