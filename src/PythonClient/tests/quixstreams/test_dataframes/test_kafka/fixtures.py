import asyncio
import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

from src.quixstreams.dataframes.kafka import AsyncProducer


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
