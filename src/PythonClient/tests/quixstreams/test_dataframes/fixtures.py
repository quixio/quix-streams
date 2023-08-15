import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions


@pytest.fixture()
def kafka_admin_client(kafka_container) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_container.broker_address})


@pytest.fixture()
def executor() -> ThreadPoolExecutor:
    with ThreadPoolExecutor(1) as executor:
        yield executor


@pytest.fixture()
def topic_factory(kafka_admin_client):
    def factory(
        topic: str = None, num_partitions: int = 1, timeout: float = 10.0
    ) -> (str, int):
        topic = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_topics(
            [NewTopic(topic=topic, num_partitions=num_partitions)]
        )
        futures[topic].result(timeout)
        return topic, num_partitions

    return factory


@pytest.fixture()
def set_topic_partitions(kafka_admin_client):
    def func(
        topic: str = None, num_partitions: int = 1, timeout: float = 10.0
    ) -> (str, int):
        topic = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_partitions(
            [NewPartitions(topic=topic, new_total_count=num_partitions)]
        )
        futures[topic].result(timeout)
        return topic, num_partitions

    return func
