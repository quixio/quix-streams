import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pytest
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

from src.quixstreams.dataframes.error_callbacks import (
    ConsumerErrorCallback,
    ProducerErrorCallback,
    ProcessingErrorCallback,
)
from src.quixstreams.dataframes.kafka import Partitioner, AutoOffsetReset
from src.quixstreams.dataframes.rowconsumer import RowConsumer
from src.quixstreams.dataframes.rowproducer import RowProducer
from src.quixstreams.dataframes.runner import MessageProcessedCallback, Runner


@pytest.fixture()
def kafka_admin_client(kafka_container) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_container.broker_address})


@pytest.fixture()
def executor() -> ThreadPoolExecutor:
    executor = ThreadPoolExecutor(1)
    try:
        yield executor
    finally:
        # Kill all the threads after leaving the test
        executor.shutdown(wait=False)


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


@pytest.fixture()
def row_consumer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: str = "tests",
        auto_offset_reset: AutoOffsetReset = "latest",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
        on_error: Optional[ConsumerErrorCallback] = None,
    ) -> RowConsumer:
        extra_config = extra_config or {}

        # Make consumers to refresh cluster metadata often
        # to react on re-assignment changes faster
        extra_config["topic.metadata.refresh.interval.ms"] = 3000
        return RowConsumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            auto_offset_reset=auto_offset_reset,
            extra_config=extra_config,
            on_error=on_error,
        )

    return factory


@pytest.fixture()
def row_producer_factory(kafka_container):
    def factory(
        broker_address: str = kafka_container.broker_address,
        partitioner: Partitioner = "murmur2",
        extra_config: dict = None,
        on_error: Optional[ProducerErrorCallback] = None,
    ) -> RowProducer:
        return RowProducer(
            broker_address=broker_address,
            partitioner=partitioner,
            extra_config=extra_config,
            on_error=on_error,
        )

    return factory


@pytest.fixture()
def runner_factory(kafka_container):
    def factory(
        auto_offset_reset: AutoOffsetReset = "latest",
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
    ) -> Runner:
        return Runner(
            broker_address=kafka_container.broker_address,
            consumer_group="tests",
            auto_offset_reset=auto_offset_reset,
            consumer_extra_config=consumer_extra_config,
            producer_extra_config=producer_extra_config,
            on_consumer_error=on_consumer_error,
            on_producer_error=on_producer_error,
            on_processing_error=on_processing_error,
            on_message_processed=on_message_processed,
        )

    return factory
