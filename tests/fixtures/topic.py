import uuid
from typing import Optional, Union

import pytest
from confluent_kafka.admin import (
    AdminClient,
    NewPartitions,
    NewTopic,
)

from quixstreams.models.serializers import (
    Deserializer,
    JSONDeserializer,
    JSONSerializer,
    Serializer,
)
from quixstreams.models.topics import (
    TimestampExtractor,
    Topic,
    TopicAdmin,
    TopicConfig,
    TopicManager,
)


@pytest.fixture()
def kafka_admin_client(kafka_container) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_container.broker_address})


@pytest.fixture()
def topic_factory(kafka_admin_client):
    """
    For when you need to create a topic in Kafka.

    The factory will return the resulting topic name and partition count
    """

    def factory(
        topic: str = None, num_partitions: int = 1, timeout: float = 20.0
    ) -> (str, int):
        topic_name = topic or str(uuid.uuid4())
        futures = kafka_admin_client.create_topics(
            [NewTopic(topic=topic_name, num_partitions=num_partitions)]
        )
        futures[topic_name].result(timeout)
        return topic_name, num_partitions

    return factory


@pytest.fixture()
def topic_json_serdes_factory(topic_factory):
    """
    For when you need to create a topic in Kafka and want a `Topic` object afterward.
    Additionally, uses JSON serdes for message values by default.

    The factory will return the resulting Topic object.
    """

    def factory(
        topic: str = None,
        num_partitions: int = 1,
        timeout: float = 10.0,
        create_topic: bool = True,
    ):
        if create_topic:
            topic_name, _ = topic_factory(
                topic=topic, num_partitions=num_partitions, timeout=timeout
            )
        else:
            topic_name = uuid.uuid4()
        return Topic(
            name=topic or topic_name,
            config=TopicConfig(num_partitions=num_partitions, replication_factor=1),
            value_deserializer=JSONDeserializer(),
            value_serializer=JSONSerializer(),
        )

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
def topic_admin(kafka_container):
    t = TopicAdmin(broker_address=kafka_container.broker_address)
    t.admin_client  # init the underlying admin so mocks can be applied whenever
    return t


@pytest.fixture()
def topic_manager_factory(topic_admin, random_consumer_group):
    """
    TopicManager with option to add an TopicAdmin (which uses Kafka Broker)
    """

    def factory(
        topic_admin_: Optional[TopicAdmin] = None,
        consumer_group: str = random_consumer_group,
        timeout: float = 10,
        create_timeout: float = 20,
    ) -> TopicManager:
        return TopicManager(
            topic_admin=topic_admin_ or topic_admin,
            consumer_group=consumer_group,
            timeout=timeout,
            create_timeout=create_timeout,
        )

    return factory


@pytest.fixture()
def topic_manager_topic_factory(topic_manager_factory):
    """
    Uses TopicManager to generate a Topic, create it, and return the Topic object
    """

    def factory(
        name: Optional[str] = None,
        partitions: int = 1,
        create_topic: bool = False,
        use_serdes_nones: bool = False,
        key_serializer: Optional[Union[Serializer, str]] = None,
        value_serializer: Optional[Union[Serializer, str]] = None,
        key_deserializer: Optional[Union[Deserializer, str]] = None,
        value_deserializer: Optional[Union[Deserializer, str]] = None,
        timestamp_extractor: Optional[TimestampExtractor] = None,
    ) -> Topic:
        name = name or str(uuid.uuid4())
        topic_manager = topic_manager_factory()
        topic_args = {
            "key_serializer": key_serializer,
            "value_serializer": value_serializer,
            "key_deserializer": key_deserializer,
            "value_deserializer": value_deserializer,
            "config": topic_manager.topic_config(num_partitions=partitions),
            "timestamp_extractor": timestamp_extractor,
        }
        if not use_serdes_nones:
            # will use the topic manager serdes defaults rather than "Nones"
            topic_args = {k: v for k, v in topic_args.items() if v is not None}
        topic = topic_manager.topic(name, **topic_args)
        if create_topic:
            topic_manager.create_all_topics()
        return topic

    return factory
