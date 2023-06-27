import pytest

from src import quixstreams as qx
from src.quixstreams import AutoOffsetReset
from .containerhelper import ContainerHelper
from .types import KafkaContainer


@pytest.fixture()
def test_name(request: pytest.FixtureRequest) -> str:
    return '.'.join(
        (
            request.cls.__name__ if request.cls else None,
            request.function.__name__
        )
    )


@pytest.fixture(scope='session')
def kafka_container() -> KafkaContainer:
    kafka_container, broker_address, kafka_port, zookeeper_port = ContainerHelper.create_kafka_container()
    print("Starting Kafka container")
    ContainerHelper.start_kafka_container(kafka_container)
    print("Started Kafka container")
    yield KafkaContainer(broker_address=broker_address)
    kafka_container.stop()


@pytest.fixture()
def kafka_streaming_client(kafka_container: KafkaContainer) -> qx.KafkaStreamingClient:
    client = qx.KafkaStreamingClient(
        broker_address=kafka_container.broker_address,
        security_options=None
    )
    with client:
        yield client


@pytest.fixture()
def topic_name(request: pytest.FixtureRequest) -> str:
    """
    Use test ID as a topic name for a given test
    """
    name_parts = [
        request.module.__name__,
        request.cls.__name__ if request.cls else None,
        request.function.__name__
    ]
    name = '.'.join(p for p in name_parts if p)
    return name


@pytest.fixture()
def topic_consumer_earliest(
        topic_name: str,
        kafka_streaming_client: qx.KafkaStreamingClient
):
    # Use test ID as a topic name

    topic_consumer = kafka_streaming_client.get_topic_consumer(
        topic=topic_name,
        consumer_group='irrelevant',
        auto_offset_reset=AutoOffsetReset.Earliest
    )
    yield topic_consumer
    topic_consumer.dispose()


@pytest.fixture()
def topic_producer(
        topic_name: str,
        kafka_streaming_client: qx.KafkaStreamingClient
):
    topic_producer = kafka_streaming_client.get_topic_producer(
        topic=topic_name,
    )
    yield topic_producer
    topic_producer.dispose()


@pytest.fixture()
def raw_topic_consumer(
        topic_name: str,
        kafka_streaming_client: qx.KafkaStreamingClient
):
    topic_consumer = kafka_streaming_client.get_raw_topic_consumer(
        topic=topic_name,
        consumer_group='irrelevant',
        auto_offset_reset=AutoOffsetReset.Earliest
    )
    yield topic_consumer
    topic_consumer.dispose()


@pytest.fixture()
def raw_topic_producer(
        topic_name: str,
        kafka_streaming_client: qx.KafkaStreamingClient
):
    topic_producer = kafka_streaming_client.get_raw_topic_producer(
        topic=topic_name,
    )
    yield topic_producer
    topic_producer.dispose()
