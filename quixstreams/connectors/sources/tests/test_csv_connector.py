import pytest
import csv
import json
import uuid

from collections import namedtuple
from typing import Optional

from tests.containerhelper import ContainerHelper
from quixstreams.connectors.sources.csv_connector.connector import CsvSourceConnector

from quixstreams.kafka import (
    AutoOffsetReset,
    Consumer,
    AssignmentStrategy,
)

KafkaContainer = namedtuple("KafkaContainer", ("broker_address",))


@pytest.fixture(scope="session")
def csv_file(tmpdir_factory):
    row_headers = ["idx", "food", "qty"]
    data = ["0,apple,7", "1,banana,2"]
    data_dicts = [dict(zip(row_headers, row.split(","))) for row in data]
    filename = str(tmpdir_factory.mktemp("data").join("data.csv"))
    with open(filename, "w") as f:
        writer = csv.DictWriter(f, fieldnames=row_headers)
        writer.writeheader()
        writer.writerows(data_dicts)
    return filename, data_dicts


@pytest.fixture(scope="session")
def kafka_container() -> KafkaContainer:
    (
        kafka_container,
        broker_address,
        kafka_port,
        zookeeper_port,
    ) = ContainerHelper.create_kafka_container()
    print(f"Starting Kafka container on {broker_address}")
    ContainerHelper.start_kafka_container(kafka_container)
    print(f"Started Kafka container on {broker_address}")
    yield KafkaContainer(broker_address=broker_address)
    kafka_container.stop()


@pytest.fixture()
def random_consumer_group() -> str:
    return str(uuid.uuid4())


@pytest.fixture()
def consumer_factory(kafka_container, random_consumer_group):
    def factory(
        broker_address: str = kafka_container.broker_address,
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "earliest",
        assignment_strategy: AssignmentStrategy = "range",
        auto_commit_enable: bool = True,
        extra_config: dict = None,
    ) -> Consumer:
        consumer_group = consumer_group or random_consumer_group
        extra_config = extra_config or {}

        # Make consumers to refresh cluster metadata often
        # to react on re-assignment changes faster
        extra_config["topic.metadata.refresh.interval.ms"] = 3000

        return Consumer(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_commit_enable=auto_commit_enable,
            assignment_strategy=assignment_strategy,
            auto_offset_reset=auto_offset_reset,
            extra_config=extra_config,
        )

    return factory


@pytest.fixture()
def consumer(consumer_factory) -> Consumer:
    return consumer_factory()


def test_csv_connector(kafka_container, consumer, csv_file):
    file, data = csv_file
    topic_name = "test_topic"
    key_column = "food"

    with CsvSourceConnector(
        csv_path=file,
        producer_broker_address=kafka_container.broker_address,
        producer_topic_name=topic_name,
        key_extractor=key_column,
    ) as connector:
        connector.run()

    results = []
    consumer.subscribe([topic_name])
    while len(results) < len(data):
        if msg := consumer.poll(1):
            results.append(
                {"key": msg.key().decode(), "value": json.loads(msg.value())}
            )

    for i in range(len(results)):
        assert data[i] == results[i]["value"]
        assert results[i]["key"] == results[i]["value"][key_column]
