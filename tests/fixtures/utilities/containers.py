import logging
from typing import Any, Generator

import pytest
from testcontainers.core.network import Network

from tests.utilities.containers import (
    ContainerHelper,
    KafkaContainer,
    SchemaRegistryContainer,
)

test_logger = logging.getLogger("quixstreams.tests")


@pytest.fixture(scope="session")
def network():
    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def schema_registry_container(
    network: Network, kafka_container: KafkaContainer
) -> Generator[SchemaRegistryContainer, None, None]:
    container, schema_registry_address = (
        ContainerHelper.create_schema_registry_container(
            network, kafka_container.internal_broker_address
        )
    )
    test_logger.debug(
        f"Starting Schema Registry container on {schema_registry_address}"
    )
    ContainerHelper.start_schema_registry_container(container)
    test_logger.debug(f"Started Schema Registry container on {schema_registry_address}")
    yield SchemaRegistryContainer(schema_registry_address=schema_registry_address)
    container.stop()


@pytest.fixture(scope="session")
def kafka_container_factory(network: Network) -> Generator[KafkaContainer, Any, None]:
    def factory() -> KafkaContainer:
        (
            kafka_container,
            internal_broker_address,
            external_broker_address,
        ) = ContainerHelper.create_kafka_container(network)
        test_logger.debug(f"Starting Kafka container on {external_broker_address}")
        ContainerHelper.start_kafka_container(kafka_container)
        test_logger.debug(f"Started Kafka container on {external_broker_address}")
        yield KafkaContainer(
            broker_address=external_broker_address,
            internal_broker_address=internal_broker_address,
        )
        kafka_container.stop()

    return factory


@pytest.fixture(scope="session")
def kafka_container(kafka_container_factory) -> KafkaContainer:
    yield from kafka_container_factory()


class ExternalKafkaFixture:
    NUMBER_OF_MESSAGES = 10

    @pytest.fixture(scope="class")
    def external_kafka_container(self, kafka_container_factory):
        yield from kafka_container_factory()

    @pytest.fixture(autouse=True)
    def external_kafka(self, external_kafka_container):
        self._external_broker_address = external_kafka_container.broker_address

    @pytest.fixture()
    def app(self, app_factory):
        return app_factory(auto_offset_reset="earliest", request_timeout=1)
