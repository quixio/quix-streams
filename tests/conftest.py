import logging.config
from collections import namedtuple
from typing import Generator

import pytest
from testcontainers.core.network import Network

from .containerhelper import ContainerHelper
from .logging import LOGGING_CONFIG, patch_logger_class

# Define list of files with fixtures for pytest autodiscovery
pytest_plugins = [
    "tests.test_quixstreams.test_dataframe.fixtures",
    "tests.test_quixstreams.fixtures",
    "tests.test_quixstreams.test_models.fixtures",
    "tests.test_quixstreams.test_platforms.test_quix.fixtures",
    "tests.test_quixstreams.test_state.fixtures",
    "tests.test_quixstreams.test_state.test_rocksdb.fixtures",
    "tests.test_quixstreams.test_state.test_rocksdb.test_windowed.fixtures",
]

KafkaContainer = namedtuple(
    "KafkaContainer",
    ["broker_address", "internal_broker_address"],
)

test_logger = logging.getLogger("quixstreams.tests")


@pytest.fixture(autouse=True, scope="session")
def configure_logging():
    logging.config.dictConfig(LOGGING_CONFIG)
    patch_logger_class()


@pytest.fixture(autouse=True)
def log_test_progress(request: pytest.FixtureRequest):
    test_logger.debug("Starting test %s", request.node.nodeid)


@pytest.fixture(scope="session")
def network():
    with Network() as network:
        yield network


@pytest.fixture(scope="session")
def kafka_container(network: Network) -> Generator[KafkaContainer, None, None]:
    (
        container,
        internal_broker_address,
        external_broker_address,
    ) = ContainerHelper.create_kafka_container(network)
    test_logger.debug(f"Starting Kafka container on {external_broker_address}")
    ContainerHelper.start_kafka_container(container)
    test_logger.debug(f"Started Kafka container on {external_broker_address}")
    yield KafkaContainer(
        broker_address=external_broker_address,
        internal_broker_address=internal_broker_address,
    )
    container.stop()
