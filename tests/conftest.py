import logging.config
from collections import namedtuple
from typing import Generator

import pytest

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

KafkaContainer = namedtuple("KafkaContainer", ("broker_address",))

test_logger = logging.getLogger("quixstreams.tests")


@pytest.fixture(autouse=True, scope="session")
def configure_logging():
    logging.config.dictConfig(LOGGING_CONFIG)
    patch_logger_class()


@pytest.fixture(autouse=True)
def log_test_progress(request: pytest.FixtureRequest):
    test_logger.debug("Starting test %s", request.node.nodeid)


@pytest.fixture(scope="session")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    (
        kafka_container,
        broker_address,
        kafka_port,
    ) = ContainerHelper.create_kafka_container()
    test_logger.debug(f"Starting Kafka container on {broker_address}")
    ContainerHelper.start_kafka_container(kafka_container)
    test_logger.debug(f"Started Kafka container on {broker_address}")
    yield KafkaContainer(broker_address=broker_address)
    kafka_container.stop()
