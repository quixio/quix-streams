import pytest

import src.quixstreams as qx
from tests.quixstreams.integrationtests.containerhelper import ContainerHelper
from .types import KafkaContainer
from .logging import LOGGING_CONFIG, patch_logger_class
import logging.config

# Uncomment the next lines to enable low-level interop logs
# It will also create a log file
# from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
# InteropUtils.enable_debug()

qx.Logging.update_factory(qx.LogLevel.Critical)


@pytest.fixture(autouse=True)
def configure_logging():
    logging.config.dictConfig(LOGGING_CONFIG)
    patch_logger_class()


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
