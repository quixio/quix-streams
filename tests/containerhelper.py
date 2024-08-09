import base64
import datetime
import random
import time
import uuid
from typing import Tuple

from testcontainers.core.container import DockerContainer

from .compat import Network


class ContainerHelper:
    @staticmethod
    def create_kafka_container() -> Tuple[DockerContainer, str, str]:
        """
        Returns (kafka container, internal broker address, external broker address) tuple
        """
        docker_image_name = "confluentinc/cp-kafka:7.6.1"
        docker_hostname = uuid.uuid4().hex

        kafka_port = random.randint(16000, 20000)
        internal_broker_address = f"{docker_hostname}:9092"
        external_broker_address = f"127.0.0.1:{kafka_port}"

        kraft_cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode()

        kafka_container = (
            DockerContainer(image=docker_image_name, hostname=docker_hostname)
            .with_env("KAFKA_NODE_ID", "0")
            .with_env("KAFKA_PROCESS_ROLES", "controller,broker")
            .with_env(
                "KAFKA_LISTENERS",
                f"PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:{kafka_port}",
            )
            .with_env(
                "KAFKA_ADVERTISED_LISTENERS",
                f"PLAINTEXT://{internal_broker_address},EXTERNAL://{external_broker_address}",
            )
            .with_env(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            )
            .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", f"0@{docker_hostname}:9093")
            .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "10")
            .with_env("CLUSTER_ID", kraft_cluster_id)
            .with_env("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
            .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .with_bind_ports(kafka_port, kafka_port)
        )
        return kafka_container, internal_broker_address, external_broker_address

    @staticmethod
    def start_kafka_container(
        kafka_container: DockerContainer, network: Network
    ) -> None:
        kafka_container.start()
        network.connect(kafka_container.get_wrapped_container().id)
        wait_for_container_readiness(kafka_container, "Kafka Server started")

    @staticmethod
    def create_schema_registry_container(
        broker_address: str,
    ) -> Tuple[DockerContainer, str]:
        docker_image_name = "confluentinc/cp-schema-registry"

        schema_registry_port = random.randint(16000, 20000)
        schema_registry_address = f"http://0.0.0.0:{schema_registry_port}"

        schema_registry_container = (
            DockerContainer(image=docker_image_name)
            .with_env(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                f"PLAINTEXT://{broker_address}",
            )
            .with_env("SCHEMA_REGISTRY_LISTENERS", schema_registry_address)
            .with_env("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .with_bind_ports(schema_registry_port, schema_registry_port)
        )
        return schema_registry_container, schema_registry_address

    @staticmethod
    def start_schema_registry_container(
        schema_registry_container: DockerContainer,
        network: Network,
    ) -> None:
        schema_registry_container.start()
        network.connect(schema_registry_container.get_wrapped_container().id)
        wait_for_container_readiness(
            schema_registry_container, "Server started, listening for requests"
        )


def wait_for_container_readiness(container: DockerContainer, text: str) -> None:
    start = datetime.datetime.utcnow()
    cut_off = start + datetime.timedelta(seconds=20)
    while cut_off > datetime.datetime.utcnow():
        time.sleep(0.5)
        logs = container.get_logs()
        for line in logs:
            line = line.decode()
            if text in line:
                return
    raise TimeoutError("Failed to start container")
