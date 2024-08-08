import base64
import datetime
import random
import time
import uuid
from typing import Tuple

from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network


class ContainerHelper:
    @staticmethod
    def create_kafka_container(network: Network) -> Tuple[DockerContainer, str, str]:
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
            .with_network(network)
        )
        return kafka_container, internal_broker_address, external_broker_address

    @staticmethod
    def start_kafka_container(kafka_container: DockerContainer) -> None:
        kafka_container.start()
        start = datetime.datetime.utcnow()
        cut_off = start + datetime.timedelta(seconds=20)
        while cut_off > datetime.datetime.utcnow():
            time.sleep(0.5)
            logs = kafka_container.get_logs()
            for line in logs:
                line = line.decode()
                if "Kafka Server started" in line:
                    return
        raise TimeoutError("Failed to start container")
