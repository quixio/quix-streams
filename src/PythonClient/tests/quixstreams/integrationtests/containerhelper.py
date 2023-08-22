from typing import Tuple
import random
import datetime
import time
import os

from testcontainers.core.container import DockerContainer


class ContainerHelper:

    @staticmethod
    def create_kafka_container() -> Tuple[DockerContainer, str, int, int]:
        """
        Returns (kafka container, broker list, kafka port, zookeeper port) tuple
        """
        kafka_address = "127.0.0.1"
        zookeeper_port = random.randint(12000, 15000)
        kafka_port = random.randint(16000, 20000)
        broker_list = '{0}:{1}'.format(kafka_address, kafka_port)

        archname = os.uname().machine
        if archname == 'arm64':
            kafka_container = DockerContainer(image='dougdonohoe/fast-data-dev:latest')
        else:
            kafka_container = DockerContainer(image='lensesio/fast-data-dev:3.3.1')
        kafka_container = kafka_container.with_env('BROKER_PORT', kafka_port)
        kafka_container = kafka_container.with_bind_ports(kafka_port, kafka_port)
        kafka_container = kafka_container.with_env('ZK_PORT', zookeeper_port)
        kafka_container = kafka_container.with_bind_ports(zookeeper_port, zookeeper_port)
        kafka_container = kafka_container.with_env('ADV_HOST', kafka_address)
        # disable rest
        kafka_container = kafka_container.with_env('REST_PORT', 0)
        kafka_container = kafka_container.with_env('WEB_PORT', 0)
        kafka_container = kafka_container.with_env('CONNECT_PORT', 0)
        kafka_container = kafka_container.with_env('REGISTRY_PORT', 0)
        kafka_container = kafka_container.with_env('RUNTESTS', 0)
        kafka_container = kafka_container.with_env('SAMPLEDATA', 0)
        kafka_container = kafka_container.with_env('FORWARDLOGS', 0)
        kafka_container = kafka_container.with_env('SUPERVISORWEB', 0)

        return (kafka_container, broker_list, kafka_port, zookeeper_port)

    @staticmethod
    def start_kafka_container(kafka_container: DockerContainer) -> None:
        kafka_container.start()
        start = datetime.datetime.utcnow()
        cut_off = start + datetime.timedelta(seconds=20)
        while cut_off > datetime.datetime.utcnow():
            time.sleep(0.5)
            logs = kafka_container.get_logs()
            if "success: broker entered RUNNING state" in logs[0].decode("utf-8") and \
                    "success: zookeeper entered RUNNING state" in logs[0].decode("utf-8"):
                return
        raise Exception("Failed to start container")
