import uuid

from testcontainers.core.docker_client import DockerClient


class Network:
    def __enter__(self) -> "Network":
        name = str(uuid.uuid4())
        self._network = DockerClient().client.networks.create(name)
        return self

    def __exit__(self, *_) -> None:
        self._network.remove()

    def connect(self, container_id: str) -> None:
        self._network.connect(container_id)
