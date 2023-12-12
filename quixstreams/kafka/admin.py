import logging

from typing import List, Dict, Mapping, Optional
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic  # type: ignore
from quixstreams.models import TopicKafkaConfigs

logger = logging.getLogger(__name__)

__all__ = ("Admin",)


class Admin:
    def __init__(
        self,
        broker_address: str,
        extra_config: Optional[Mapping] = None,
    ):
        self._inner_admin: Optional[AdminClient] = None
        self._config = {
            "bootstrap.servers": broker_address,
            **(extra_config or {}),
        }

    @property
    def _admin_client(self) -> AdminClient:
        if not self._inner_admin:
            self._inner_admin = AdminClient(self._config)
        return self._inner_admin

    def inspect_topics(
        self, topics: List[str]
    ) -> Dict[str, Optional[TopicKafkaConfigs]]:
        futures_dict = {}
        cluster_topics = self._admin_client.list_topics().topics
        if existing_topics := [topic for topic in topics if topic in cluster_topics]:
            futures_dict = self._admin_client.describe_configs(
                [ConfigResource(2, topic) for topic in existing_topics]
            )
        configs = {
            config_resource.name: {c.name: c.value for c in config.result().values()}
            for config_resource, config in futures_dict.items()
        }
        return {
            topic: TopicKafkaConfigs(
                name=topic,
                num_partitions=len(cluster_topics[topic].partitions),
                replication_factor=len(cluster_topics[topic].partitions[0].replicas),
                optionals=configs[topic],
            )
            if topic in existing_topics
            else None
            for topic in topics
        }

    def create_topics(self, topics: List[NewTopic]):
        # TODO: confirm topics created
        # TODO: decide if/where to filter pre-existing topics (how do I know topic is "ready" for non-quix?)
        return self._admin_client.create_topics(topics)
