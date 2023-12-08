import logging

from typing import List, Dict, Mapping, Optional, Union
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic  # type: ignore
from quixstreams.platforms.quix import QuixKafkaConfigsBuilder
from quixstreams.models import (
    Topic,
    TopicKafkaConfigs,
    SerializerType,
    DeserializerType,
)

logger = logging.getLogger(__name__)

__all__ = ("TopicAdmin",)


def dict_values(d) -> List:
    if d:
        if isinstance(d, dict):
            return [i for v in d.values() for i in dict_values(v)]
        elif isinstance(d, list):
            return d
        return [d]
    return []


class TopicAdmin:
    # TODO: consider putting only kafka-only functionality here, another class for topic management?
    def __init__(
        self,
        broker_address: str,
        consumer_group: str,
        extra_config: Optional[Mapping] = None,
        auto_create_topics: bool = True,
    ):
        self._inner_admin: Optional[AdminClient] = None
        self._config = {
            "bootstrap.servers": broker_address,
            **(extra_config or {}),
        }
        self._consumer_group = consumer_group
        self.quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None
        self._topics: Dict[str:Topic] = {}
        self._changelog_topics: Dict[str, Dict[str, Topic]] = {}
        self._auto_create_topics = auto_create_topics

    @property
    def _admin(self) -> AdminClient:
        if not self._inner_admin:
            self._inner_admin = AdminClient(self._config)
        return self._inner_admin

    @property
    def has_quix_builder(self) -> bool:
        return self.quix_config_builder is not None

    @property
    def topics(self) -> List[Topic]:
        return dict_values(self._topics)

    @property
    def changelog_topics(self) -> List[Topic]:
        return dict_values(self._changelog_topics)

    def inspect_topics(
        self, topics: List[str]
    ) -> Dict[str, Optional[TopicKafkaConfigs]]:
        futures_dict = {}
        cluster_topics = self._admin.list_topics().topics
        if existing_topics := [topic for topic in topics if topic in cluster_topics]:
            futures_dict = self._admin.describe_configs(
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
                is_quix_topic=self.has_quix_builder,
            )
            if topic in existing_topics
            else None
            for topic in topics
        }

    def _create_topics(self, topics: List[NewTopic]):
        # TODO: confirm topics created
        # TODO: decide if/where to filter pre-existing topics (how do I know topic is "ready" for non-quix?)
        return self._admin.create_topics(topics)

    def create_topics(self, topics: List[TopicKafkaConfigs]):
        if self.has_quix_builder:
            logger.debug("Creating topics via Quix API")
            creator = self.quix_config_builder.create_topics
        else:
            logger.debug(f"Creating topics via Kafka API")
            creator = self._create_topics
        creator([topic.as_creation_config() for topic in topics])

    def auto_create_or_validate_topics(self):
        topics = self.topics + self.changelog_topics
        topics = [topic.kafka_configs for topic in topics]
        logger.info(f"topics required for app operation: {[t.name for t in topics]}")
        if self._auto_create_topics:
            logger.info(
                "auto-creation is enabled: attempting to create required topics"
            )
            self.create_topics(topics)
        else:
            logger.info("auto-creation is disabled: validating required topics exist")
            self.validate_topics(topics)

    def validate_topics(self, topics: Union[List[Topic], List[TopicKafkaConfigs]]):
        # TODO: add non-quix validator
        if self.has_quix_builder:
            self.quix_config_builder.confirm_topics_exist(topics)

    def topic(
        self,
        name: str,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = "bytes",
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = "bytes",
        kafka_configs: Optional[TopicKafkaConfigs] = None,
    ) -> Topic:
        if self._auto_create_topics:
            if self.has_quix_builder:
                name = self.quix_config_builder.append_workspace_id(name)
            if kafka_configs:
                kafka_configs = TopicKafkaConfigs(
                    name=name,
                    num_partitions=kafka_configs.num_partitions,
                    replication_factor=kafka_configs.replication_factor,
                    optionals=kafka_configs.optionals,
                    is_quix_topic=self.has_quix_builder,
                )
            else:
                kafka_configs = TopicKafkaConfigs(
                    name=name, is_quix_topic=self.has_quix_builder
                )
        topic = Topic(
            name=name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            kafka_configs=kafka_configs,
        )
        self._topics[name] = topic
        return topic

    def changelog_topic(
        self,
        source_topic_name: str,
        suffix: str,
        configs_to_import: tuple = ("retention.bytes", "retention.ms"),
    ) -> Topic:
        topic_configs = (
            self.inspect_topics([source_topic_name])[source_topic_name]
            or self._topics[source_topic_name].kafka_configs
        )
        name = f"__changelog--{self._consumer_group}--{source_topic_name}--{suffix}"
        base_optionals = {} if self.has_quix_builder else {"cleanup.policy": "compact"}
        topic = Topic(
            name=name,
            key_serializer="bytes",
            value_serializer="bytes",
            key_deserializer="bytes",
            value_deserializer="bytes",
            kafka_configs=TopicKafkaConfigs(
                name=name,
                is_quix_topic=self.has_quix_builder,
                num_partitions=topic_configs.num_partitions,
                replication_factor=topic_configs.replication_factor,
                optionals={
                    **{
                        k: topic_configs.optionals[k]
                        for k in configs_to_import
                        if k in topic_configs.optionals
                    },
                    **base_optionals,
                },
            ),
        )
        self._changelog_topics.setdefault(source_topic_name, {}).setdefault(
            suffix, topic
        )
        return topic
