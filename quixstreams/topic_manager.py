from typing import Dict, Callable, List, Mapping, Optional
import logging

from .kafka.admin import Admin
from .models.topics import Topic, TopicKafkaConfigs
from .models.serializers import DeserializerType, SerializerType


logger = logging.getLogger(__name__)

__all__ = ("TopicManager",)


def dict_values(d) -> List:
    if d:
        if isinstance(d, dict):
            return [i for v in d.values() for i in dict_values(v)]
        elif isinstance(d, list):
            return d
        return [d]
    return []


class TopicManager:
    _is_quix = False

    def __init__(self, admin_client: Admin, auto_create_topics: bool = True):
        self._admin_client = admin_client
        self._topics: Dict[str:Topic] = {}
        self._changelog_topics: Dict[str, Dict[str, Topic]] = {}
        self._auto_create_topics = auto_create_topics

    @property
    def topics(self) -> List[Topic]:
        return dict_values(self._topics)

    @property
    def changelog_topics(self) -> List[Topic]:
        return dict_values(self._changelog_topics)

    @property
    def _topic_creator(self) -> Callable[[List[Mapping]], None]:
        # TODO: add timeout arg
        return self._admin_client.create_topics

    @property
    def _topic_validator(self):
        # TODO: implement
        return None

    # TODO: maybe make it take a callable?
    def _apply_topic_prefix(self, name: str) -> str:
        return name

    def create_topics(self, topic_configs: List[TopicKafkaConfigs]):
        self._topic_creator([cfg.as_creation_config() for cfg in topic_configs])

    def validate_topics(self, topic_configs: List[TopicKafkaConfigs]):
        ...

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

    def _process_changelog_topic_configs(
        self,
        name: str,
        kafka_configs: Optional[TopicKafkaConfigs] = None,
        default_optionals: Optional[Mapping] = None,
    ) -> TopicKafkaConfigs:
        if default_optionals is None:
            default_optionals = {"cleanup.policy": "compact"}
        if kafka_configs:
            if cleanup_policy := kafka_configs.optionals.get("cleanup.policy"):
                if cleanup_policy not in ["compact", "compact,delete"]:
                    raise Exception(
                        "An invalid changelog topic 'cleanup.policy' "
                        "setting was defined"
                    )
                default_optionals.pop("cleanup.policy")
        return self._process_topic_configs(
            name=name, kafka_configs=kafka_configs, optional_overrides=default_optionals
        )

    def _process_topic_configs(
        self,
        name: str,
        kafka_configs: Optional[TopicKafkaConfigs] = None,
        optional_overrides: Optional[Mapping] = None,
    ) -> TopicKafkaConfigs:
        if not optional_overrides:
            optional_overrides = {}
        if self._auto_create_topics:
            if kafka_configs:
                return TopicKafkaConfigs(
                    name=name,
                    num_partitions=kafka_configs.num_partitions,
                    replication_factor=kafka_configs.replication_factor,
                    optionals={**kafka_configs.optionals, **optional_overrides},
                    is_quix_topic=self._is_quix,
                )
            return TopicKafkaConfigs(
                name=name, optionals=optional_overrides, is_quix_topic=self._is_quix
            )

    def topic(
        self,
        name: str,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = "bytes",
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = "bytes",
        kafka_configs: Optional[TopicKafkaConfigs] = None,
    ) -> Topic:
        name = self._apply_topic_prefix(name)
        topic = Topic(
            name=name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            kafka_configs=self._process_topic_configs(name, kafka_configs),
        )
        self._topics[name] = topic
        return topic

    def _format_changelog_name(
        self, consumer_group: str, source_topic_name: str, suffix: str
    ):
        return f"changelog__{consumer_group}--{source_topic_name}--{suffix}"

    def changelog_topic(
        self,
        source_topic_name: str,
        suffix: str,
        consumer_group: str,
        configs_to_import: tuple = ("retention.bytes", "retention.ms"),
    ) -> Topic:
        name = self._format_changelog_name(consumer_group, source_topic_name, suffix)
        # TODO: should prob use the topic info passed in at runtime to validate against
        # instead of defaulting to the values currently in the cluster
        topic_configs = (
            self._admin_client.inspect_topics([source_topic_name])[source_topic_name]
            or self._topics[source_topic_name].kafka_configs
        )

        topic = Topic(
            name=name,
            key_serializer="bytes",
            value_serializer="bytes",
            key_deserializer="bytes",
            value_deserializer="bytes",
            kafka_configs=self._process_changelog_topic_configs(
                name,
                TopicKafkaConfigs(
                    name=name,
                    num_partitions=topic_configs.num_partitions,
                    replication_factor=topic_configs.replication_factor,
                    optionals={
                        k: topic_configs.optionals[k]
                        for k in configs_to_import
                        if k in topic_configs.optionals
                    },
                ),
            ),
        )
        # TODO: think more about whether potential "overwrites" matter here
        print(f"adding changelog topic {topic.name}")
        self._changelog_topics.setdefault(source_topic_name, {})[suffix] = topic
        return topic
