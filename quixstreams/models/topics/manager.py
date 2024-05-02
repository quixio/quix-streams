import logging
from typing import Dict, List, Optional, Set

from quixstreams.models.serializers import DeserializerType, SerializerType
from quixstreams.utils.dicts import dict_values
from .admin import TopicAdmin
from .exceptions import (
    TopicNameLengthExceeded,
    TopicConfigurationMismatch,
    TopicNotFoundError,
)
from .topic import Topic, TopicConfig, TimestampExtractor

logger = logging.getLogger(__name__)

__all__ = ("TopicManager",)


def affirm_ready_for_create(topics: List[Topic]):
    """
    Validate a list of topics is ready for creation attempt

    :param topics: list of `Topic`s
    """
    if invalid := [topic.name for topic in topics if not topic.config]:
        raise ValueError(f"configs for Topics {invalid} were NoneTypes")


class TopicManager:
    """
    The source of all topic management with quixstreams.

    Generally initialized and managed automatically by an `Application`,
    but allows a user to work with it directly when needed, such as using it alongside
    a plain `Producer` to create its topics.

    See methods for details.
    """

    _topic_partitions = 1
    _topic_replication = 1
    _max_topic_name_len = 255

    _topic_extra_config_defaults = {}
    _groupby_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}
    _changelog_extra_config_defaults = {"cleanup.policy": "compact"}
    _changelog_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}

    def __init__(
        self,
        topic_admin: TopicAdmin,
        consumer_group: str,
        create_timeout: int = 60,
    ):
        """
        :param topic_admin: an `Admin` instance (required for some functionality)
        :param consumer_group: the consumer group (of the `Application`)
        :param create_timeout: timeout for topic creation
        """
        self._admin = topic_admin
        self._consumer_group = consumer_group
        self._topics: Dict[str, Topic] = {}
        self._internal_topics: Dict[str, Topic] = {}
        self._changelog_topics: Dict[str, Dict[str, Topic]] = {}
        self._create_timeout = create_timeout

    @property
    def topics(self) -> Dict[str, Topic]:
        return self._topics

    @property
    def topics_list(self) -> List[Topic]:
        return list(self._topics.values())

    @property
    def _non_changelog_topics(self) -> Dict[str, Topic]:
        return {**self._topics, **self._internal_topics}

    @property
    def changelog_topics(self) -> Dict[str, Dict[str, Topic]]:
        """
        Note: `Topic`s are the changelogs.

        returns: the changelog topic dict, {topic_name: {suffix: Topic}}
        """
        return self._changelog_topics

    @property
    def changelog_topics_list(self) -> List[Topic]:
        return dict_values(self._changelog_topics)

    @property
    def all_topics(self) -> List[Topic]:
        return (
            self.topics_list
            + list(self._internal_topics.values())
            + self.changelog_topics_list
        )

    def _resolve_topic_name(self, name: str) -> str:
        """
        Here primarily for adjusting the topic name for Quix topics.

        :return: name, no changes (identity function)
        """
        return name

    def _validated_topic_name_length(self, name: str):
        if len(name) > self._max_topic_name_len:
            raise TopicNameLengthExceeded(
                f"Topic {name} exceeds the {self._max_topic_name_len} character limit"
            )
        return name

    def _internal_topic_name(
        self,
        name: str,
        topic_name: str,
        store_name: Optional[str] = None,
    ):
        """
        This naming scheme guarantees uniqueness across all independent `Application`s.

        :param name: a unique name for the internal topic (changelog, groupby, etc...)
        :param topic_name: name of consumed topic (app input topic)
        :param store_name: name of storage type (default, rolling10s, etc.)

        :return: formatted topic name
        """
        return self._validated_topic_name_length(
            self._resolve_topic_name(
                f"{name}__{'--'.join(filter(None, [self._consumer_group, topic_name, store_name]))}"
            )
        )

    def _create_topics(self, topics: List[Topic]):
        """
        Method that actually creates the topics in Kafka via an `Admin` instance.

        :param topics: list of `Topic`s
        """
        self._admin.create_topics(topics, timeout=self._create_timeout)

    def _get_source_topic_config(
        self, topic_name: str, extras_imports: Optional[Set[str]] = None
    ) -> TopicConfig:
        """
        Retrieve configs for a topic, defaulting to stored Topic objects if topic does
        not exist in Kafka.

        :param topic_name: name of the topic to get configs from

        :return: a TopicConfig
        """
        topic_config = (
            self._admin.inspect_topics([topic_name])[topic_name]
            or self._non_changelog_topics[topic_name].config
        )

        # Copy only certain configuration values from original topic
        if extras_imports:
            topic_config.extra_config = {
                k: v
                for k, v in topic_config.extra_config.items()
                if k in extras_imports
            }
        return topic_config

    def topic_config(
        self,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        extra_config: Optional[dict] = None,
    ) -> TopicConfig:
        """
        Convenience method for generating a `TopicConfig` with default settings

        :param num_partitions: the number of topic partitions
        :param replication_factor: the topic replication factor
        :param extra_config: other optional configuration settings

        :return: a TopicConfig object
        """
        return TopicConfig(
            num_partitions=num_partitions or self._topic_partitions,
            replication_factor=replication_factor or self._topic_replication,
            extra_config=extra_config or self._topic_extra_config_defaults,
        )

    def topic(
        self,
        name: str,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = "bytes",
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = "bytes",
        config: Optional[TopicConfig] = None,
        timestamp_extractor: Optional[TimestampExtractor] = None,
    ) -> Topic:
        """
        A convenience method for generating a `Topic`. Will use default config options
        as dictated by the TopicManager.

        :param name: topic name
        :param value_deserializer: a deserializer type for values
        :param key_deserializer: a deserializer type for keys
        :param value_serializer: a serializer type for values
        :param key_serializer: a serializer type for keys
        :param config: optional topic configurations (for creation/validation)
        :param timestamp_extractor: a callable that returns a timestamp in
            milliseconds from a deserialized message.

        :return: Topic object with creation configs
        """
        name = self._validated_topic_name_length(self._resolve_topic_name(name))

        if not config:
            config = TopicConfig(
                num_partitions=self._topic_partitions,
                replication_factor=self._topic_replication,
                extra_config=self._topic_extra_config_defaults,
            )
        topic = Topic(
            name=name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            config=config,
            timestamp_extractor=timestamp_extractor,
        )
        self._topics[name] = topic
        return topic

    def repartition_topic(
        self,
        operation: str,
        topic_name: str,
        store_name: Optional[str] = None,
        value_deserializer: Optional[DeserializerType] = "json",
        key_deserializer: Optional[DeserializerType] = "json",
        value_serializer: Optional[SerializerType] = "json",
        key_serializer: Optional[SerializerType] = "json",
    ) -> Topic:
        """
        Create an internal repartition topic.

        :param operation: name of the GroupBy operation (column name or user-defined).
        :param topic_name: name of the topic the GroupBy is sourced from.
        :param store_name: optional state store name for joins or aggregates.
        :param value_deserializer: a deserializer type for values; default - JSON
        :param key_deserializer: a deserializer type for keys; default - JSON
        :param value_serializer: a serializer type for values; default - JSON
        :param key_serializer: a serializer type for keys; default - JSON

        """
        name = self._internal_topic_name(
            f"repartition--{operation}", topic_name, store_name
        )
        topic = Topic(
            name=name,
            value_deserializer=value_deserializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            key_serializer=key_serializer,
            config=self._get_source_topic_config(
                topic_name,
                extras_imports=self._groupby_extra_config_imports_defaults,
            ),
        )
        self._internal_topics[name] = topic
        return topic

    def changelog_topic(
        self,
        topic_name: str,
        store_name: str,
    ) -> Topic:
        """
        Performs all the logic necessary to generate a changelog topic based on a
        "source topic" (aka input/consumed topic).

        Its main goal is to ensure partition counts of the to-be generated changelog
        match the source topic, and ensure the changelog topic is compacted. Also
        enforces the serialization type. All `Topic` objects generated with this are
        stored on the TopicManager.

        If source topic already exists, defers to the existing topic settings, else
        uses the settings as defined by the `Topic` (and its defaults) as generated
        by the `TopicManager`.

        In general, users should NOT need this; an Application knows when/how to
        generate changelog topics. To turn off changelogs, init an Application with
        "use_changelog_topics"=`False`.

        :param topic_name: name of consumed topic (app input topic)
            > NOTE: normally contain any prefixes added by TopicManager.topic()
        :param store_name: name of the store this changelog belongs to
            (default, rolling10s, etc.)

        :return: `Topic` object (which is also stored on the TopicManager)
        """

        topic_name = self._resolve_topic_name(topic_name)
        name = self._validated_topic_name_length(
            self._internal_topic_name("changelog", topic_name, store_name)
        )

        source_topic_config = self._get_source_topic_config(
            topic_name, extras_imports=self._changelog_extra_config_imports_defaults
        )
        source_topic_config.extra_config.update(self._changelog_extra_config_defaults)

        changelog_config = self.topic_config(
            num_partitions=source_topic_config.num_partitions,
            replication_factor=source_topic_config.replication_factor,
            extra_config=source_topic_config.extra_config,
        )

        topic = Topic(
            name=name,
            key_serializer="bytes",
            value_serializer="bytes",
            key_deserializer="bytes",
            value_deserializer="bytes",
            config=changelog_config,
        )
        self._changelog_topics.setdefault(topic_name, {})[store_name] = topic
        return topic

    def create_topics(self, topics: List[Topic]):
        """
        Creates topics via an explicit list of provided `Topics`.

        Exists as a way to manually specify what topics to create; otherwise,
        `create_all_topics()` is generally simpler.

        :param topics: list of `Topic`s
        """
        if not topics:
            logger.debug("No topics provided for creation...skipping!")
            return
        affirm_ready_for_create(topics)
        self._create_topics(topics)

    def create_all_topics(self):
        """
        A convenience method to create all Topic objects stored on this TopicManager.
        """
        self.create_topics(self.all_topics)

    def validate_all_topics(self):
        """
        Validates all topics exist and changelogs have correct topic and rep factor.

        Issues are pooled and raised as an Exception once inspections are complete.
        """
        logger.info(f"Validating Kafka topics exist and are configured correctly...")
        topics = self.all_topics
        changelog_names = [topic.name for topic in self.changelog_topics_list]
        actual_configs = self._admin.inspect_topics([t.name for t in topics])

        for topic in topics:
            # Validate that topic exists
            actual_topic_config = actual_configs[topic.name]
            if actual_topic_config is None:
                raise TopicNotFoundError(
                    f'Topic "{topic.name}" is not found on the broker'
                )

            # For changelog topics, validate the amount of partitions and
            # a replication factor match with the source topic
            if topic.name in changelog_names:
                # Ensure that changelog topic has the same amount of partitions and
                # replication factor as the source topic
                if topic.config.num_partitions != actual_topic_config.num_partitions:
                    raise TopicConfigurationMismatch(
                        f'Invalid partition count for the topic "{topic.name}": '
                        f"expected {topic.config.num_partitions}, "
                        f"got {actual_topic_config.num_partitions}"
                    )

                if (
                    topic.config.replication_factor
                    != actual_topic_config.replication_factor
                ):
                    raise TopicConfigurationMismatch(
                        f'Invalid replication factor for the topic "{topic.name}": '
                        f"expected {topic.config.replication_factor}, "
                        f"got {actual_topic_config.replication_factor}"
                    )
        logger.info(f"Kafka topics validation complete")
