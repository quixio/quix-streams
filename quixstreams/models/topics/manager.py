import logging
from typing import Dict, List, Optional

from quixstreams.models.serializers import DeserializerType, SerializerType
from quixstreams.utils.dicts import dict_values
from .admin import TopicAdmin
from .exceptions import (
    TopicNameLengthExceeded,
    TopicConfigurationMismatch,
    TopicNotFoundError,
    TopicNameCollision,
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
    _changelog_extra_config_defaults = {"cleanup.policy": "compact"}
    _changelog_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}

    def __init__(
        self,
        topic_admin: TopicAdmin,
        create_timeout: int = 60,
    ):
        """
        :param topic_admin: an `Admin` instance (required for some functionality)
        :param create_timeout: timeout for topic creation
        """
        self._admin = topic_admin
        self._topics: Dict[str, Topic] = {}
        self._changelog_topics: Dict[str, Dict[str, Topic]] = {}
        self._create_timeout = create_timeout

    @property
    def topics(self) -> Dict[str, Topic]:
        return self._topics

    @property
    def topics_list(self) -> List[Topic]:
        return dict_values(self._topics)

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
        return self.topics_list + self.changelog_topics_list

    def _apply_topic_prefix(self, name: str) -> str:
        """
        Apply a prefix to the given name, or return if it already contains it.

        :param name: topic name

        :return: name with prefix added as required
        """
        return name

    def _validated_topic_name_length(self, name: str):
        if len(name) > self._max_topic_name_len:
            raise TopicNameLengthExceeded(
                f"Topic {name} exceeds the {self._max_topic_name_len} character limit"
            )
        return name

    def _internal_topic_suffix(
        self, consumer_group: str, topic_name: str, store_name: Optional[str] = None
    ):
        return "--".join(filter(None, [consumer_group, topic_name, store_name]))

    def _format_changelog_name(
        self, consumer_group: str, topic_name: str, store_name: str
    ):
        """
        Generate the name of the changelog topic based on the following parameters.

        This naming scheme guarantees uniqueness across all independent `Application`s.

        :param consumer_group: name of consumer group (for this app)
        :param topic_name: name of consumed topic (app input topic)
        :param store_name: name of storage type (default, rolling10s, etc.)

        :return: formatted topic name
        """
        return f"changelog__{self._internal_topic_suffix(consumer_group, topic_name, store_name)}"

    def _format_groupby_name(
        self,
        operation: str,
        consumer_group: str,
        topic_name: str,
        store_name: Optional[str] = None,
    ):
        return f"groupby__{operation}--{self._internal_topic_suffix(consumer_group, topic_name, store_name)}"

    def _create_topics(self, topics: List[Topic]):
        """
        Method that actually creates the topics in Kafka via an `Admin` instance.

        :param topics: list of `Topic`s
        """
        self._admin.create_topics(topics, timeout=self._create_timeout)

    def _get_source_topic_config(self, topic_name: str) -> TopicConfig:
        """
        Retrieve configs for a topic, defaulting to stored Topic objects if topic does
        not exist in Kafka.

        :param topic_name: name of the topic to get configs from

        :return: a TopicConfig
        """
        return (
            self._admin.inspect_topics([topic_name])[topic_name]
            or self._topics[topic_name].config
        )

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
        add_to_cache: bool = True,
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
        :param add_to_cache: whether to cache resulting Topic in the TopicManager

        :return: Topic object with creation configs
        """
        name = self._validated_topic_name_length(self._apply_topic_prefix(name))

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
        if add_to_cache:
            self._topics[name] = topic
        return topic

    def groupby_topic(
        self,
        operation: str,
        topic_name: str,
        consumer_group: str,
        store_name: Optional[str] = None,
        config_topic: Optional[Topic] = None,
    ) -> Topic:
        """
        Create a new "GroupBy" topic.

        Can optionally provide another Topic, which will be used to configure the
        GroupBy topic (besides the name).

        :param operation: name of the GroupBy operation (column name or user-defined).
        :param topic_name: name of the topic the GroupBy is sourced from.
        :param consumer_group: name of consumer group.
        :param store_name: optional state store name for joins or aggregates.
        :param config_topic: optional topic to configure the new GroupBy topic off of.

        # TODO: add non-private _key/_value serializer attributes on Topics?
        """
        name = self._format_groupby_name(
            operation, consumer_group, topic_name, store_name
        )
        if name in self._topics:
            raise TopicNameCollision(
                f"group_by name '{operation}' already in use; "
                f"choose a new unique value for StreamingDataFrame.group_by() "
                f"'name' parameter."
            )
        return self.topic(
            name=name,
            key_serializer=config_topic._key_serializer if config_topic else "json",
            value_serializer=config_topic._value_serializer if config_topic else "json",
            key_deserializer=config_topic._key_deserializer if config_topic else "json",
            value_deserializer=(
                config_topic._value_deserializer if config_topic else "json"
            ),
            config=(
                config_topic.config
                if config_topic
                else self._get_source_topic_config(topic_name)
            ),
        )

    def changelog_topic(
        self,
        topic_name: str,
        store_name: str,
        consumer_group: str,
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

        :param consumer_group: name of consumer group (for this app)
        :param topic_name: name of consumed topic (app input topic)
            > NOTE: normally contain any prefixes added by TopicManager.topic()
        :param store_name: name of the store this changelog belongs to
            (default, rolling10s, etc.)

        :return: `Topic` object (which is also stored on the TopicManager)
        """

        topic_name = self._apply_topic_prefix(topic_name)
        name = self._validated_topic_name_length(
            self._format_changelog_name(consumer_group, topic_name, store_name)
        )

        # Get a configuration of the source topic
        source_topic_config = self._get_source_topic_config(topic_name)

        # Copy only certain configuration values from original topic
        # to the changelog topic
        settings_to_import = {
            k: v
            for k, v in source_topic_config.extra_config.items()
            if k in self._changelog_extra_config_imports_defaults
        }
        extra_config = dict(settings_to_import)
        extra_config.update(self._changelog_extra_config_defaults)

        changelog_config = self.topic_config(
            num_partitions=source_topic_config.num_partitions,
            replication_factor=source_topic_config.replication_factor,
            extra_config=extra_config,
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
