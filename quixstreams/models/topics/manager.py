import logging
from copy import deepcopy
from typing import Dict, List, Optional, Set, Literal

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

    # Default topic params
    default_num_partitions = 1
    default_replication_factor = 1
    default_extra_config = {}

    # Max topic name length for the new topics
    _max_topic_name_len = 255

    _groupby_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}
    _changelog_extra_config_defaults = {"cleanup.policy": "compact"}
    _changelog_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}

    def __init__(
        self,
        topic_admin: TopicAdmin,
        consumer_group: str,
        timeout: float = 30,
        create_timeout: float = 60,
    ):
        """
        :param topic_admin: an `Admin` instance (required for some functionality)
        :param consumer_group: the consumer group (of the `Application`)
        :param timeout: response timeout (seconds)
        :param create_timeout: timeout for topic creation
        """
        self._admin = topic_admin
        self._consumer_group = consumer_group
        self._topics: Dict[str, Topic] = {}
        self._repartition_topics: Dict[str, Topic] = {}
        self._changelog_topics: Dict[str, Dict[str, Topic]] = {}
        self._timeout = timeout
        self._create_timeout = create_timeout

    @property
    def _topics_list(self) -> List[Topic]:
        return list(self._topics.values())

    @property
    def _changelog_topics_list(self) -> List[Topic]:
        return dict_values(self._changelog_topics)

    @property
    def _non_changelog_topics(self) -> Dict[str, Topic]:
        return {**self._topics, **self._repartition_topics}

    @property
    def _all_topics_list(self) -> List[Topic]:
        return (
            self._topics_list
            + list(self._repartition_topics.values())
            + self._changelog_topics_list
        )

    @property
    def topics(self) -> Dict[str, Topic]:
        return self._topics

    @property
    def repartition_topics(self) -> Dict[str, Topic]:
        return self._repartition_topics

    @property
    def changelog_topics(self) -> Dict[str, Dict[str, Topic]]:
        """
        Note: `Topic`s are the changelogs.

        returns: the changelog topic dict, {topic_name: {suffix: Topic}}
        """
        return self._changelog_topics

    @property
    def all_topics(self) -> Dict[str, Topic]:
        """
        Every registered topic name mapped to its respective `Topic`.

        returns: full topic dict, {topic_name: Topic}
        """
        return {topic.name: topic for topic in self._all_topics_list}

    def _resolve_topic_name(self, name: str) -> str:
        """
        Here primarily for adjusting the topic name for Quix topics.

        Also validates topic name is not too long.

        :return: name, no changes (identity function)
        """
        if len(name) > self._max_topic_name_len:
            raise TopicNameLengthExceeded(
                f"Topic {name} exceeds the {self._max_topic_name_len} character limit"
            )
        return name

    def _format_nested_name(self, topic_name: str) -> str:
        """
        Reformat an "internal" topic name for its inclusion in _another_ internal topic.
        Part of this includes removing group name, which should only appear once.

        Goes from <{GROUP}__{TYPE}--{TOPIC}--{SUFFIX}> to <{TYPE}.{TOPIC}.{SUFFIX}>

        New "internal" topic uses this result for the {TOPIC} portion of its name.

        :param topic_name: the topic name

        :return: altered (if an "internal" topic name) or unaltered topic name
        """
        if f"__{self._consumer_group}--" in topic_name:
            return topic_name.replace(f"__{self._consumer_group}", "").replace(
                "--", "."
            )
        return topic_name

    def _internal_name(
        self,
        topic_type: Literal["changelog", "repartition"],
        topic_name: str,
        suffix: str,
    ) -> str:
        """
        Generate an "internal" topic name.

        This naming scheme guarantees uniqueness across all independent `Application`s.

        The internal format is <{TYPE}__{GROUP}--{NAME}--{SUFFIX}>

        :param topic_type: topic type, added as prefix (changelog, repartition)
        :param topic_name: name of consumed topic (app input topic)
        :param suffix: a unique descriptor related to topic type, added as suffix

        :return: formatted topic name
        """
        nested_name = self._format_nested_name(topic_name)
        return self._resolve_topic_name(
            f"{topic_type}__{'--'.join([self._consumer_group, nested_name, suffix])}"
        )

    def _create_topics(
        self, topics: List[Topic], timeout: float, create_timeout: float
    ):
        """
        Method that actually creates the topics in Kafka via an `Admin` instance.

        :param topics: list of `Topic`s
        :param timeout: creation acknowledge timeout (seconds)
        :param create_timeout: topic finalization timeout (seconds)
        """
        self._admin.create_topics(
            topics, timeout=timeout, finalize_timeout=create_timeout
        )

    def _get_source_topic_config(
        self,
        topic_name: str,
        timeout: float,
        extras_imports: Optional[Set[str]] = None,
    ) -> TopicConfig:
        """
        Retrieve configs for a topic, defaulting to stored Topic objects if topic does
        not exist in Kafka.

        :param topic_name: name of the topic to get configs from
        :param timeout: config lookup timeout (seconds); Default 30
        :param extras_imports: set of extra configs that should be imported from topic

        :return: a TopicConfig
        """
        topic_config = self._admin.inspect_topics([topic_name], timeout=timeout)[
            topic_name
        ] or deepcopy(self._non_changelog_topics[topic_name].config)

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
            num_partitions=num_partitions or self.default_num_partitions,
            replication_factor=replication_factor or self.default_replication_factor,
            extra_config=extra_config or self.default_extra_config,
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
        name = self._resolve_topic_name(name)

        if not config:
            config = TopicConfig(
                num_partitions=self.default_num_partitions,
                replication_factor=self.default_replication_factor,
                extra_config=self.default_extra_config,
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

    def register(self, topic: Topic):
        """
        Register an already generated :class:`quixstreams.models.topics.Topic` to the topic manager.

        The topic name and config can be updated by the topic manager.

        :param topic: The topic to register
        """
        topic.name = self._resolve_topic_name(topic.name)
        if topic.config is None:
            topic.config = TopicConfig(
                num_partitions=self.default_num_partitions,
                replication_factor=self.default_replication_factor,
                extra_config=self.default_extra_config,
            )
        self._topics[topic.name] = topic

    def repartition_topic(
        self,
        operation: str,
        topic_name: str,
        value_deserializer: Optional[DeserializerType] = "json",
        key_deserializer: Optional[DeserializerType] = "json",
        value_serializer: Optional[SerializerType] = "json",
        key_serializer: Optional[SerializerType] = "json",
        timeout: Optional[float] = None,
    ) -> Topic:
        """
        Create an internal repartition topic.

        :param operation: name of the GroupBy operation (column name or user-defined).
        :param topic_name: name of the topic the GroupBy is sourced from.
        :param value_deserializer: a deserializer type for values; default - JSON
        :param key_deserializer: a deserializer type for keys; default - JSON
        :param value_serializer: a serializer type for values; default - JSON
        :param key_serializer: a serializer type for keys; default - JSON
        :param timeout: config lookup timeout (seconds); Default 30

        :return: `Topic` object (which is also stored on the TopicManager)
        """
        name = self._internal_name(f"repartition", topic_name, operation)

        topic = Topic(
            name=name,
            value_deserializer=value_deserializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            key_serializer=key_serializer,
            config=self._get_source_topic_config(
                topic_name,
                extras_imports=self._groupby_extra_config_imports_defaults,
                timeout=timeout if timeout is not None else self._timeout,
            ),
        )
        self._repartition_topics[name] = topic
        return topic

    def changelog_topic(
        self,
        topic_name: str,
        store_name: str,
        timeout: Optional[float] = None,
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
        :param timeout: config lookup timeout (seconds); Default 30

        :return: `Topic` object (which is also stored on the TopicManager)
        """

        topic_name = self._resolve_topic_name(topic_name)
        source_topic_config = self._get_source_topic_config(
            topic_name,
            extras_imports=self._changelog_extra_config_imports_defaults,
            timeout=timeout if timeout is not None else self._timeout,
        )
        source_topic_config.extra_config.update(self._changelog_extra_config_defaults)

        changelog_config = self.topic_config(
            num_partitions=source_topic_config.num_partitions,
            replication_factor=source_topic_config.replication_factor,
            extra_config=source_topic_config.extra_config,
        )

        topic = Topic(
            name=self._internal_name("changelog", topic_name, store_name),
            key_serializer="bytes",
            value_serializer="bytes",
            key_deserializer="bytes",
            value_deserializer="bytes",
            config=changelog_config,
        )
        self._changelog_topics.setdefault(topic_name, {})[store_name] = topic
        return topic

    def create_topics(
        self,
        topics: List[Topic],
        timeout: Optional[float] = None,
        create_timeout: Optional[float] = None,
    ):
        """
        Creates topics via an explicit list of provided `Topics`.

        Exists as a way to manually specify what topics to create; otherwise,
        `create_all_topics()` is generally simpler.

        :param topics: list of `Topic`s
        :param timeout: creation acknowledge timeout (seconds); Default 30
        :param create_timeout: topic finalization timeout (seconds); Default 60
        """
        if not topics:
            logger.debug("No topics provided for creation...skipping!")
            return
        affirm_ready_for_create(topics)
        self._create_topics(
            topics,
            timeout=timeout if timeout is not None else self._timeout,
            create_timeout=(
                create_timeout if create_timeout is not None else self._create_timeout
            ),
        )

    def create_all_topics(
        self, timeout: Optional[float] = None, create_timeout: Optional[float] = None
    ):
        """
        A convenience method to create all Topic objects stored on this TopicManager.

        :param timeout: creation acknowledge timeout (seconds); Default 30
        :param create_timeout: topic finalization timeout (seconds); Default 60
        """
        self.create_topics(
            self._all_topics_list, timeout=timeout, create_timeout=create_timeout
        )

    def validate_all_topics(self, timeout: Optional[float] = None):
        """
        Validates all topics exist and changelogs have correct topic and rep factor.

        Issues are pooled and raised as an Exception once inspections are complete.
        """
        logger.info(f"Validating Kafka topics exist and are configured correctly...")
        all_topic_names = [t.name for t in self._all_topics_list]
        actual_configs = self._admin.inspect_topics(
            all_topic_names,
            timeout=timeout if timeout is not None else self._timeout,
        )

        if missing := [t for t in all_topic_names if actual_configs[t] is None]:
            raise TopicNotFoundError(f"Topics {missing} not found on the broker")

        for source_name in self._non_changelog_topics.keys():
            source_cfg = actual_configs[source_name]
            # For any changelog topics, validate the amount of partitions and
            # replication factor match with the source topic
            for changelog in self.changelog_topics.get(source_name, {}).values():
                changelog_cfg = actual_configs[changelog.name]

                if changelog_cfg.num_partitions != source_cfg.num_partitions:
                    raise TopicConfigurationMismatch(
                        f'changelog topic "{changelog.name}" partition count '
                        f'does not match its source topic "{source_name}": '
                        f"expected {source_cfg.num_partitions}, "
                        f'got {changelog_cfg.num_partitions}"'
                    )
                if changelog_cfg.replication_factor != source_cfg.replication_factor:
                    raise TopicConfigurationMismatch(
                        f'changelog topic "{changelog.name}" replication factor '
                        f'does not match its source topic "{source_name}": '
                        f"expected {source_cfg.replication_factor}, "
                        f'got {changelog_cfg.replication_factor}"'
                    )

        logger.info(f"Kafka topics validation complete")
