import copy
import logging
from itertools import chain
from typing import Dict, List, Literal, Optional, Set

from quixstreams.models.serializers import DeserializerType, SerializerType

from .admin import TopicAdmin
from .exceptions import (
    TopicConfigurationMismatch,
    TopicNameLengthExceeded,
    TopicNotFoundError,
)
from .topic import TimestampExtractor, Topic, TopicConfig

logger = logging.getLogger(__name__)

__all__ = ("TopicManager",)


class TopicManager:
    """
    The source of all topic management for a Quix Streams Application.

    Intended only for internal use by Application.

    To create a Topic, use Application.topic() or generate them directly.
    """

    # Default topic params
    default_num_partitions: Optional[int] = 1
    default_replication_factor: Optional[int] = 1
    default_extra_config: dict[str, str] = {}

    # Max topic name length for the new topics
    _max_topic_name_len = 255

    _groupby_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}
    _changelog_extra_config_override = {"cleanup.policy": "compact"}
    _changelog_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}

    def __init__(
        self,
        topic_admin: TopicAdmin,
        consumer_group: str,
        timeout: float = 30,
        create_timeout: float = 60,
        auto_create_topics: bool = True,
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
        self._changelog_topics: Dict[Optional[str], Dict[str, Topic]] = {}
        self._timeout = timeout
        self._create_timeout = create_timeout
        self._auto_create_topics = auto_create_topics

    @property
    def _all_topics_list(self) -> List[Topic]:
        return (
            list(self._topics.values())
            + list(self._repartition_topics.values())
            + self.changelog_topics_list
        )

    @property
    def topics(self) -> Dict[str, Topic]:
        return self._topics

    @property
    def repartition_topics(self) -> Dict[str, Topic]:
        return self._repartition_topics

    @property
    def changelog_topics(self) -> Dict[Optional[str], Dict[str, Topic]]:
        """
        Note: `Topic`s are the changelogs.

        returns: the changelog topic dict, {topic_name: {suffix: Topic}}
        """
        return self._changelog_topics

    @property
    def changelog_topics_list(self) -> List[Topic]:
        """
        Returns a list of changelog topics

        returns: the changelog topic dict, {topic_name: {suffix: Topic}}
        """
        return list(chain(*(d.values() for d in self.changelog_topics.values())))

    @property
    def non_changelog_topics(self) -> Dict[str, Topic]:
        """
        Returns a dict with normal and repartition topics
        """
        return {**self._topics, **self._repartition_topics}

    @property
    def all_topics(self) -> Dict[str, Topic]:
        """
        Every registered topic name mapped to its respective `Topic`.

        returns: full topic dict, {topic_name: Topic}
        """
        return {topic.name: topic for topic in self._all_topics_list}

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
            # copy the default extra_config to ensure we don't mutate the default
            extra_config=extra_config or self.default_extra_config.copy(),
        )

    def topic(
        self,
        name: str,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = "bytes",
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = "bytes",
        create_config: Optional[TopicConfig] = None,
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
        :param create_config: optional topic configurations (for creation/validation)
        :param timestamp_extractor: a callable that returns a timestamp in
            milliseconds from a deserialized message.

        :return: Topic object with creation configs
        """
        if not create_config:
            create_config = TopicConfig(
                num_partitions=self.default_num_partitions,
                replication_factor=self.default_replication_factor,
                extra_config=self.default_extra_config,
            )

        topic = self._finalize_topic(
            Topic(
                name=name,
                value_serializer=value_serializer,
                value_deserializer=value_deserializer,
                key_serializer=key_serializer,
                key_deserializer=key_deserializer,
                create_config=create_config,
                timestamp_extractor=timestamp_extractor,
            )
        )
        self._topics[topic.name] = topic
        return topic

    def register(self, topic: Topic) -> Topic:
        """
        Register an already generated :class:`quixstreams.models.topics.Topic` to the topic manager.

        The topic name and config can be updated by the topic manager.

        :param topic: The topic to register
        """
        if topic.create_config is None:
            topic.create_config = TopicConfig(
                num_partitions=self.default_num_partitions,
                replication_factor=self.default_replication_factor,
                extra_config=self.default_extra_config,
            )
        topic = self._finalize_topic(topic)
        self._topics[topic.name] = topic
        return topic

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
        topic = self._finalize_topic(
            Topic(
                name=self._internal_name("repartition", topic_name, operation),
                value_deserializer=value_deserializer,
                key_deserializer=key_deserializer,
                value_serializer=value_serializer,
                key_serializer=key_serializer,
                create_config=self._get_source_topic_config(
                    topic_name,
                    extras_imports=self._groupby_extra_config_imports_defaults,
                    timeout=timeout if timeout is not None else self._timeout,
                ),
            )
        )
        self._repartition_topics[topic.name] = topic
        return topic

    def changelog_topic(
        self,
        topic_name: Optional[str],
        store_name: str,
        config: Optional[TopicConfig] = None,
        timeout: Optional[float] = None,
    ) -> Topic:
        """
        Performs all the logic necessary to generate a changelog topic based on an
        optional "source topic" (aka input/consumed topic).

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
        :param config: the changelog topic configuration. Default to `topic_name` configuration or TopicManager default
        :param timeout: config lookup timeout (seconds); Default 30

        :return: `Topic` object (which is also stored on the TopicManager)
        """
        if config is None:
            if topic_name is None:
                config = self.topic_config(
                    num_partitions=self.default_num_partitions,
                    replication_factor=self.default_replication_factor,
                )
            else:
                source_topic_config = self._get_source_topic_config(
                    topic_name,
                    extras_imports=self._changelog_extra_config_imports_defaults,
                    timeout=timeout if timeout is not None else self._timeout,
                )

                config = self.topic_config(
                    num_partitions=source_topic_config.num_partitions,
                    replication_factor=source_topic_config.replication_factor,
                    # copy the extra_config to ensure we don't mutate the source topic extra config
                    extra_config=source_topic_config.extra_config.copy(),
                )

        # always override some default configuration
        config.extra_config.update(self._changelog_extra_config_override)

        topic = self._finalize_topic(
            Topic(
                name=self._internal_name("changelog", topic_name, store_name),
                key_serializer="bytes",
                value_serializer="bytes",
                key_deserializer="bytes",
                value_deserializer="bytes",
                create_config=config,
            )
        )
        self._changelog_topics.setdefault(topic_name, {})[store_name] = topic
        return topic

    def validate_all_topics(self):
        """
        Validates that all topics have ".broker_config" set
        and changelog topics have correct numbers of partitions and replication factors.

        Issues are pooled and raised as an Exception once inspections are complete.
        """
        logger.info("Validating Kafka topics are configured correctly")
        for source_topic in self.non_changelog_topics.values():
            # For any changelog topics, validate the amount of partitions and
            # replication factor match with the source topic
            source_config = source_topic.broker_config
            for changelog_topic in self.changelog_topics.get(
                source_topic.name, {}
            ).values():
                changelog_config = changelog_topic.broker_config
                if changelog_config.num_partitions != source_config.num_partitions:
                    raise TopicConfigurationMismatch(
                        f'changelog topic "{changelog_topic.name}" partition count '
                        f'does not match its source topic "{source_topic.name}": '
                        f"expected {source_config.num_partitions}, "
                        f'got {changelog_config.num_partitions}"'
                    )
                if (
                    changelog_config.replication_factor
                    != source_config.replication_factor
                ):
                    raise TopicConfigurationMismatch(
                        f'changelog topic "{changelog_topic.name}" replication factor '
                        f'does not match its source topic "{source_topic.name}": '
                        f"expected {source_config.replication_factor}, "
                        f'got {changelog_config.replication_factor}"'
                    )

        logger.info("Kafka topics validation complete")

    def _validate_topic_name(self, name: str) -> None:
        """
        Validates the original topic name
        """
        if len(name) > self._max_topic_name_len:
            raise TopicNameLengthExceeded(
                f"'{name}' exceeds the {self._max_topic_name_len} character limit"
            )

    def _fetch_topic(self, topic: Topic) -> Topic:
        topic_name = topic.name
        actual_configs = self._admin.inspect_topics(
            [topic_name],
            timeout=self._timeout,
        )
        topic_config = actual_configs[topic_name]
        if topic_config is None:
            raise TopicNotFoundError(f'Topic "{topic_name}" not found on the broker')
        topic = Topic(name=topic_name)
        topic.broker_config = topic_config
        return topic

    def _finalize_topic(self, topic: Topic) -> Topic:
        """
        Validates the original topic name and returns the Topic.

        Does more in QuixTopicManager.
        """
        if self._auto_create_topics:
            self._validate_topic_name(name=topic.name)
            self._create_topic(
                topic, timeout=self._timeout, create_timeout=self._create_timeout
            )

        broker_topic = self._fetch_topic(topic=topic)
        broker_config = broker_topic.broker_config

        extra_config_imports = (
            self._groupby_extra_config_imports_defaults
            | self._changelog_extra_config_imports_defaults
        )

        # Set a broker config for the topic
        broker_config = TopicConfig(
            num_partitions=broker_config.num_partitions,
            replication_factor=broker_config.replication_factor,
            extra_config={
                k: v
                for k, v in broker_config.extra_config.items()
                if k in extra_config_imports
            },
        )
        topic.broker_config = broker_config
        return topic

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
        topic_name: Optional[str],
        suffix: str,
    ) -> str:
        """
        Generate an "internal" topic name.

        This naming scheme guarantees uniqueness across all independent `Application`s.

        The internal format is <{TYPE}__{GROUP}--{NAME}--{SUFFIX}>

        :param topic_type: topic type, added as prefix (changelog, repartition)
        :param topic_name: name of consumed topic, if exist (app input topic)
        :param suffix: a unique descriptor related to topic type, added as suffix

        :return: formatted topic name
        """

        if topic_name is None:
            parts = [self._consumer_group, suffix]
        else:
            nested_name = self._format_nested_name(topic_name)
            parts = [self._consumer_group, nested_name, suffix]

        return f"{topic_type}__{'--'.join(parts)}"

    def _create_topic(self, topic: Topic, timeout: float, create_timeout: float):
        """
        Method that actually creates the topics in Kafka via an `Admin` instance.

        :param topic: a Topic to create
        :param timeout: creation acknowledge timeout (seconds)
        :param create_timeout: topic finalization timeout (seconds)
        """
        self._admin.create_topics(
            [topic], timeout=timeout, finalize_timeout=create_timeout
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
        ]
        # If the source topic is not created yet, take its "create_config" instead
        if topic_config is None and topic_name in self.non_changelog_topics:
            topic_config = copy.deepcopy(
                self.non_changelog_topics[topic_name].create_config
            )

        if topic_config is None:
            raise RuntimeError(f"No configuration can be found for topic {topic_name}")

        # If "extra_imports" is present, copy the specified config values
        # from the original topic
        if extras_imports:
            extra_config = {
                k: v
                for k, v in topic_config.extra_config.items()
                if k in extras_imports
            }
            topic_config = TopicConfig(
                num_partitions=topic_config.num_partitions,
                replication_factor=topic_config.replication_factor,
                extra_config=extra_config,
            )
        return topic_config
