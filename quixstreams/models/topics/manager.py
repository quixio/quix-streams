import logging
from itertools import chain
from typing import Dict, Iterable, List, Literal, Optional, Sequence

from quixstreams.models.serializers import DeserializerType, SerializerType

from .admin import TopicAdmin
from .exceptions import (
    TopicConfigurationMismatch,
    TopicNameLengthExceeded,
    TopicNotFoundError,
)
from .topic import TimestampExtractor, Topic, TopicConfig, TopicType

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

    _extra_config_imports = {
        "retention.bytes",
        "retention.ms",
        "cleanup.policy",
    }

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
        self._regular_topics: Dict[str, Topic] = {}
        self._repartition_topics: Dict[str, Topic] = {}
        self._changelog_topics: Dict[Optional[str], Dict[str, Topic]] = {}
        self._timeout = timeout
        self._create_timeout = create_timeout
        self._auto_create_topics = auto_create_topics

    @property
    def topics(self) -> Dict[str, Topic]:
        return self._regular_topics

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
        return {**self._regular_topics, **self._repartition_topics}

    @property
    def all_topics(self) -> Dict[str, Topic]:
        """
        Every registered topic name mapped to its respective `Topic`.

        returns: full topic dict, {topic_name: Topic}
        """
        all_topics_list = (
            list(self._regular_topics.values())
            + list(self._repartition_topics.values())
            + self.changelog_topics_list
        )
        return {topic.name: topic for topic in all_topics_list}

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

        topic = Topic(
            name=name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            create_config=create_config,
            timestamp_extractor=timestamp_extractor,
            topic_type=TopicType.REGULAR,
        )
        broker_topic = self._get_or_create_broker_topic(topic)
        topic = self._configure_topic(topic, broker_topic)
        self._regular_topics[topic.name] = topic
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
        broker_topic = self._get_or_create_broker_topic(topic)
        topic = self._configure_topic(topic, broker_topic)
        self._regular_topics[topic.name] = topic
        return topic

    def repartition_topic(
        self,
        operation: str,
        stream_id: str,
        config: TopicConfig,
        value_deserializer: Optional[DeserializerType] = "json",
        key_deserializer: Optional[DeserializerType] = "json",
        value_serializer: Optional[SerializerType] = "json",
        key_serializer: Optional[SerializerType] = "json",
    ) -> Topic:
        """
        Create an internal repartition topic.

        :param operation: name of the GroupBy operation (column name or user-defined).
        :param stream_id: stream id.
        :param config: a config for the repartition topic.
        :param value_deserializer: a deserializer type for values; default - JSON
        :param key_deserializer: a deserializer type for keys; default - JSON
        :param value_serializer: a serializer type for values; default - JSON
        :param key_serializer: a serializer type for keys; default - JSON

        :return: `Topic` object (which is also stored on the TopicManager)
        """
        topic = Topic(
            name=self._internal_name("repartition", stream_id, operation),
            value_deserializer=value_deserializer,
            key_deserializer=key_deserializer,
            value_serializer=value_serializer,
            key_serializer=key_serializer,
            create_config=config,
            topic_type=TopicType.REPARTITION,
        )
        broker_topic = self._get_or_create_broker_topic(topic)
        topic = self._configure_topic(topic, broker_topic)
        self._repartition_topics[topic.name] = topic
        return topic

    def changelog_topic(
        self,
        stream_id: Optional[str],
        store_name: str,
        config: TopicConfig,
    ) -> Topic:
        """
        Create and register a changelog topic for the given "stream_id" and store name.

        If the topic already exists, validate that the partition count
        is the same as requested.

        In general, users should NOT need this; an Application knows when/how to
        generate changelog topics. To turn off changelogs, init an Application with
        "use_changelog_topics"=`False`.

        :param stream_id: stream id
        :param store_name: name of the store this changelog belongs to
            (default, rolling10s, etc.)
        :param config: the changelog topic configuration

        :return: `Topic` object (which is also stored on the TopicManager)
        """
        # Always enable compaction for changelog topics
        config.extra_config.update({"cleanup.policy": "compact"})

        topic = Topic(
            name=self._internal_name("changelog", stream_id, store_name),
            key_serializer="bytes",
            value_serializer="bytes",
            key_deserializer="bytes",
            value_deserializer="bytes",
            create_config=config,
            topic_type=TopicType.CHANGELOG,
        )
        broker_topic = self._get_or_create_broker_topic(topic)
        topic = self._configure_topic(topic, broker_topic)
        if topic.broker_config.num_partitions != config.num_partitions:
            raise TopicConfigurationMismatch(
                f'Invalid partition count for the changelog topic "{topic.name}": '
                f"expected {config.num_partitions}, "
                f'got {topic.broker_config.num_partitions}"'
            )

        self._changelog_topics.setdefault(stream_id, {})[store_name] = topic
        return topic

    @classmethod
    def derive_topic_config(cls, topics: Iterable[Topic]) -> TopicConfig:
        """
        Derive a topic config based on one or more input Topic configs.
        To be used for generating the internal changelogs and repartition topics.

        This method expects that Topics contain "retention.ms" and "retention.bytes"
        config values.

        Multiple topics are expected for merged and joins streams.
        """
        if not topics:
            raise ValueError("At least one Topic must be passed")

        # Pick the maximum number of partitions across topics
        num_partitions = max(
            t.broker_config.num_partitions
            for t in topics
            if t.broker_config.num_partitions is not None
        )

        # Pick the maximum replication factor
        replication_factor = max(
            t.broker_config.replication_factor
            for t in topics
            if t.broker_config.replication_factor is not None
        )

        # Pick the maximum "retention.bytes" value across topics. -1 means infinity
        retention_bytes_values = [
            int(t.broker_config.extra_config["retention.bytes"]) for t in topics
        ]
        retention_bytes = (
            -1 if -1 in retention_bytes_values else max(retention_bytes_values)
        )

        # Pick maximum "retention.ms" value across topics. -1 means infinity
        retention_ms_values = [
            int(t.broker_config.extra_config["retention.ms"]) for t in topics
        ]
        retention_ms = -1 if -1 in retention_ms_values else max(retention_ms_values)

        return TopicConfig(
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            extra_config={
                "retention.bytes": str(retention_bytes),
                "retention.ms": str(retention_ms),
            },
        )

    def stream_id_from_topics(self, topics: Sequence[Topic]) -> str:
        """
        Generate a stream_id by combining names of the provided topics.
        """
        if not topics:
            raise ValueError("At least one Topic must be passed")

        return "--".join(sorted(t.name for t in topics))

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

    def _get_or_create_broker_topic(self, topic: Topic) -> Topic:
        """
        Validates the original topic name and returns the Topic from the broker
        either by fetching it or creating it if it doesn't exist.
        """
        try:
            return self._fetch_topic(topic=topic)
        except TopicNotFoundError:
            if not self._auto_create_topics:
                raise
            self._validate_topic_name(name=topic.name)
            self._create_topic(
                topic, timeout=self._timeout, create_timeout=self._create_timeout
            )
            return self._fetch_topic(topic=topic)

    def _configure_topic(self, topic: Topic, broker_topic: Topic) -> Topic:
        """
        Configure the topic with the correct broker config and extra config imports.

        Does more in QuixTopicManager.
        """
        broker_config = broker_topic.broker_config

        # Set a broker config for the topic
        broker_config = TopicConfig(
            num_partitions=broker_config.num_partitions,
            replication_factor=broker_config.replication_factor,
            extra_config={
                k: v
                for k, v in broker_config.extra_config.items()
                if k in self._extra_config_imports
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
