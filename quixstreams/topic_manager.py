import logging
import pprint
from typing import Dict, List, Mapping, Optional, Set, Literal

from quixstreams.platforms.quix import QuixKafkaConfigsBuilder
from .kafka.admin import Admin
from .models.serializers import DeserializerType, SerializerType
from .models.topics import Topic, TopicConfig, TopicList, TopicMap

logger = logging.getLogger(__name__)

__all__ = ("TopicManager",)


def dict_values(d: object) -> List:
    """
    Recursively unpacks a set of nested dicts to get a flattened list of leaves,
    where "leaves" are the first non-dict item.

    i.e {"a": {"b": {"c": 1}, "d": 2}, "e": 3} becomes [1, 2, 3]

    :param d: initially, a dict (with potentially nested dicts)

    :return: a list with all the leaves of the various contained dicts
    """
    if d:
        if isinstance(d, dict):
            return [i for v in d.values() for i in dict_values(v)]
        elif isinstance(d, list):
            return d
        return [d]
    return []


def affirm_ready_for_create(topics: TopicList):
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

    _topic_partitions = 2
    _topic_replication = 1

    _topic_extra_config_defaults = {}
    _changelog_extra_config_defaults = {"cleanup.policy": "compact"}
    _changelog_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}

    def __init__(
        self,
        admin_client: Admin,
        create_timeout: int = 60,
    ):
        """
        :param admin_client: an `Admin` instance
        :param create_timeout: timeout for topic creation
        """
        self._admin_client = admin_client
        self._topics: TopicMap = {}
        self._changelog_topics: Dict[str, TopicMap] = {}
        self._create_timeout = create_timeout

    @classmethod
    def Quix(
        cls,
        admin_client: Optional[Admin] = None,
        create_timeout: int = 60,
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
    ) -> "QuixTopicManager":
        return QuixTopicManager(
            admin_client=admin_client,
            create_timeout=create_timeout,
            quix_config_builder=quix_config_builder,
        )

    class TopicValidationError(Exception):
        ...

    class MissingTopicForChangelog(Exception):
        ...

    @property
    def topics(self) -> TopicList:
        return dict_values(self._topics)

    @property
    def changelog_topics(self) -> TopicList:
        return dict_values(self._changelog_topics)

    @property
    def all_topics(self) -> TopicList:
        return self.topics + self.changelog_topics

    @property
    def pretty_formatted_topic_configs(self):
        return pprint.pformat(
            {topic.name: topic.config.__dict__ for topic in self.all_topics}
        )

    def _topic_config_with_defaults(
        self,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        extra_config: Optional[Mapping] = None,
        extra_config_defaults: Optional[Mapping] = None,
    ):
        """
        Generates a TopicConfig with default settings. Also hides unneeded user
        option "extra_config_defaults"

        :param num_partitions: the number of topic partitions
        :param replication_factor: the topic replication factor
        :param extra_config: other optional configuration settings
        :param extra_config_defaults: a way to override what the extra_config defaults
            should be; generally used for swapping to changelog topic defaults.

        :return: a TopicConfig object
        """
        topic_config = TopicConfig(
            num_partitions=num_partitions or self._topic_partitions,
            replication_factor=replication_factor or self._topic_replication,
            extra_config=extra_config,
        )
        if extra_config_defaults is None:
            extra_config_defaults = self._topic_extra_config_defaults
        topic_config.update_extra_config(defaults=extra_config_defaults)
        return topic_config

    def topic_config(
        self,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        extra_config: Optional[Mapping] = None,
    ) -> TopicConfig:
        """
        Convenience method for generating a `TopicConfig` with default settings

        :param num_partitions: the number of topic partitions
        :param replication_factor: the topic replication factor
        :param extra_config: other optional configuration settings

        :return: a TopicConfig object
        """
        return self._topic_config_with_defaults(
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            extra_config=extra_config,
        )

    def _apply_topic_prefix(self, name: str) -> str:
        """
        Apply a prefix to the given name

        Intended for easy replacement via inheritance (QuixTopicManager).

        :param name: topic name

        :return: name with added prefix (no change in this case)
        """
        return name

    def _create_topics(self, topics: TopicList):
        """
        Method that actually creates the topics in Kafka via an `Admin` instance.

        Intended for easy replacement via inheritance (QuixTopicManager).

        :param topics: list of `Topic`s
        """
        # TODO: have create topics return list of topics created to speed up validation
        self._admin_client.create_topics(topics, timeout=self._create_timeout)

    def create_topics(self, topics: TopicList):
        """
        Creates topics via an explicit list of provided `Topics`.

        Exists as a way to manually specify what topics to create; otherwise,
        `create_all_topics()` is generally simpler.

        :param topics: list of `Topic`s
        """
        logger.info("Creating topics...")
        affirm_ready_for_create(topics)
        return self._create_topics(topics)

    def create_all_topics(self):
        """
        A convenience method to create all Topic objects stored on this TopicManager.
        """
        self.create_topics(self.all_topics)

    def validate_topics(
        self,
        topics: List[Topic],
        validation_level: Optional[Literal["exists", "required", "all"]] = "exists",
    ):
        """
        Validates topics via an explicit list of `Topic`s.

        Issues are pooled and raised as an Exception once all inspections are completed.

        Can specify the degree of validation, but the default behavior is checking
        that the partition counts and replication factors match what is in Kafka.

        :param topics: list of `Topic`s
        :param validation_level: The degree of topic validation; Default - "exists"
            None - No validation.
            "exists" - Confirm expected topics exist.
            "required" - Confirm topics match your provided `Topic`
                partition + replication factor
            "all" - Confirm topic settings are EXACT.
        """
        if not validation_level:
            logger.info("Skipping topic validation...")
            return
        logger.info(f"Validating topics at level '{validation_level}'...")
        exists_only = validation_level == "exists"
        extras = validation_level == "all"
        issues = {}
        actual_configs = self._admin_client.inspect_topics([t.name for t in topics])
        for topic in topics:
            expected = topic.config
            actual = actual_configs[topic.name]
            if topic.name in actual_configs:
                if not exists_only:
                    if extras:
                        actual.update_extra_config(
                            allowed={k for k in expected.extra_config}
                        )
                    else:
                        actual.extra_config = expected.extra_config
                    if expected != actual:
                        issues[topic.name] = {
                            "expected": expected.__dict__,
                            "actual": actual.__dict__,
                        }
            else:
                issues[topic.name] = "TOPIC MISSING"
        if issues:
            raise self.TopicValidationError(
                f"the following topics had issues:\n{pprint.pformat(issues)}"
            )
        logger.info("All topics validated!")

    def validate_all_topics(
        self,
        validation_level: Optional[Literal["exists", "required", "all"]] = "exists",
    ):
        """
        A convenience method for validating all `Topic`s stored on this TopicManager.

        See `TopicManager.validate_topics()` for more details.

        :param validation_level: The degree of topic validation; Default - "exists"
            None - No validation.
            "exists" - Confirm expected topics exist.
            "required" - Confirm topics match your provided `Topic`
                partition + replication factor
            "all" - Confirm topic settings are EXACT.
        """
        self.validate_topics(topics=self.all_topics, validation_level=validation_level)

    def _process_topic_configs(
        self,
        topic_config: Optional[TopicConfig] = None,
        extra_config_defaults: Optional[Mapping] = None,
        auto_create_config: bool = True,
    ) -> Optional[TopicConfig]:
        """
        Helps parse `TopicConfigs` by creating them if needed and adding swapping
        out extra_config defaults (for changelog topics).

        :param topic_config: a starting `TopicConfig` object, else generate one based
            on "auto_create_config"
        :param extra_config_defaults: override class extra_config defaults with these
        :param auto_create_config: if no "topic_config", create one; Default - True
            > NOTE: this setting is generally manipulated by the Application class via
              its "auto_create_topics" option.

        :return: TopicConfig or None, depending on function arguments
        """
        if topic_config:
            return self._topic_config_with_defaults(
                num_partitions=topic_config.num_partitions,
                replication_factor=topic_config.replication_factor,
                extra_config=topic_config.extra_config,
                extra_config_defaults=extra_config_defaults,
            )
        else:
            if not auto_create_config:
                return
            return self._topic_config_with_defaults(
                extra_config_defaults=extra_config_defaults,
            )

    def topic(
        self,
        name: str,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = "bytes",
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = "bytes",
        config: Optional[TopicConfig] = None,
        auto_create_config: bool = True,
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
        :param auto_create_config: if no "topic_config", create one; Default - True
            > NOTE: this setting is generally manipulated by the Application class via
              its "auto_create_topics" option.

        :return: Topic object with creation configs
        """
        name = self._apply_topic_prefix(name)
        topic = Topic(
            name=name,
            value_serializer=value_serializer,
            value_deserializer=value_deserializer,
            key_serializer=key_serializer,
            key_deserializer=key_deserializer,
            config=self._process_topic_configs(
                config, auto_create_config=auto_create_config
            ),
        )
        self._topics[name] = topic
        return topic

    def _format_changelog_name(
        self, consumer_group: str, source_topic_name: str, suffix: str
    ):
        """
        Generate the name of the changelog topic based on the following parameters.

        This naming scheme guarantees uniqueness across all independent `Application`s.

        Intended for easy replacement via inheritance (QuixTopicManager).

        :param consumer_group: name of consumer group (for this app)
        :param source_topic_name: name of consumed topic (app input topic)
        :param suffix: name of storage type (default, rolling10s, etc.)

        :return: formatted topic name
        """
        return f"changelog__{consumer_group}--{source_topic_name}--{suffix}"

    def changelog_topic(
        self,
        source_topic_name: str,
        suffix: str,
        consumer_group: str,
        configs_to_import: Set[str] = None,
    ) -> Topic:
        """
        Performs all the logic necessary to generate a changelog topic based on a
        "source topic" (aka input/consumed topic).

        Its main goal is to ensure partition counts of the to-be generated changelog
        match the source topic, and ensure the changelog topic is compacted. Also
        enforces the serialization type. All `Topic` objects generated with this are
        stored on the TopicManager.

        If source topic already exists, defers to the existing topic settings, else
        uses the settings as defined by the Application.

        In general, users should NOT need this; an Application will know
        when to generate changelog topics. To turn off changelogs, init an
        Application with "use_changelog_topics"=`False`

        :param consumer_group: name of consumer group (for this app)
        :param source_topic_name: name of consumed topic (app input topic)
        :param suffix: name of storage type (default, rolling10s, etc.)
        :param configs_to_import: what extra_configs should be allowed when importing
            settings from the source topic.

        :return: `Topic` object (which is also stored on the TopicManager)
        """
        # TODO: consider removing configs_to_import as changelog settings management
        # around retention, quix compact settings, etc matures.
        name = self._format_changelog_name(consumer_group, source_topic_name, suffix)
        if not configs_to_import:
            configs_to_import = self._changelog_extra_config_imports_defaults
        configs_to_import.discard("cleanup.policy")
        topic_config = (
            self._admin_client.inspect_topics([source_topic_name])[source_topic_name]
            or self._topics[source_topic_name].config
        )
        if not topic_config:
            raise self.MissingTopicForChangelog(
                f"There is no Topic object or existing topic in Kafka for topic "
                f"'{source_topic_name}' for desired changelog '{name}'; confirm "
                f"configs are allowed to be auto-created (for an `Application`, "
                f"set 'auto_create_topics=True')"
            )
        topic_config.update_extra_config(allowed=configs_to_import)
        topic = Topic(
            name=name,
            key_serializer="bytes",
            value_serializer="bytes",
            key_deserializer="bytes",
            value_deserializer="bytes",
            config=self._process_topic_configs(
                topic_config,
                extra_config_defaults=self._changelog_extra_config_defaults,
            ),
        )
        self._changelog_topics.setdefault(source_topic_name, {})[suffix] = topic
        return topic


class QuixTopicManager(TopicManager):
    """
    The source of all topic management with quixstreams.

    This is specifically for Applications using the Quix platform.

    Generally initialized and managed automatically by an `Application.Quix`,
    but allows a user to work with it directly when needed, such as using it alongside
    a plain `Producer` to create its topics.

    See methods for details.
    """

    _topic_partitions = 2
    _topic_replication = 2

    _topic_extra_config_defaults = {
        "retention.ms": f"{10080 * 60000}",  # minutes converted to ms
        "retention.bytes": "52428800",
    }
    _changelog_extra_config_defaults = {}
    _changelog_extra_config_imports_defaults = {"retention.bytes", "retention.ms"}

    def __init__(
        self,
        admin_client: Optional[Admin] = None,
        create_timeout: int = 60,
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
    ):
        """
        :param admin_client: an `Admin` instance
        :param create_timeout: timeout for topic creation
        :param quix_config_builder: A QuixKafkaConfigsBuilder instance, else one is
            generated for you.
        """
        quix_config_builder = quix_config_builder or QuixKafkaConfigsBuilder()
        if not admin_client:
            admin_configs = quix_config_builder.get_confluent_broker_config()
            admin_client = Admin(
                broker_address=admin_configs.pop("bootstrap.servers"),
                extra_config=admin_configs,
            )
        super().__init__(
            admin_client=admin_client,
            create_timeout=create_timeout,
        )
        self._quix_config_builder = quix_config_builder

    def _create_topics(self, topics: TopicList):
        """
        Method that actually creates the topics in Kafka via the
        QuixConfigBuilder instance.

        :param topics: list of `Topic`s
        """
        self._quix_config_builder.create_topics(
            topics, finalize_timeout_seconds=self._create_timeout
        )

    def _apply_topic_prefix(self, name: str) -> str:
        """
        Prepend workspace ID to a given topic name

        :param name: topic name

        :return: name with workspace ID prepended
        """
        return self._quix_config_builder.append_workspace_id(name)

    # TODO: remove this once 43 char limit is removed
    def _strip_changelog_chars(self, value: str):
        """
        A temporary function to capture character stripping necessary while we
        wait for character limit in Quix to be increased.

        :param value: a string

        :return: a string with only its first few and last chars
        """
        stripped = self._quix_config_builder.strip_workspace_id(value)
        return f"{stripped[:5]}{stripped[-5:]}"

    def _format_changelog_name(
        self, consumer_group: str, source_topic_name: str, suffix: str
    ):
        """
        Generate the name of the changelog topic based on the following parameters.

        This naming scheme guarantees uniqueness across all independent `Application`s.

        :param consumer_group: name of consumer group (for this app)
        :param source_topic_name: name of consumed topic (app input topic)
        :param suffix: name of storage type (default, rolling10s, etc.)

        :return: formatted topic name
        """
        # TODO: "strip" should be `self._quix_config_builder.strip_workspace_id` once
        # We fix the 43 char limit

        # TODO: remove suffix limitation and standardize the topic name template to
        # match the non-quix counterpart

        strip = self._strip_changelog_chars
        return self._quix_config_builder.append_workspace_id(
            f"changelog__{strip(consumer_group)}-{strip(source_topic_name)}-{suffix[:9]}"
        )
