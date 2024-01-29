from typing import (
    List,
    Optional,
    Any,
    Callable,
    Dict,
    Mapping,
    Protocol,
    ClassVar,
    Literal,
    Set,
)

from .admin import TopicAdmin
from .topic import Topic, TopicConfig
from quixstreams.models import (
    DeserializerType,
    SerializerType,
    MessageHeadersTuples,
    TimestampType,
)

__all__ = ("TimestampExtractor", "TopicManagerType")

TimestampExtractor = Callable[
    [Any, Optional[MessageHeadersTuples], int, TimestampType],
    int,
]


class TopicManagerType(Protocol):
    """
    Outlines interface for any intended "TopicManager" instance along with defining
    any simple shared functionality across all implementations.
    """

    _topic_partitions: ClassVar[int]
    _topic_replication: ClassVar[int]
    _topic_extra_config_defaults: ClassVar[dict]
    _changelog_extra_config_defaults: ClassVar[dict]
    _changelog_extra_config_imports_defaults: ClassVar[Set]

    @property
    def topics(self) -> Dict[str, Topic]:
        ...

    @property
    def topics_list(self) -> List[Topic]:
        ...

    @property
    def changelog_topics(self) -> Dict[str, Dict[str, Topic]]:
        """
        Note: `Topic`s are the changelogs.

        returns: the changelog topic dict, {topic_name: {suffix: Topic}}
        """
        ...

    @property
    def changelog_topics_list(self) -> List[Topic]:
        ...

    @property
    def all_topics(self) -> List[Topic]:
        ...

    @property
    def pretty_formatted_topic_configs(self) -> str:
        """
        Returns a print-friendly version of all the topics and their configs

        :return: a pprint-formatted string of all the topics
        """
        ...

    @property
    def admin(self) -> TopicAdmin:
        """
        Raises an exception so that things that require an Admin instance fail.
        """
        ...

    @property
    def has_admin(self) -> bool:
        """
        Whether an admin client has been defined or not.

        :return: bool
        """
        ...

    def set_admin(self, admin: TopicAdmin):
        """
        Allows for adding an Admin class post-init.

        :param admin: an Admin instance
        """
        ...

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
        ...

    def topic(
        self,
        name: str,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = "bytes",
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = "bytes",
        config: Optional[TopicConfig] = None,
        auto_create_config: bool = True,
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
        :param auto_create_config: if no "topic_config", create one; Default - True
            > NOTE: this setting is generally manipulated by the Application class via
              its "auto_create_topics" option.
        :param timestamp_extractor: a callable that returns a timestamp in
            milliseconds from a deserialized message.

        :return: Topic object with creation configs
        """
        ...

    def changelog_topic(
        self,
        topic_name: str,
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
        uses the settings as defined by the `Topic` (and its defaults) as generated
        by the `TopicManager`.

        In general, users should NOT need this; an Application knows when/how to
        generate changelog topics. To turn off changelogs, init an Application with
        "use_changelog_topics"=`False`.

        :param consumer_group: name of consumer group (for this app)
        :param topic_name: name of consumed topic (app input topic)
        :param suffix: name of storage type (default, rolling10s, etc.)
        :param configs_to_import: what extra_configs should be allowed when importing
            settings from the source topic.

        :return: `Topic` object (which is also stored on the TopicManager)
        """
        # TODO: consider removing configs_to_import as changelog settings management
        #  around retention, quix compact settings, etc. matures.
        ...

    def create_all_topics(self):
        """
        A convenience method to create all Topic objects stored on this TopicManager.
        """
        ...

    def validate_all_topics(
        self,
        validation_level: Literal["exists", "required", "all"] = "exists",
    ):
        """
        A convenience method for validating all `Topic`s stored on this TopicManager.

        See `TopicManager.validate_topics()` for more details.

        :param validation_level: The degree of topic validation; Default - "exists"
            "exists" - Confirm expected topics exist.
            "required" - Confirm topics match your provided `Topic`
                partition + replication factor
            "all" - Confirm topic settings are EXACT.
        """
        ...
