from typing import Optional, Dict, List

from quixstreams.models.topics import TopicManager, TopicAdmin, Topic
from .config import QuixKafkaConfigsBuilder

__all__ = ("QuixTopicManager",)


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
        admin: Optional[TopicAdmin] = None,
        create_timeout: int = 60,
        quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
    ):
        """
        :param admin: an `Admin` instance
        :param create_timeout: timeout for topic creation
        :param quix_config_builder: A QuixKafkaConfigsBuilder instance, else one is
            generated for you.
        """
        quix_config_builder = quix_config_builder or QuixKafkaConfigsBuilder()
        if not admin:
            admin_configs = quix_config_builder.get_confluent_broker_config()
            admin = TopicAdmin(
                broker_address=admin_configs.pop("bootstrap.servers"),
                extra_config=admin_configs,
            )
        self._admin = admin
        self._topics: Dict[str, Topic] = {}
        self._changelog_topics: Dict[str, Dict[str, Topic]] = {}
        self._create_timeout = create_timeout
        self._quix_config_builder = quix_config_builder
        super().__init__(create_timeout=create_timeout)

    def _create_topics(self, topics: List[Topic]):
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
        return self._quix_config_builder.prepend_workspace_id(name)

    # TODO: remove this once 43 char limit is removed
    def _strip_changelog_chars(self, value: str):
        """
        A temporary function to capture character stripping necessary while we
        wait for character limit in Quix to be increased.

        :param value: a string

        :return: a string with only its first few and last chars
        """
        stripped = self._quix_config_builder.strip_workspace_id_prefix(value)
        return f"{stripped[:5]}{stripped[-5:]}"

    def _format_changelog_name(self, consumer_group: str, topic_name: str, suffix: str):
        """
        Generate the name of the changelog topic based on the following parameters.

        This naming scheme guarantees uniqueness across all independent `Application`s.

        :param consumer_group: name of consumer group (for this app)
        :param topic_name: name of consumed topic (app input topic)
        :param suffix: name of storage type (default, rolling10s, etc.)

        :return: formatted topic name
        """
        # TODO: "strip" should be `self._quix_config_builder.strip_workspace_id_prefix`
        #  once we fix the 43 char limit

        # TODO: remove suffix limitation and standardize the topic name template to
        #  match the non-quix counterpart

        strip = self._strip_changelog_chars
        return self._quix_config_builder.prepend_workspace_id(
            f"changelog__{strip(consumer_group)}-{strip(topic_name)}-{suffix[:9]}"
        )
