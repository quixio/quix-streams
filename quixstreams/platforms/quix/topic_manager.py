from typing import List, Literal

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

    # Default topic params
    # Set these to None to use defaults defined in Quix Cloud
    default_num_partitions = None
    default_replication_factor = None

    # Max topic name length for the new topics
    _max_topic_name_len = 249

    def __init__(
        self,
        topic_admin: TopicAdmin,
        consumer_group: str,
        quix_config_builder: QuixKafkaConfigsBuilder,
        timeout: float = 30,
        create_timeout: float = 60,
    ):
        """
        :param topic_admin: an `Admin` instance
        :param quix_config_builder: A QuixKafkaConfigsBuilder instance, else one is
            generated for you.
        :param timeout: response timeout (seconds)
        :param create_timeout: timeout for topic creation
        """
        super().__init__(
            topic_admin=topic_admin,
            consumer_group=quix_config_builder.strip_workspace_id_prefix(
                consumer_group
            ),
            timeout=timeout,
            create_timeout=create_timeout,
        )
        self._quix_config_builder = quix_config_builder

    def _create_topics(
        self, topics: List[Topic], timeout: float, create_timeout: float
    ):
        """
        Method that actually creates the topics in Kafka via the
        QuixConfigBuilder instance.

        :param topics: list of `Topic`s
        :param timeout: creation acknowledge timeout (seconds)
        :param create_timeout: topic finalization timeout (seconds)
        """
        self._quix_config_builder.create_topics(
            topics, timeout=timeout, finalize_timeout=create_timeout
        )

    def _resolve_topic_name(self, name: str) -> str:
        """
        Checks if provided topic name is registered via Quix API; if yes,
        return its corresponding topic ID (AKA the actual topic name, usually
        just has prepended workspace ID).

        Otherwise, assume it doesn't exist and prepend the workspace ID to match the
        topic naming pattern in Quix.

        :param name: topic name

        :return: actual cluster topic name to use
        """
        if quix_topic := self._quix_config_builder.get_topic(name):
            name = quix_topic["id"]
        else:
            name = self._quix_config_builder.prepend_workspace_id(name)
        return super()._resolve_topic_name(name)

    def _internal_name(
        self,
        topic_type: Literal["changelog", "repartition"],
        topic_name: str,
        suffix: str,
    ):
        """
        Generate an "internal" topic name.

        This naming scheme guarantees uniqueness across all independent `Application`s.

        The internal format is <{TYPE}__{GROUP}--{NAME}--{SUFFIX}>

        :param topic_type: topic type, added as prefix (changelog, repartition)
        :param topic_name: name of consumed topic (app input topic)
        :param suffix: a unique descriptor related to topic type, added as suffix

        :return: formatted topic name
        """
        return super()._internal_name(
            topic_type,
            self._quix_config_builder.strip_workspace_id_prefix(topic_name),
            suffix,
        )
