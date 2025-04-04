import logging
from typing import Literal, Optional, Sequence

from quixstreams.models.topics import Topic, TopicAdmin, TopicConfig, TopicManager
from quixstreams.models.topics.exceptions import TopicNotFoundError

from .config import QuixKafkaConfigsBuilder
from .exceptions import QuixApiRequestFailure

__all__ = ("QuixTopicManager",)

logger = logging.getLogger(__name__)


class QuixTopicManager(TopicManager):
    """
    The source of all topic management with quixstreams.

    This is specifically for Applications using the Quix Cloud.

    Generally initialized and managed automatically by a Quix Application,
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
        auto_create_topics: bool = True,
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
            auto_create_topics=auto_create_topics,
        )
        self._quix_config_builder = quix_config_builder

    def stream_id_from_topics(self, topics: Sequence[Topic]) -> str:
        """
        Generate a stream_id by combining names of the provided topics.
        """
        if not topics:
            raise ValueError("At least one Topic must be passed")
        elif len(topics) == 1:
            # If only one topic is passed, return its full name
            # for backwards compatibility
            return topics[0].name

        # Use the "quix_name" to generate stream_id.
        # In Quix Cloud, the "quix_name" can differ from the actual broker topic name
        return "--".join(sorted(t.quix_name for t in topics))

    def _fetch_topic(self, topic: Topic) -> Topic:
        try:
            quix_topic_info = self._quix_config_builder.get_topic(
                topic=topic, timeout=self._timeout
            )
        except QuixApiRequestFailure as e:
            if e.status_code == 404:
                raise TopicNotFoundError(
                    f'Topic "{topic.name}" not found on the broker'
                )
            raise
        quix_topic = self._quix_config_builder.convert_topic_response(quix_topic_info)
        return quix_topic

    def _configure_topic(self, topic: Topic, broker_topic: Topic) -> Topic:
        """
        Configure the topic with the correct broker config and extra config imports.
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
        topic_out = topic.__clone__(name=broker_topic.name)
        topic_out.broker_config = broker_config
        self._quix_config_builder.wait_for_topic_ready_statuses([topic_out])
        return topic_out

    def _create_topic(self, topic: Topic, timeout: float, create_timeout: float):
        try:
            self._quix_config_builder.create_topic(topic=topic, timeout=timeout)
        except QuixApiRequestFailure as exc:
            error_msg = str(exc)
            # Find a better way to determine the exact error in the future
            already_exists = (
                f"Topic with name similar to '{topic.name}' already exists."
                in error_msg
            )
            if exc.status_code == 400 and already_exists:
                logger.info(f'Topic "{topic.name}" already exists')
            else:
                raise

    def _internal_name(
        self,
        topic_type: Literal["changelog", "repartition"],
        topic_name: Optional[str],
        suffix: str,
    ):
        """
        Generate an "internal" topic name.

        This naming scheme guarantees uniqueness across all independent `Application`s.

        For Quix Cloud, the original topic name passed by the user (rather than
        the topic id as returned by the get/create) is used to construct it.

        The internal format is <{TYPE}__{GROUP}--{NAME}--{SUFFIX}>

        :param topic_type: topic type, added as prefix (changelog, repartition)
        :param topic_name: name of consumed topic (app input topic)
        :param suffix: a unique descriptor related to topic type, added as suffix

        :return: formatted topic name
        """

        # Map the full topic name to the shorter "quix_name" and use
        # it for internal topics.
        # "quix_name" is not prefixed with the workspace id.
        if topic_name is not None:
            topic = self.non_changelog_topics.get(topic_name)
            if topic is not None:
                topic_name = topic.quix_name

        return super()._internal_name(topic_type, topic_name, suffix)
