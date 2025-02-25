import logging
from typing import Literal, Optional

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
        self._topic_id_to_name: dict[str, str] = {}

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

    def _finalize_topic(self, topic: Topic) -> Topic:
        """
        Returns a Topic object with the true topic name by attempting by finding or
        creating it in Quix Cloud and using the returned ID (the true topic name).

        Additionally, sets the actual topic configuration since we now have it anyway.
        """

        if self._auto_create_topics:
            self._validate_topic_name(name=topic.name)
            self._create_topic(
                topic, timeout=self._timeout, create_timeout=self._create_timeout
            )

        broker_topic = self._fetch_topic(topic=topic)
        broker_config = broker_topic.broker_config

        # A hack to pass extra info back from Quix cloud
        quix_topic_name = broker_config.extra_config.pop("__quix_topic_name__")
        topic_out = topic.__clone__(name=broker_topic.name)

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
        topic_out.broker_config = broker_config
        self._topic_id_to_name[topic_out.name] = quix_topic_name
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
        return super()._internal_name(
            topic_type,
            self._topic_id_to_name[topic_name] if topic_name else None,
            suffix,
        )
