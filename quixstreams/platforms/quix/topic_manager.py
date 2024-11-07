from typing import List, Literal

from quixstreams.models.topics import Topic, TopicAdmin, TopicManager

from .config import QuixKafkaConfigsBuilder

__all__ = ("QuixTopicManager",)


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
        self._topic_id_to_name = {}

    def _finalize_topic(self, topic: Topic) -> Topic:
        """
        Returns a Topic object with the true topic name by attempting by finding or
        creating it in Quix Cloud and using the returned ID (the true topic name).

        Additionally, sets the actual topic configuration since we now have it anyway.
        """
        quix_topic_info = self._quix_config_builder.get_or_create_topic(topic)
        quix_topic = self._quix_config_builder.convert_topic_response(quix_topic_info)
        # allows us to include the configs not included in the API response
        quix_topic.config.extra_config = {
            **topic.config.extra_config,
            **quix_topic.config.extra_config,
        }
        topic_out = topic.__clone__(name=quix_topic.name, config=quix_topic.config)
        self._topic_id_to_name[topic_out.name] = quix_topic_info["name"]
        return super()._finalize_topic(topic_out)

    def _create_topics(
        self, topics: List[Topic], timeout: float, create_timeout: float
    ):
        """
        Because we create topics immediately upon Topic generation for Quix Cloud,
        this method simply confirms topics all have a "Ready" status.
        """
        self._quix_config_builder.wait_for_topic_ready_statuses(topics)

    def _internal_name(
        self,
        topic_type: Literal["changelog", "repartition"],
        topic_name: str,
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
            self._topic_id_to_name[topic_name],
            suffix,
        )
