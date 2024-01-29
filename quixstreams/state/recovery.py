import logging

from quixstreams.topic_manager import TopicManagerType

logger = logging.getLogger(__name__)


__all__ = ("ChangelogManager",)


class ChangelogManager:
    """
    A simple interface for managing all things related to changelog topics and is
    primarily used by the StateStoreManager.

    Facilitates creation of changelog topics and assigning their partitions during
    rebalances, and handles recovery process loop calls from `Application`.
    """

    def __init__(
        self,
        topic_manager: TopicManagerType,
    ):
        self._topic_manager = topic_manager

    def add_changelog(self, topic_name: str, store_name: str, consumer_group: str):
        self._topic_manager.changelog_topic(
            topic_name=topic_name,
            suffix=store_name,
            consumer_group=consumer_group,
        )
