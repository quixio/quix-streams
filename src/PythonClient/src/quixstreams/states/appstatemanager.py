from typing import Dict, Type
from collections import defaultdict

from .topicstatemanager import IStateStorage, TopicStateManager


class AppStateManager:
    """
    AppStateManager is a class that manages the states of different topics in the app.

    Attributes:
        storage: The state storage to use for the application state.
        topic_state_managers: A dictionary that maps topic names to their state managers.
    """

    def __init__(self, storage: IStateStorage):
        """
        The constructor for AppStateManager class.

        Args:
            storage: The state storage to use for the application state.
        """
        self.storage = storage
        # self.logger = self.logger_factory.getLogger("AppStateManager")
        self.topic_state_managers: Dict[str, TopicStateManager] = defaultdict(TopicStateManager)

    def get_topic_states(self) -> Dict[str, Type[IStateStorage]]:
        """
        Returns all available topic states for the current app.

        Returns:
            A dictionary of string values representing the topic state names.
        """
        return self.storage.get_sub_storages()

    def delete_topic_states(self) -> int:
        """
        Deletes all topic states for the current app.

        Returns:
            The number of topic states that were deleted.
        """
        # self.logger.trace("Deleting topic states")
        count = self.storage.delete_sub_storages()
        self.topic_state_managers.clear()
        return count

    def delete_topic_state(self, topic_name: str) -> bool:
        """
        Deletes the topic state with the specified name.

        Args:
            topic_name: The name of the topic state to delete.

        Returns:
            A boolean indicating whether the topic state was deleted.
        """
        # self.logger.trace(f"Deleting topic states for {topic_name}")
        if not self.storage.delete_sub_storage(self.get_sub_storage_name(topic_name)):
            return False
        del self.topic_state_managers[topic_name]
        return True

    def get_sub_storage_name(self, topic_name: str) -> str:
        """
        Returns the sub storage name with correct prefix.

        Args:
            topic_name: The topic name to prefix.

        Returns:
            The prefixed topic name.
        """
        return topic_name.replace('/', '_').replace('\\', '_').replace(':', '_').replace('//', '_')

    def get_topic_state_manager(self, topic_name: str, topic_consumer=None) -> TopicStateManager:
        """
        Gets an instance of the TopicStateManager class for the specified topic name.

        Args:
            topic_name: The topic name.
            topic_consumer: The topic consumer to create the state manager for.

        Returns:
            The newly created TopicStateManager instance.
        """
        if topic_name not in self.topic_state_managers:
            # self.logger.trace(f"Creating Topic state manager for {topic_name}")
            self.topic_state_managers[topic_name] = TopicStateManager(topic_consumer=topic_consumer,
                                                                      topic_name=topic_name,
                                                                      state_storage=self.storage.get_or_create_sub_storage(
                                                                          self.get_sub_storage_name(topic_name)))
        return self.topic_state_managers[topic_name]
