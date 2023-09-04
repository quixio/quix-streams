from typing import Dict, Type
from collections import defaultdict
import threading

from ..native.Python.QuixStreamsStreaming.ITopicConsumer import ITopicConsumer
from .streamstatemanager import StreamStateManager
from ..statestorages import IStateStorage


class TopicStateManager:
    """
    TopicStateManager is a class that manages the states of a topic.

    Attributes:
        topic_consumer: The topic consumer this manager is for.
        topic_name: The name of the topic.
        state_storage: The state storage to use.
        logger: The logger to use.
        stream_state_managers: A dictionary that maps stream ids to their state managers.
    """

    def __init__(self, topic_name: str, state_storage: IStateStorage,
                 topic_consumer: ITopicConsumer = None):
        """
        The constructor for TopicStateManager class.

        Args:
            topic_name: The name of the topic.
            state_storage: The state storage to use.
            topic_consumer: The topic consumer this manager is for.
        """
        self.topic_consumer = topic_consumer
        self.topic_name = topic_name
        self.state_storage = state_storage
        # self.logger = self.logger_factory.getLogger("TopicStateManager")
        self.stream_state_managers: Dict[str, StreamStateManager] = defaultdict(StreamStateManager)
        self.state_lock = threading.Lock()

    def get_stream_states(self) -> Dict[str, Type[IStateStorage]]:
        """
        Returns all available stream states for the current topic.

        Returns:
            A dictionary of string values representing the stream state names.
        """
        return self.state_storage.get_sub_storages()

    def delete_stream_states(self) -> int:
        """
        Deletes all stream states for the current topic.

        Returns:
            The number of stream states that were deleted.
        """
        # self.logger.trace(f"Deleting Stream states for topic {self.topic_name}")
        count = self.state_storage.delete_sub_storages()
        self.stream_state_managers.clear()
        return count

    def delete_stream_state(self, stream_id: str) -> bool:
        """
        Deletes the stream state with the specified stream id.

        Args:
            stream_id: The id of the stream state to delete.

        Returns:
            A boolean indicating whether the stream state was deleted.
        """
        # self.logger.trace(f"Deleting Stream states for {stream_id}")
        if not self.state_storage.delete_sub_storage(self.get_sub_storage_name(stream_id)):
            return False
        del self.stream_state_managers[stream_id]
        return True

    def get_sub_storage_name(self, stream_id: str) -> str:
        """
        Returns the sub storage name with correct prefix.

        Args:
            stream_id: The stream id to prefix.

        Returns:
            The prefixed stream id.
        """
        return stream_id.replace('/', '_').replace('\\', '_').replace(':', '_').replace('//', '_')

    def get_stream_state_manager(self, stream_id: str) -> StreamStateManager:
        """
        Gets an instance of the StreamStateManager class for the specified stream id.

        Args:
            stream_id: The ID of the stream.

        Returns:
            The newly created StreamStateManager instance.
        """
        if stream_id not in self.stream_state_managers:
            # self.logger.trace(f"Creating Stream state manager for {stream_id}")
            self.stream_state_managers[stream_id] = StreamStateManager(stream_id=stream_id,
                                                                       state_storage=self.state_storage.get_or_create_sub_storage(
                                                                           sub_storage_name=self.get_sub_storage_name(stream_id),
                                                                           db_name="TOPIC PARTITION"),
                                                                       log_prefix=self.topic_name + " ",
                                                                       topic_consumer=self.topic_consumer)
        return self.stream_state_managers[stream_id]
