import ctypes
import weakref
from typing import List

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.States.AppStateManager import AppStateManager as tsmi

from ..native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei

from .topicstatemanager import TopicStateManager

@nativedecorator
class AppStateManager(object):
    """
    Manages the states of an app.
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of AppStateManager.

        NOTE: Do not initialize this class manually, use App.get_state_manager

        Args:
            net_pointer: The .net object representing a AppStateManager.
        """

        if net_pointer is None:
            raise Exception("AppStateManager is none")

        self._topic_state_cache = weakref.WeakValueDictionary()
        self._interop = tsmi(net_pointer)

    def get_topic_states(self) -> List[str]:
        """
        Returns a collection of all available topic states for the app.

        Returns:
            List[str]: All available app topic states for the app.
        """

        return ei.ReadStrings(self._interop.GetTopicStates())

    def get_topic_state_manager(self, topic_name: str) -> TopicStateManager:
        """
        Gets an instance of the TopicStateManager for the specified topic.

        Args:
            topic_name: The name of the topic
        """

        instance = self._topic_state_cache.get(topic_name)
        if instance is not None:
            return instance

        instance = TopicStateManager(self._interop.GetTopicStateManager(topic_name))
        self._topic_state_cache[topic_name] = instance
        return instance

    def delete_topic_state(self, topic_name: str) -> bool:
        """
        Deletes the specified topic state

        Args:
            topic_name: The name of the topic

        Returns:
            bool: Whether the topic state was deleted
        """

        del self._topic_state_cache[topic_name]

        return self._interop.DeleteTopicState(topic_name)

    def delete_topic_states(self) -> int:
        """
        Deletes all topic states for the app.

        Returns:
            int: The number of topic states that were deleted
        """

        self._topic_state_cache.clear()

        return self._interop.DeleteTopicStates()
