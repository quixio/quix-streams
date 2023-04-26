import ctypes
import weakref
from typing import List

from quixstreams.helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.States.TopicStateManager import TopicStateManager as tsmi

from quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei

from streamstatemanager import StreamStateManager

@nativedecorator
class TopicStateManager(object):
    """
    Manages the states of a topic.
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of TopicStateManager.

        NOTE: Do not initialize this class manually, use TopicConsumer.get_state_manager

        Args:
            net_pointer: The .net object representing a TopicStateManager.
        """

        if net_pointer is None:
            raise Exception("TopicStateManager is none")

        self._stream_state_cache = weakref.WeakValueDictionary()
        self._interop = tsmi(net_pointer)

    def get_stream_states(self) -> List[str]:
        """
        Returns a collection of all available stream state ids for the current topic.

        Returns:
            List[str]: All available stream state ids for the current topic.
        """

        return ei.ReadStrings(self._interop.GetStreamStates())

    def get_stream_state_manager(self, stream_id: str) -> StreamStateManager:
        """
        Gets an instance of the StreamStateManager class for the specified stream_id.

        Args:
            stream_id: The ID of the stream
        """

        instance = self._stream_state_cache.get(stream_id)
        if instance is not None:
            return instance

        instance = StreamStateManager(self._interop.GetStreamStateManager(stream_id))
        self._stream_state_cache[stream_id] = instance
        return instance

    def delete_stream_state(self, stream_id: str) -> bool:
        """
        Deletes the stream state for the specified stream

        Args:
            stream_id: The ID of the stream

        Returns:
            bool: Whether the stream state was deleted
        """

        del self._stream_state_cache[stream_id]

        return self._interop.DeleteStreamState(stream_id)

    def delete_stream_states(self) -> int:
        """
        Deletes all stream states for the current topic.

        Returns:
            int: The number of stream states that were deleted
        """

        self._stream_state_cache.clear()

        return self._interop.DeleteStreamStates()
