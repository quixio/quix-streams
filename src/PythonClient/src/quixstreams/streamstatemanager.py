import ctypes
import weakref

from .helpers.nativedecorator import nativedecorator
from .native.Python.QuixStreamsStreaming.StreamStateManager import StreamStateManager as ssmi

from .streamstate import StreamState


@nativedecorator
class StreamStateManager(object):
    """
    Manages the states of a stream.
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamStateManager.

        NOTE: Do not initialize this class manually, use StreamConsumer.get_state_manager

        Args:
            net_pointer: The .net object representing a StreamStateManager.
        """

        if net_pointer is None:
            raise Exception("StreamStateManager is none")

        self._cache = weakref.WeakValueDictionary()
        self._interop = ssmi(net_pointer)

    def get_state(self, name_of_state: str) -> StreamState:
        """
        Creates a new application state with automatically managed lifecycle for the stream

        Args:
            name_of_state: The name of the state
        """

        instance = self._cache.get(name_of_state)
        if instance is not None:
            return instance

        instance = StreamState(self._interop.GetState(name_of_state))
        self._cache[name_of_state] = instance
        return instance
