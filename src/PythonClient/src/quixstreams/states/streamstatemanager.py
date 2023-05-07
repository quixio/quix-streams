import ctypes
import weakref

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.States.StreamStateManager import StreamStateManager as ssmi

from .streamstate import StreamState, StateValue

from typing import TypeVar, Callable

StreamStateType = TypeVar('StreamStateType')

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

    def get_state(self, name_of_state: str, state_type: StreamStateType, default_value_factory: Callable[[str], StreamStateType] = None) -> StreamState[StreamStateType]:
        """
        Creates a new application state with automatically managed lifecycle for the stream

        Args:
            name_of_state: The name of the state
            state_type: The type of the state
            default_value_factory: The default value factory to create value when the key is not yet present in the state
        """

        if state_type is None:
            raise Exception('state_type must be specified to determine stream state type')

        instance: StreamState = self._cache.get(name_of_state)
        if instance is not None:
            if instance.type is StreamStateType:
                return instance
            raise Exception(f'State {name_of_state} already exists with a different type ({instance.type.__name__}), unable to create with {state_type.__name__}.')

        instance = StreamState[StreamStateType](self._interop.GetState(name_of_state), state_type, default_value_factory)
        self._cache[name_of_state] = instance
        return instance
