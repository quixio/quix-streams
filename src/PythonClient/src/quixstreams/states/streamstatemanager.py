import ctypes
import logging
import warnings
import weakref

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.States.StreamStateManager import StreamStateManager as ssmi

from .streamstate import StreamState

from typing import TypeVar, Callable, get_origin, Any
import inspect

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

    def get_state(self, name_of_state: str, default_value_factory: Callable[[str], StreamStateType] = None, state_type: StreamStateType = None) -> StreamState[StreamStateType]:
        """
        Creates a new application state with automatically managed lifecycle for the stream

        Args:
            name_of_state: The name of the state
            state_type: The type of the state
            default_value_factory: The default value factory to create value when the key is not yet present in the state

        Example:
            >>> state_manager.get_state('some_state')
            This will return a state where type is 'Any'

            >>> state_manager.get_state('some_state', lambda key: return {})
            this will return a state where type is a generic dictionary, with an empty dictionary as default value when
            key is not available. The lambda function will be invoked with 'get_state_type_check' key to determine type

            >>> state_manager.get_state('some_state', lambda key: return {}, Dict[str, float])
            this will return a state where type is a specific dictionary type, with default value

            >>> state_manager.get_state('some_state', state_type=float)
            this will return a state where type is a float without default value, resulting in KeyError when not found
        """

        if state_type is None:
            state_type = Any
            # Try to figure out the type based on python typehints
            if default_value_factory is not None:
                try:
                    state_type = type(default_value_factory('get_state_type_check'))
                    if state_type is None:
                        state_type = Any
                except:
                    pass

        instance: StreamState = self._cache.get(name_of_state)
        if instance is not None:
            if instance.type is not state_type:
                logging.log(logging.WARNING, f'State {name_of_state} already exists with a different type ({instance.type.__name__}), new type is {state_type.__name__}. Returning original state instance.')
            return instance

        instance = StreamState[StreamStateType](self._interop.GetState(name_of_state), state_type, default_value_factory)
        self._cache[name_of_state] = instance
        return instance
