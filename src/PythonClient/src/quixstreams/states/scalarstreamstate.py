from typing import Callable, Generic
import logging

from quixstreams.states.istreamstate import IStreamState
from quixstreams.states.scalarstate import ScalarState
from quixstreams.statestorages.istatestorage import IStateStorage
from quixstreams.statestorages.statetype import StreamStateType


class ScalarStreamState(Generic[StreamStateType], IStreamState):
    """
    Represents a state container that stores a scalar value with the ability to flush changes to a specified storage.
    """

    def __init__(self, storage: IStateStorage, state_type: StreamStateType, default_value_factory: Callable[[], StreamStateType]):
        """
        Initializes a new instance of ScalarStreamState.

        NOTE: Do not initialize this class manually, use StreamStateManager.get_scalar_state

        Args:
            storage: The storage to flush the state to
            default_value_factory: A function that returns a default value of type T when the value has not been set yet
        """

        if state_type is None:
            raise Exception('state_type must be specified')

        self._scalar_state = ScalarState(storage, None)
        self._default_value_factory = default_value_factory or (lambda _: raise_key_error())
        self._type = state_type

        self._flushing_callbacks = []
        self._flushed_callbacks = []

    def add_flushing_callback(self, callback):
        self._flushing_callbacks.append(callback)

    def add_flushed_callback(self, callback):
        self._flushed_callbacks.append(callback)

    @property
    def type(self) -> type:
        """
        Gets the type of the ScalarStreamState

        Returns:
            StreamStateType: type of the state
        """
        return self._type

    @property
    def value(self):
        """
        Gets the value of the state.

        Returns:
            StreamStateType: The value of the state.
        """
        _value = self._scalar_state.value
        if _value is not None:
            return _value
        if self._default_value_factory:
            _value = self._default_value_factory()
            self._scalar_state.value = _value
            return _value
        return None

    @value.setter
    def value(self, new_value: StreamStateType):
        """
        Sets the value of the state.

        Args:
            new_value: New value of the state.
        """
        self._scalar_state.value = new_value

    def flush(self):
        """Trigger flush operations."""
        # log
        for callback in self._flushing_callbacks:
            callback(self)

        self._scalar_state.flush()

        # log
        for callback in self._flushed_callbacks:
            callback(self)

    def clear(self):
        self._scalar_state.clear()

    def reset(self):
        """
        Reset the state to before in-memory modifications
        """

        self._scalar_state.reset()


def raise_key_error():
    raise KeyError("The specified key was not found and there was no default value factory set.")
