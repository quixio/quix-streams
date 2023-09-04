from collections.abc import MutableMapping
from typing import Generic, Callable

from .istreamstate import IStreamState
from .dictstate import DictState
from ..statestorages import IStateStorage, StreamStateType


class DictStreamState(Generic[StreamStateType], MutableMapping, IStreamState):
    """
    Represents a state container that stores key-value pairs with the ability to flush changes to a specified storage.
    """

    def __init__(self, storage: IStateStorage, state_type: StreamStateType, default_value_factory: Callable[[str], StreamStateType]):
        """
        Initializes a new instance of DictStreamState.

        NOTE: Do not initialize this class manually, use StreamStateManager.get_dict_state

        Args:
            storage: The storage to flush the state to
            state_type: The type of the state
            default_value_factory: The default value factory to create value when the key is not yet present in the state
        """

        if state_type is None:
            raise Exception('state_type must be specified')

        self._dictionary_state = DictState(storage, None)
        self._type = state_type
        self._default_value_factory = default_value_factory or (lambda _: raise_key_error())
        self._flushing_callbacks = []
        self._flushed_callbacks = []

    def __getitem__(self, key: str) -> StreamStateType:
        if not self.is_case_sensitive:
            key = key.lower()

        if key in self._dictionary_state:
            return self._dictionary_state[key]

        value = self._default_value_factory(key)
        self._dictionary_state[key] = value
        return value

    def __setitem__(self, key: str, value: StreamStateType):
        self._dictionary_state[key] = value

    def __delitem__(self, key: str):
        del self._dictionary_state[key]

    def __iter__(self):
        return iter(self._dictionary_state)

    def __len__(self):
        return len(self._dictionary_state)

    def add_flushing_callback(self, callback):
        self._flushing_callbacks.append(callback)

    def add_flushed_callback(self, callback):
        self._flushed_callbacks.append(callback)

    @property
    def type(self) -> type:
        """
        Gets the type of the StreamState

        Returns:
            StreamStateType: type of the state
        """
        return self._type

    @property
    def is_fixed_size(self):
        return self._dictionary_state.is_fixed_size

    def contains(self, key):
        """Check if the key exists in the dictionary_state."""
        return key in self._dictionary_state

    @property
    def is_case_sensitive(self) -> bool:
        return self._dictionary_state.is_case_sensitive

    @property
    def is_synchronized(self) -> bool:
        return self._dictionary_state.is_synchronized

    @property
    def is_read_only(self):
        return False

    def try_get_value(self, key) -> StreamStateType:
        """Try to get the value for a key. Returns None if key doesn't exist."""
        try:
            return self._dictionary_state[key]
        except KeyError:
            return None

    def flush(self):
        """Trigger flush operations."""
        # log
        for callback in self._flushing_callbacks:
            callback(self)
        self._dictionary_state.flush()
        # log
        for callback in self._flushed_callbacks:
            callback(self)

    def reset(self):
        self._dictionary_state.reset()


def raise_key_error():
    raise KeyError("The specified key was not found and there was no default value factory set.")
