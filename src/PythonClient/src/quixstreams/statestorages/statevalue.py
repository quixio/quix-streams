import pickle
from typing import Any
from ..statestorages.statetype import StateType


class StateValue:
    """
    A wrapper class for values that can be stored inside the storage.
    """

    def __init__(self, value: Any):
        """
        Initializes the wrapped value inside the store.

        Args:
            value: The value to be wrapped, which can be one of the following types:
                StateValue, str, int, float, bool, bytes, bytearray, or object (via pickle).
        """

        self._type = self._type_from_value(value)
        self._value = value

    @staticmethod
    def _type_from_value(value: Any) -> StateType:
        if isinstance(value, bool):
            return StateType.Bool
        elif isinstance(value, int):
            return StateType.Long
        elif isinstance(value, bytes) or isinstance(value, bytearray):
            return StateType.Binary
        elif isinstance(value, str):
            return StateType.String
        elif isinstance(value, float):
            return StateType.Double
        else:
            return StateType.Object

    @property
    def type(self) -> StateType:
        """
        Gets the type of the wrapped value.

        Returns:
            StateType: The type of the wrapped value.
        """
        return self._type

    @property
    def value(self) -> Any:
        """
        Gets the wrapped value.

        Returns:
            The wrapped value.
        """
        return self._value

    def is_null(self) -> bool:
        return self._value is None

    def __eq__(self, other) -> bool:
        if isinstance(other, StateValue) and self._type == other.type:
            if self._type == StateType.Object:
                return pickle.loads(self._value) == pickle.loads(other.value)
            return self._value == other.value
        return False

    def __hash__(self) -> int:
        if self._type == StateType.Object:
            return hash(pickle.loads(self._value))
        return hash(self._value)
