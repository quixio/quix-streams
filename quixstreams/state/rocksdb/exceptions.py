from quixstreams.exceptions import QuixException

__all__ = (
    "StateSerializationError",
    "StateTransactionError",
    "NestedPrefixError",
    "ColumnFamilyDoesNotExist",
    "ColumnFamilyAlreadyExists",
)


class StateError(QuixException):
    ...


class StateSerializationError(StateError):
    ...


class StateTransactionError(StateError):
    ...


class NestedPrefixError(StateError):
    ...


class ColumnFamilyDoesNotExist(StateError):
    ...


class ColumnFamilyAlreadyExists(StateError):
    ...
