from quixstreams.exceptions import QuixException

__all__ = (
    "StateSerializationError",
    "StateTransactionError",
    "ColumnFamilyDoesNotExist",
    "ColumnFamilyAlreadyExists",
    "ColumnFamilyHeaderMissing",
    "InvalidChangelogOffset",
)


class StateError(QuixException): ...


class StateSerializationError(StateError): ...


class StateTransactionError(StateError): ...


class ColumnFamilyDoesNotExist(StateError): ...


class ColumnFamilyAlreadyExists(StateError): ...


class ColumnFamilyHeaderMissing(StateError): ...


class InvalidChangelogOffset(StateError): ...
