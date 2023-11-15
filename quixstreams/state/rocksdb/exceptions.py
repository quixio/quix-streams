from quixstreams.exceptions import QuixException

__all__ = ("StateSerializationError", "StateTransactionError", "NestedPrefixError")


class StateSerializationError(QuixException):
    ...


class StateTransactionError(QuixException):
    ...


class NestedPrefixError(QuixException):
    ...
