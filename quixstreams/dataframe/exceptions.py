from quixstreams.exceptions.base import QuixException


__all__ = (
    "InvalidOperation",
    "GroupByLimitExceeded",
    "InvalidColumnReference",
    "ColumnDoesNotExist",
    "DataFrameLocked",
)


class InvalidOperation(QuixException): ...


class ColumnDoesNotExist(QuixException): ...


class InvalidColumnReference(QuixException): ...


class GroupByLimitExceeded(QuixException): ...


class DataFrameLocked(QuixException): ...
