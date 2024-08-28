from quixstreams.exceptions.base import QuixException


__all__ = (
    "InvalidOperation",
    "GroupByNestingLimit",
    "InvalidColumnReference",
    "ColumnDoesNotExist",
    "DataFrameLocked",
    "StreamingDataFrameDuplicate",
    "GroupByDuplicate",
)


class InvalidOperation(QuixException): ...


class ColumnDoesNotExist(QuixException): ...


class InvalidColumnReference(QuixException): ...


class GroupByNestingLimit(QuixException): ...


class GroupByDuplicate(QuixException): ...


class DataFrameLocked(QuixException): ...


class StreamingDataFrameDuplicate(QuixException): ...
