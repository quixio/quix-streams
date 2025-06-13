from quixstreams.exceptions.base import QuixException

__all__ = (
    "GroupByNestingLimit",
    "InvalidColumnReference",
    "ColumnDoesNotExist",
    "StreamingDataFrameDuplicate",
    "GroupByDuplicate",
)


class ColumnDoesNotExist(QuixException): ...


class InvalidColumnReference(QuixException): ...


class GroupByNestingLimit(QuixException): ...


class GroupByDuplicate(QuixException): ...


class StreamingDataFrameDuplicate(QuixException): ...
