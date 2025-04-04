from quixstreams.exceptions.base import QuixException

__all__ = (
    "InvalidOperation",
    "GroupByNestingLimit",
    "InvalidColumnReference",
    "ColumnDoesNotExist",
    "StreamingDataFrameDuplicate",
    "GroupByDuplicate",
    "TopicPartitionsMismatch",
)


class InvalidOperation(QuixException): ...


class ColumnDoesNotExist(QuixException): ...


class InvalidColumnReference(QuixException): ...


class GroupByNestingLimit(QuixException): ...


class GroupByDuplicate(QuixException): ...


class StreamingDataFrameDuplicate(QuixException): ...


class TopicPartitionsMismatch(QuixException): ...
