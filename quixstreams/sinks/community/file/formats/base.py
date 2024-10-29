import abc
from typing import Any

__all__ = ["BatchFormat"]


# TODO: Document the compatible topic formats for each formatter
# TODO: Check the types of the values before serializing


class BatchFormat:
    """
    Base class to format batches for S3 Sink
    """

    @property
    @abc.abstractmethod
    def file_extension(self) -> str: ...

    @abc.abstractmethod
    def serialize_batch_values(self, values: list[Any]) -> bytes: ...
