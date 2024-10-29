from abc import ABC, abstractmethod

from quixstreams.sinks.base import SinkItem

__all__ = ["BatchFormat"]


# TODO: Document the compatible topic formats for each formatter
# TODO: Check the types of the values before serializing


class BatchFormat(ABC):
    """
    Base class to format batches for File Sink
    """

    @property
    @abstractmethod
    def file_extension(self) -> str: ...

    @abstractmethod
    def serialize(self, messages: list[SinkItem]) -> bytes: ...
