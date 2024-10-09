import abc

from typing import List

from quixstreams.sinks.base.item import SinkItem

__all__ = ("FileFormatter",)


class FileFormatter:
    """
    Base class to format batches for S3 Sink
    """

    @property
    @abc.abstractmethod
    def file_extension(self) -> str: ...

    @abc.abstractmethod
    def write_batch_values(self, filepath: str, items: List[SinkItem]) -> None: ...
