from abc import ABC, abstractmethod

from quixstreams.sinks.base import SinkBatch

__all__ = ["Format"]


class Format(ABC):
    """
    Base class for formatting batches in file sinks.

    This abstract base class defines the interface for batch formatting
    in file sinks. Subclasses should implement the `file_extension`
    property and the `serialize` method to define how batches are
    formatted and saved.
    """

    @property
    @abstractmethod
    def file_extension(self) -> str:
        """
        Returns the file extension used for output files.

        :return: The file extension as a string.
        """
        ...

    @property
    @abstractmethod
    def supports_append(self) -> bool:
        """
        Indicates if the format supports appending data to an existing file.

        :return: True if appending is supported, otherwise False.
        """
        ...

    @abstractmethod
    def serialize(self, batch: SinkBatch) -> bytes:
        """
        Serializes a batch of messages into bytes.

        :param batch: The batch of messages to serialize.
        :return: The serialized batch as bytes.
        """
        ...
