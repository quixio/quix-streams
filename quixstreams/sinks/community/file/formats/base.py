from abc import ABC, abstractmethod

from quixstreams.sinks.base import SinkItem

__all__ = ["Format"]


# TODO: Document the compatible topic formats for each formatter
# TODO: Check the types of the values before serializing


class Format(ABC):
    """
    Base class to format batches for File Sink.

    This abstract base class defines the interface for batch formatting
    in file sinks. Subclasses should implement the `file_extension`
    property and the `serialize` method to define how messages are
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

    @abstractmethod
    def serialize(self, messages: list[SinkItem]) -> bytes:
        """
        Serializes a list of messages into a byte string.

        :param messages: The list of messages to serialize.
        :return: The serialized messages as bytes.
        """
        ...
