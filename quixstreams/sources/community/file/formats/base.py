from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable

from ..compressions import COMPRESSION_MAPPER, CompressionName, Decompressor

__all__ = ("Format", "FormatName")


FormatName = ["json", "parquet"]


class Format(ABC):
    """
    Base class for reading files serialized by the Quix Streams File Sink
    Connector.

    Formats include things like JSON, Avro, Parquet, etc.

    Also handles different compression types.
    """

    @staticmethod
    def _get_decompressor(extension_or_name: CompressionName) -> Decompressor:
        return COMPRESSION_MAPPER[extension_or_name]()

    @abstractmethod
    def deserialize(self, filepath: Path) -> Iterable[dict]:
        """
        Serializes a list of messages into a byte string.

        :return: The serialized messages as bytes.
        """
        ...
