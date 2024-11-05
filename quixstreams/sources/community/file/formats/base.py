from abc import ABC, abstractmethod
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Generator, Iterable, Literal, Optional

from ..compressions import COMPRESSION_MAPPER, CompressionName, Decompressor

__all__ = ("Format", "FormatName")


FormatName = Literal["json", "parquet"]


class Format(ABC):
    """
    Base class for reading files serialized by the Quix Streams File Sink
    Connector.

    Formats include things like JSON, Parquet, etc.

    Also handles different compression types.
    """

    @abstractmethod
    def __init__(self, compression: Optional[CompressionName] = None):
        """
        super().__init__() this for a usable init.
        """
        self._file: Optional[BinaryIO] = None
        self._decompressor: Optional[Decompressor] = None
        if compression:
            self._set_decompressor(compression)

    @abstractmethod
    def deserialize(self, filestream: BinaryIO) -> Iterable[dict]:
        """
        Parse a filelike byte stream into a collection of records
        using the designated format's deserialization approach.

        The opening, decompression, and closing of the byte stream's origin is handled
        automatically.

        The iterable should output dicts with the following data/naming structure:
        {_key: str, _value: dict, _timestamp: int}.

        :param filestream: a filelike byte stream (such as `f` from `f = open(file)`)
        :return:
        """
        ...

    def _set_decompressor(self, extension_or_name: CompressionName):
        self._decompressor = COMPRESSION_MAPPER[extension_or_name]()

    def _open_filestream(self, filepath: Path):
        # TODO: maybe check that file extension is valid?
        if self._decompressor:
            self._file = BytesIO(self._decompressor.decompress(filepath))
        else:
            self._file = open(filepath, "rb")

    def _close_filestream(self):
        if self._file:
            self._file.close()
        self._file = None

    def file_read(self, filepath: Path) -> Generator[dict, None, None]:
        self._open_filestream(filepath)
        yield from self.deserialize(self._file)
        self._close_filestream()
