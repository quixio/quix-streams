import contextlib
from abc import ABC, abstractmethod
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Generator, Iterable, Literal, Optional, Union

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

    def _decompress(self, filestream: BinaryIO) -> BinaryIO:
        if not self._decompressor:
            return filestream
        return BytesIO(self._decompressor.decompress(filestream))

    @contextlib.contextmanager
    def _open(self, file: Union[Path, BinaryIO]) -> BinaryIO:
        if isinstance(file, Path):
            with open(file, "rb") as f:
                # yield for when no decompression is done
                yield self._decompress(f)
        else:
            yield self._decompress(file)

    def _set_decompressor(self, extension_or_name: CompressionName):
        self._decompressor = COMPRESSION_MAPPER[extension_or_name]()

    def file_read(self, file: Union[Path, BinaryIO]) -> Generator[dict, None, None]:
        with self._open(file) as filestream:
            yield from self.deserialize(filestream)
