import contextlib
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO, Literal, Union

__all__ = (
    "Decompressor",
    "CompressionName",
)


CompressionName = Literal["gz", "gzip"]


class Decompressor(ABC):
    @abstractmethod
    def _decompress(self, filestream: BinaryIO) -> bytes: ...

    @contextlib.contextmanager
    def _open(self, file: Union[Path, BinaryIO]) -> BinaryIO:
        if isinstance(file, Path):
            with open(file, "rb") as f:
                yield f
        else:
            yield file

    def decompress(self, file: Union[Path, BinaryIO]) -> bytes:
        with self._open(file) as filestream:
            return self._decompress(filestream)
