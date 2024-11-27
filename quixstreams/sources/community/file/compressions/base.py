from abc import ABC, abstractmethod
from typing import BinaryIO, Literal

__all__ = (
    "Decompressor",
    "CompressionName",
)


CompressionName = Literal["gz", "gzip"]


class Decompressor(ABC):
    @abstractmethod
    def decompress(self, filestream: BinaryIO) -> bytes: ...
