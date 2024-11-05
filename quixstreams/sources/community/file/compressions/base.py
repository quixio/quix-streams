from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal

__all__ = (
    "Decompressor",
    "CompressionName",
)


CompressionName = Literal["gz", "gzip"]


class Decompressor(ABC):
    @abstractmethod
    def decompress(self, filepath: Path) -> bytes: ...
