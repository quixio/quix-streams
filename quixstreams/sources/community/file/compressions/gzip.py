from gzip import decompress
from pathlib import Path

from .base import Decompressor

__all__ = ("GZipDecompressor",)


class GZipDecompressor(Decompressor):
    def decompress(self, filepath: Path) -> bytes:
        with open(filepath, "rb") as f:
            return decompress(f.read())
