from pathlib import Path

from .base import Decompressor

__all__ = ("GZipDecompressor",)


class GZipDecompressor(Decompressor):
    def __init__(self):
        from gzip import decompress

        self._decompressor = decompress

    def decompress(self, filepath: Path) -> bytes:
        with open(filepath, "rb") as f:
            return self._decompressor(f.read())
