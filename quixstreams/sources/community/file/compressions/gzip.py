from gzip import decompress
from typing import BinaryIO

from .base import Decompressor

__all__ = ("GZipDecompressor",)


class GZipDecompressor(Decompressor):
    def __init__(self):
        self._decompressor = decompress

    def decompress(self, filestream: BinaryIO) -> bytes:
        return self._decompressor(filestream.read())
