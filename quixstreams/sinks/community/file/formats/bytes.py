from gzip import compress as gzip_compress

from quixstreams.sinks.base import SinkItem

from .base import BatchFormat

__all__ = ["BytesFormat"]


class BytesFormat(BatchFormat):
    """
    Bypass formatter to serialize
    """

    def __init__(
        self,
        separator: bytes = b"\n",
        file_extension: str = ".bin",
        compress: bool = False,
    ) -> None:
        self._separator = separator
        self._file_extension = file_extension
        self._compress = compress
        if self._compress:
            self._file_extension += ".gz"

    @property
    def file_extension(self) -> str:
        return self._file_extension

    def serialize(self, messages: list[SinkItem]) -> bytes:
        value_bytes = self._separator.join(messages)
        if self._compress:
            value_bytes = gzip_compress(value_bytes)
        return value_bytes
