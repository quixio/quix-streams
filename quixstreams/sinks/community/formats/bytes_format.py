from gzip import compress as gzip_compress
from typing import Any

from ..file_formats import BatchFormat


class BytesFormat(BatchFormat):
    """
    Bypass formatter to serialize
    """

    def __init__(
        self,
        separator: bytes = b"\n",
        file_extension: str = ".bin",
        compress: bool = False,
    ):
        self._separator = separator
        self._file_extension = file_extension
        self._compress = compress
        if self._compress:
            self._file_extension += ".gz"

    @property
    def file_extension(self) -> str:
        return self._file_extension

    # TODO: Make this returns KafkaMessage.
    def deserialize_value(self, value: bytes) -> Any:
        return value

    def serialize_batch_values(self, values: list[bytes]) -> bytes:
        value_bytes = self._separator.join(values)
        if self._compress:
            value_bytes = gzip_compress(value_bytes)
        return value_bytes
