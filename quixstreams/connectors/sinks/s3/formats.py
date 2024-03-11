import abc
import json
from gzip import compress as gzip_compress
from io import BytesIO
from typing import Optional, Callable, Any, List

from jsonlines import Writer

__all__ = ("S3SinkBatchFormat", "JSONFormat", "BytesFormat")


# TODO: Document the compatible topic formats for each formatter
# TODO: Check the types of the values before serializing


class S3SinkBatchFormat:
    """
    Base class to format batches for S3 Sink
    """

    @property
    @abc.abstractmethod
    def file_extension(self) -> str:
        ...

    @abc.abstractmethod
    def deserialize_value(self, value: bytes) -> Any:
        ...

    @abc.abstractmethod
    def serialize_batch_values(self, values: List[Any]) -> bytes:
        ...


class BytesFormat(S3SinkBatchFormat):
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

    def deserialize_value(self, value: bytes) -> Any:
        return value

    def serialize_batch_values(self, values: List[bytes]) -> bytes:
        value_bytes = self._separator.join(values)
        if self._compress:
            value_bytes = gzip_compress(value_bytes)
        return value_bytes


class JSONFormat(S3SinkBatchFormat):
    # TODO: Docs
    def __init__(
        self,
        dumps: Optional[Callable[[Any], bytes]] = None,
        loads: Optional[Callable[[bytes], Any]] = None,
        file_extension: str = ".json",
        compress: bool = False,
    ):
        self._dumps = dumps or json.dumps
        self._loads = loads or json.loads
        self._compress = compress
        self._file_extension = file_extension
        if self._compress:
            self._file_extension += ".gz"

    @property
    def file_extension(self) -> str:
        return self._file_extension

    def deserialize_value(self, value: bytes) -> Any:
        # TODO: Wrap an exception with more info here
        return self._loads(value)

    def serialize_batch_values(self, values: List[Any]) -> bytes:
        with BytesIO() as f:
            with Writer(f, compact=True, dumps=self._dumps) as writer:
                writer.write_all(values)
            value_bytes = f.getvalue()
            if self._compress:
                value_bytes = gzip_compress(value_bytes)
            return value_bytes
