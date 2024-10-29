import json
from gzip import compress as gzip_compress
from io import BytesIO
from typing import Any, Callable, Optional

from jsonlines import Writer

from .base import BatchFormat

__all__ = ["JSONFormat"]


class JSONFormat(BatchFormat):
    # TODO: Docs
    def __init__(
        self,
        dumps: Optional[Callable[[Any], bytes]] = json.dumps,
        file_extension: str = ".jsonl",
        compress: bool = False,
    ):
        self._dumps = dumps
        self._compress = compress
        self._file_extension = file_extension
        if self._compress:
            self._file_extension += ".gz"

    @property
    def file_extension(self) -> str:
        return self._file_extension

    def serialize(self, messages: list[any]) -> bytes:
        _to_str = bytes.decode if isinstance(messages[0].key, bytes) else str

        with BytesIO() as f:
            with Writer(f, compact=True, dumps=self._dumps) as writer:
                for message in messages:
                    obj = {
                        "timestamp": message.timestamp,
                        "key": _to_str(message.key),
                        "value": json.dumps(message.value),
                    }
                    writer.write(obj)
            value_bytes = f.getvalue()
            if self._compress:
                return gzip_compress(value_bytes)
            return value_bytes
