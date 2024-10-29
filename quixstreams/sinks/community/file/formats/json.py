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
        file_extension: str = ".jsonl",
        compress: bool = False,
        dumps: Optional[Callable[[Any], str]] = None,
    ):
        self._file_extension = file_extension

        self._compress = compress
        if self._compress:
            self._file_extension += ".gz"

        self._writer_arguments = {"compact": True}

        # If `dumps` is provided, `compact` will be ignored
        if dumps is not None:
            self._writer_arguments["dumps"] = dumps

    @property
    def file_extension(self) -> str:
        return self._file_extension

    def serialize(self, messages: list[any]) -> bytes:
        _to_str = bytes.decode if isinstance(messages[0].key, bytes) else str

        with BytesIO() as fp:
            with Writer(fp, **self._writer_arguments) as writer:
                writer.write_all(
                    {
                        "timestamp": message.timestamp,
                        "key": _to_str(message.key),
                        "value": message.value,
                    }
                    for message in messages
                )

            value = fp.getvalue()

        return gzip_compress(value) if self._compress else value
