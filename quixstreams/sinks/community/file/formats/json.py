from gzip import compress as gzip_compress
from io import BytesIO
from typing import Any, Callable, Optional

from jsonlines import Writer

from quixstreams.sinks.base import SinkItem

from .base import BatchFormat

__all__ = ["JSONFormat"]


class JSONFormat(BatchFormat):
    """
    Serializes messages into JSON Lines format with optional gzip compression.

    This class provides functionality to serialize a list of messages into bytes
    in JSON Lines format. It supports optional gzip compression and allows for
    custom JSON serialization through the `dumps` parameter.
    """

    def __init__(
        self,
        file_extension: str = ".jsonl",
        compress: bool = False,
        dumps: Optional[Callable[[Any], str]] = None,
    ) -> None:
        """
        Initializes the JSONFormat.

        :param file_extension: The file extension to use for output files.
            Defaults to ".jsonl".
        :param compress: If `True`, compresses the output using gzip and appends
            ".gz" to the file extension. Defaults to `False`.
        :param dumps: A custom function to serialize objects to JSON-formatted strings.
        """
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

    def serialize(self, messages: list[SinkItem]) -> bytes:
        """
        Serializes a list of messages into JSON Lines format.

        Each message is converted into a JSON object with "timestamp", "key",
        and "value" fields. If the message key is in bytes, it is decoded to a
        string.

        :param messages: The list of messages to serialize.
        :return: The serialized messages in JSON Lines format, optionally
            compressed with gzip.
        """

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
