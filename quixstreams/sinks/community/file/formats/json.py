from gzip import compress as gzip_compress
from io import BytesIO
from typing import Any, Callable, Optional

from jsonlines import Writer

from quixstreams.sinks.base import SinkBatch

from .base import Format

__all__ = ["JSONFormat"]


class JSONFormat(Format):
    """
    Serializes batches of messages into JSON Lines format with optional gzip
    compression.

    This class provides functionality to serialize a `SinkBatch` into bytes
    in JSON Lines format. It supports optional gzip compression and allows
    for custom JSON serialization through the `dumps` parameter.

    This format supports appending to existing files.
    """

    supports_append = True

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
        :param compress: If `True`, compresses the output using gzip and
            appends ".gz" to the file extension. Defaults to `False`.
        :param dumps: A custom function to serialize objects to JSON-formatted
            strings. If provided, the `compact` option is ignored.
        """
        self._file_extension = file_extension

        self._compress = compress
        if self._compress:
            self._file_extension += ".gz"

        self._writer_arguments: dict[str, Any] = {"compact": True}

        # If `dumps` is provided, `compact` will be ignored
        if dumps is not None:
            self._writer_arguments["dumps"] = dumps

    @property
    def file_extension(self) -> str:
        """
        Returns the file extension used for output files.

        :return: The file extension as a string.
        """
        return self._file_extension

    def serialize(self, batch: SinkBatch) -> bytes:
        """
        Serializes a `SinkBatch` into bytes in JSON Lines format.

        Each item in the batch is converted into a JSON object with
        "_timestamp", "_key", and "_value" fields. If the message key is
        in bytes, it is decoded to a string.

        :param batch: The `SinkBatch` to serialize.
        :return: The serialized batch in JSON Lines format, optionally
            compressed with gzip.
        """

        with BytesIO() as fp:
            with Writer(fp, **self._writer_arguments) as writer:
                writer.write_all(
                    {
                        "_timestamp": item.timestamp,
                        "_key": item.key.decode()
                        if isinstance(item.key, bytes)
                        else str(item),
                        "_value": item.value,
                    }
                    for item in batch
                )

            value = fp.getvalue()

        return gzip_compress(value) if self._compress else value
