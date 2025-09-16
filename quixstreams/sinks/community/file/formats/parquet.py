from io import BytesIO
from typing import Literal

from quixstreams.sinks.base import SinkBatch

from .base import Format

__all__ = ["ParquetFormat"]

Compression = Literal["none", "snappy", "gzip", "brotli", "lz4", "zstd"]


class ParquetFormat(Format):
    """
    Serializes batches of messages into Parquet format.

    This class provides functionality to serialize a `SinkBatch` into bytes
    in Parquet format using PyArrow. It allows setting the file extension
    and compression algorithm used for the Parquet files.

    This format does not support appending to existing files.
    """

    supports_append = False

    def __init__(
        self,
        file_extension: str = ".parquet",
        compression: Compression = "snappy",
    ) -> None:
        """
        Initializes the ParquetFormat.

        :param file_extension: The file extension to use for output files.
            Defaults to ".parquet".
        :param compression: The compression algorithm to use for Parquet files.
            Allowed values are "none", "snappy", "gzip", "brotli", "lz4",
            or "zstd". Defaults to "snappy".
        """
        self._file_extension = file_extension
        self._compression = compression
        self._write = self._get_writer()

    def _get_writer(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                f"Package {exc.name} is missing: "
                'run "pip install quixstreams[parquet]" to use the Parquet file format '
                '(Note: options can be installed together i.e. "quixstreams[s3,parquet]")'
            ) from exc

        def _writer(messages: list[dict], fp: BytesIO):
            return pq.write_table(
                pa.Table.from_pylist(messages), fp, compression=self._compression
            )

        return _writer

    @property
    def file_extension(self) -> str:
        """
        Returns the file extension used for output files.

        :return: The file extension as a string.
        """
        return self._file_extension

    def serialize(self, batch: SinkBatch) -> bytes:
        """
        Serializes a `SinkBatch` into bytes in Parquet format.

        Each item in the batch is converted into a dictionary with "_timestamp",
        "_key", and the keys from the message value. If the message key is in
        bytes, it is decoded to a string.

        Missing fields in messages are filled with `None` to ensure all rows
        have the same columns.

        :param batch: The `SinkBatch` to serialize.
        :return: The serialized batch as bytes in Parquet format.
        """

        # Get all unique keys (columns) across all messages
        columns = set()
        for item in batch:
            columns.update(item.value.keys())

        # Normalize messages: Ensure all messages have the same keys,
        # filling missing ones with None.
        normalized_messages = [
            {
                "_timestamp": item.timestamp,
                "_key": item.key.decode() if isinstance(item.key, bytes) else str(item),
                **{column: item.value.get(column, None) for column in columns},
            }
            for item in batch
        ]

        with BytesIO() as fp:
            self._write(normalized_messages, fp)
            return fp.getvalue()
