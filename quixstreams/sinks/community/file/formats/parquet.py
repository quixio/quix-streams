from io import BytesIO
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq

from quixstreams.sinks.base import SinkItem

from .base import Format

__all__ = ["ParquetFormat"]

Compression = Literal["NONE", "SNAPPY", "GZIP", "BROTLI", "LZ4", "ZSTD"]


class ParquetFormat(Format):
    # TODO: Docs
    def __init__(
        self,
        file_extension: str = ".parquet",
        compression: Compression = "SNAPPY",
    ) -> None:
        self._file_extension = file_extension
        self._compression = compression

    @property
    def file_extension(self) -> str:
        return self._file_extension

    @property
    def supports_append(self) -> bool:
        return True

    def serialize(self, messages: list[SinkItem]) -> bytes:
        _to_str = bytes.decode if isinstance(messages[0].key, bytes) else str

        # Get all unique keys (columns) across all messages
        columns = set()
        for message in messages:
            columns.update(message.value.keys())

        # Normalize messages: Ensure all messages have the same keys,
        # filling missing ones with None.
        normalized_messages = [
            {
                "timestamp": message.timestamp,
                "key": _to_str(message.key),
                **{column: message.value.get(column, None) for column in columns},
            }
            for message in messages
        ]

        # Convert normalized messages to a pyarrow Table
        table = pa.Table.from_pylist(normalized_messages)

        with BytesIO() as fp:
            pq.write_table(table, fp, compression=self._compression)
            return fp.getvalue()
