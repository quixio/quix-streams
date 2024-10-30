import gzip
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

from quixstreams.sinks.base import SinkItem

from .base import Format

__all__ = ["ParquetFormat"]


class ParquetFormat(Format):
    # TODO: Docs
    def __init__(
        self,
        file_extension: str = ".parquet",
        compress: bool = False,
        compression_type: str = "snappy",  # Parquet compression: snappy, gzip, none, etc.
    ) -> None:
        self._compress = compress
        self._compression_type = compression_type if compress else "none"
        self._file_extension = file_extension

    @property
    def file_extension(self) -> str:
        return self._file_extension

    @property
    def supports_append(self) -> bool:
        return True

    def serialize(self, messages: list[SinkItem]) -> bytes:
        # Get all unique keys (columns) across all messages
        all_keys = set()
        for message in messages:
            all_keys.update(message.value.keys())

        # Normalize messages: Ensure all messages have the same keys, filling missing ones with None
        normalized_messages = [
            {key: message.value.get(key, None) for key in all_keys}
            for message in messages
        ]

        columns = {
            "timestamp": [message.timestamp for message in messages],
            "key": [bytes.decode(message.key) for message in messages],
        }

        # Convert normalized messages to a pyarrow Table
        columns = {
            **columns,
            **{
                key: [message[key] for message in normalized_messages]
                for key in all_keys
            },
        }

        table = pa.Table.from_pydict(columns)

        with BytesIO() as f:
            pq.write_table(table, f, compression=self._compression_type)
            value_bytes = f.getvalue()

            if (
                self._compress and self._compression_type == "none"
            ):  # Handle manual gzip if no Parquet compression
                value_bytes = gzip.compress(value_bytes)

            return value_bytes
