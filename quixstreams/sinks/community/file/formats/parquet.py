from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from quixstreams.sinks.base.item import SinkItem
from .base import FileFormatter

__all__ = ("ParquetFormatter",)


class ParquetFormatter(FileFormatter):
    def __init__(
        self,
        compression: str = "snappy",  # Parquet compression: snappy, gzip, none, etc.
    ):
        self._compression_type = compression

    @property
    def file_extension(self) -> str:
        return ".parquet"

    def write_batch_values(self, filepath: str, items: List[SinkItem]) -> None:
        data = [
            {"key": item.key, "value": item.value, "timestamp": item.timestamp}
            for item in items
        ]
        table = pa.Table.from_pylist(data)
        pq.write_table(table, filepath, compression=self._compression_type)
