import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import gzip
from quixstreams.models.messages import KafkaMessage
from io import BytesIO
from typing import Any, List

from ..file_formats import BatchFormat


class ParquetFormat(BatchFormat):
    # TODO: Docs
    def __init__(
        self,
        file_extension: str = ".parquet",
        compress: bool = False,
        compression_type: str = "snappy"  # Parquet compression: snappy, gzip, none, etc.
    ):
        self._compress = compress
        self._compression_type = compression_type if compress else "none"
        self._file_extension = file_extension

    @property
    def file_extension(self) -> str:
        return self._file_extension
    
    @property
    def supports_append(self) -> bool:
        return True

    #TODO: Convert this to return KafkaMessages.
    def deserialize_value(self, value: bytes) -> Any:
        # Use pyarrow to load Parquet data
        with BytesIO(value) as f:
            table = pq.read_table(f)
            return table.to_pydict()

    def serialize_batch_values(self, values: List[Any]) -> bytes:
        # Get all unique keys (columns) across all rows
        all_keys = set()
        for row in values:
            all_keys.update(row.value.keys())

        # Normalize rows: Ensure all rows have the same keys, filling missing ones with None
        normalized_values = [
            {key: row.value.get(key, None) for key in all_keys} for row in values
        ]

        columns = {
            "timestamp": [row.timestamp for row in values], 
            "key": [bytes.decode(row.key) for row in values]
        }

        # Convert normalized values to a pyarrow Table
        columns = {**columns, **{key: [row[key] for row in normalized_values] for key in all_keys}}
                   
        table = pa.Table.from_pydict(columns)

        with BytesIO() as f:
            pq.write_table(table, f, compression=self._compression_type)
            value_bytes = f.getvalue()

            if self._compress and self._compression_type == "none":  # Handle manual gzip if no Parquet compression
                value_bytes = gzip.compress(value_bytes)

            return value_bytes