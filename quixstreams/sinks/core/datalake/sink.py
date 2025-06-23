import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Optional, TypedDict, Union

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from fastavro import writer as avro_writer

from quixstreams.models.types import HeadersTuples
from quixstreams.sinks.base.sink import BaseSink

logger = logging.getLogger(__name__)

AVRO_SCHEMA = {
    "type": "record",
    "name": "KafkaMessage",
    "fields": [
        {"name": "timestamp", "type": "long"},
        {"name": "key", "type": ["null", "bytes"]},
        {
            "name": "headers",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Header",
                    "fields": [
                        {"name": "key", "type": "string"},
                        {"name": "value", "type": ["string", "bytes"]},
                    ],
                },
            },
        },
        {"name": "value", "type": "bytes"},
        {"name": "offset", "type": "long"},
    ],
}


@dataclass
class QuixDatalakeSinkConfig:
    storage_url: str  # fsspec URL (e.g., abfs://, s3://, etc)
    storage_options: Optional[Dict[str, Any]] = None
    datacatalog_api_url: Optional[str] = None
    avro_compression: str = "snappy"
    datacatalog_timeout_sec: int = 5


class AvroItem(TypedDict):
    value: Any
    key: bytes
    timestamp: int
    headers: list[dict[str, Union[str, bytes]]]
    offset: int


class Batch(TypedDict):
    topic: str
    partition: int
    min_ts: int
    max_ts: int
    start_offset: int
    end_offset: int
    keys: Dict[bytes, list[AvroItem]]  # key -> list of items with that key


class FileMetadata(TypedDict):
    path: str
    topic: str
    key: str
    partition: int
    timestamp_start: int
    timestamp_end: int
    start_offset: int
    end_offset: int
    record_count: int
    file_size_bytes: int
    created_at: int


BufferKey = tuple[str, int, bytes]  # (topic, partition, key)


class QuixDatalakeSink(BaseSink):
    """
    Sink for persisting raw Kafka topic data to Blob Storage in Avro format, with Hive-style partitioning and Parquet metadata for Quix DataLake (DuckDB-compatible).
    """

    def __init__(self, config: QuixDatalakeSinkConfig) -> None:
        super().__init__()
        self.config = config

        self.fs: fsspec.AbstractFileSystem = fsspec.filesystem(
            self._get_protocol(config.storage_url), **(config.storage_options or {})
        )
        self._http_session = requests.Session()
        # Change: topic -> partition -> Batch
        self._topic_partition_data: dict[str, dict[int, Batch]] = defaultdict(dict)

    def _get_protocol(self, url: str) -> str:
        return url.split(":")[0]

    def add(
        self,
        value: Any,
        key: Union[str, bytes],
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        if partition not in self._topic_partition_data[topic]:
            self._topic_partition_data[topic][partition] = {
                "topic": topic,
                "partition": partition,
                "min_ts": timestamp,
                "max_ts": timestamp,
                "start_offset": offset,
                "end_offset": offset,
                "keys": defaultdict(list),
            }
        else:
            meta = self._topic_partition_data[topic][partition]
            meta["min_ts"] = min(meta["min_ts"], timestamp)
            meta["max_ts"] = max(meta["max_ts"], timestamp)
            meta["end_offset"] = offset

        k: bytes = key.encode() if isinstance(key, str) else key
        self._topic_partition_data[topic][partition]["keys"][k].append(
            {
                "value": value,
                "key": k,
                "timestamp": timestamp,
                "headers": [{"key": k, "value": v} for k, v in headers],
                "offset": offset,
            }
        )

    def flush(self) -> None:
        if not self._topic_partition_data:
            logger.debug("No records to flush")
            return

        for topic, partitions in self._topic_partition_data.items():
            for partition, batch in partitions.items():
                start = time.time()

                created_files = self._flush_batch(batch)
                self._update_metadata(
                    topic=topic,
                    partition=partition,
                    start_offset=batch["start_offset"],
                    end_offset=batch["end_offset"],
                    files=created_files,
                )

                elapsed = time.time() - start
                logger.info(
                    f"Persisted records from {topic}[{partition}]{batch['start_offset']}..{batch['end_offset']} in {elapsed:.3f}s with {len(batch['keys'])} unique keys"
                )

            self._notify_datacatalog(topic)

        self._topic_partition_data.clear()

    def _flush_batch(self, batch: Batch) -> list[FileMetadata]:
        created_files = []
        for key, records in batch["keys"].items():
            start = time.time()
            metadata = self._write_avro_file(
                batch,
                key,
                records,
            )
            elapsed = time.time() - start
            logger.debug(
                f"Flushed {metadata['record_count']} records to {metadata['path']} "
                f"offsets {metadata['start_offset']}..{metadata['end_offset']} "
                f"in {elapsed:.3f}s, {metadata['file_size_bytes']} bytes"
            )
            created_files.append(metadata)
        return created_files

    def _write_avro_file(
        self,
        batch: Batch,
        key: bytes,
        records: list[AvroItem],
    ) -> FileMetadata:
        record_count = len(records)
        start_date = time.strftime("%Y-%m-%d", time.gmtime(batch["min_ts"] // 1000))
        decoded_key = key.decode("utf-8", errors="backslashreplace")
        avro_fname = f"ts_{batch['min_ts']}_{batch['max_ts']}_part_{batch['partition']}_off_{batch['start_offset']}_{batch['end_offset']}.avro.{self.config.avro_compression}"
        avro_path = f"Raw/Topic={batch['topic']}/Key={decoded_key}/Start={start_date}/{avro_fname}"
        full_path = f"{self.config.storage_url.rstrip('/')}/{avro_path}"

        with self.fs.open(full_path, "wb") as f:
            avro_writer(f, AVRO_SCHEMA, records, codec=self.config.avro_compression)

        # Get file size after closing the file
        file_size: Optional[int] = self.fs.size(full_path)

        file: FileMetadata = {
            "path": avro_path,
            "topic": batch["topic"],
            "key": decoded_key,
            "partition": batch["partition"],
            "timestamp_start": batch["min_ts"],
            "timestamp_end": batch["max_ts"],
            "start_offset": batch["start_offset"],
            "end_offset": batch["end_offset"],
            "record_count": record_count,
            "file_size_bytes": 0 if file_size is None else file_size,
            "created_at": round(time.time() * 1000),  # Store as milliseconds
        }
        return file

    def _update_metadata(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        end_offset: int,
        files: list[FileMetadata],
    ) -> None:
        index_path = f"Metadata/Topic={topic}/index_{partition}_{start_offset}_{end_offset}.parquet"
        full_index_path = f"{self.config.storage_url.rstrip('/')}/{index_path}"

        # Write all metadata rows at once, overwriting the file
        with self.fs.open(full_index_path, "wb") as f:
            pq.write_table(pa.Table.from_pylist(files), f)

    def _notify_datacatalog(self, topic: str) -> None:
        if not self.config.datacatalog_api_url:
            return
        logger.debug(f"Notifying Data Catalog for topic: {topic}")
        url = f"{self.config.datacatalog_api_url}/{topic}/refresh-cache"
        try:
            resp = self._http_session.post(
                url, timeout=self.config.datacatalog_timeout_sec
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Data Catalog notification failed: {url} error={e}")
            return
        logger.debug(f"Notified Data Catalog at: {url}")
