import logging
import time
from collections import defaultdict
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


class IndexFile(TypedDict):
    topic: str
    partition: int
    start_offset: int
    key: str
    files: list[FileMetadata]
    created_at: int


class QuixDatalakeSink(BaseSink):
    """
    Sink for persisting raw Kafka topic data to Blob Storage in Avro format, with Hive-style partitioning and Parquet metadata for Quix DataLake (DuckDB-compatible).
    """

    def __init__(
        self,
        storage_url: str,
        storage_options: Optional[Dict[str, Any]] = None,
        datacatalog_api_url: Optional[str] = None,
        avro_compression: str = "snappy",
        datacatalog_timeout_sec: int = 5,
    ) -> None:
        """
        Initialize the QuixDatalakeSink.

        :param storage_url: The root URL for storage (e.g., 's3://bucket/path', 'file:///tmp/path').
        :param storage_options: Extra options for the fsspec filesystem (e.g., credentials).
        :param datacatalog_api_url: Optional URL for the Data Catalog API to notify after flush.
        :param avro_compression: Avro compression codec to use (default: 'snappy').
        :param datacatalog_timeout_sec: Timeout in seconds for Data Catalog notification (default: 5).
        """
        super().__init__()
        self.storage_url = storage_url
        self.storage_options = storage_options
        self.datacatalog_api_url = datacatalog_api_url
        self.avro_compression = avro_compression
        self.datacatalog_timeout_sec = datacatalog_timeout_sec

        self._fs: fsspec.AbstractFileSystem = fsspec.filesystem(
            self._get_protocol(storage_url), **(storage_options or {})
        )
        self._http_session = requests.Session()
        self._topic_partition_data: dict[str, dict[int, Batch]] = defaultdict(dict)
        self._index: dict[tuple[str, int, str], IndexFile] = {}

    def _get_protocol(self, url: str) -> str:
        """
        Extract the protocol from a storage URL (e.g., 's3', 'file').

        :param url: The storage URL.
        :return: The protocol string.
        """
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
        """
        Add a record to the sink buffer for a given topic and partition.

        :param value: The record value (bytes or serializable object).
        :param key: The record key (str or bytes).
        :param timestamp: The record timestamp (epoch ms).
        :param headers: List of (key, value) tuples for record headers.
        :param topic: Kafka topic name.
        :param partition: Kafka partition number.
        :param offset: Kafka record offset.
        """
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
        """
        Persist all buffered records to Avro and Parquet files, and notify Data Catalog if configured.
        Clears the buffer after flushing.
        """
        if not self._topic_partition_data:
            logger.debug("No records to flush")
            return

        for topic, partitions in self._topic_partition_data.items():
            for partition, batch in partitions.items():
                start = time.time()
                for records in batch["keys"].values():
                    self._persist_records(records, batch)

                elapsed = time.time() - start
                logger.info(
                    f"Persisted records from {topic}[{partition}]{batch['start_offset']}..{batch['end_offset']} in {elapsed:.3f}s with {len(batch['keys'])} unique keys"
                )

            self._notify_datacatalog(topic)

        self._topic_partition_data.clear()

    def _persist_records(self, records: list[AvroItem], batch: Batch) -> None:
        """
        Write a batch of records for a single key to Avro and update the Parquet index.

        :param records: List of AvroItem dicts for a single key.
        :param batch: Batch metadata for the topic/partition.
        """
        start = time.time()

        decoded_key = records[0]["key"].decode("utf-8", errors="backslashreplace")

        file = self._write_avro_file(
            batch,
            decoded_key,
            records,
        )

        index_key = (batch["topic"], batch["partition"], decoded_key)
        if index_key not in self._index:
            # Create a new index entry if it doesn't exist
            self._index[index_key] = {
                "topic": batch["topic"],
                "partition": batch["partition"],
                "start_offset": batch["start_offset"],
                "key": decoded_key,
                "files": [],
                "created_at": round(time.time() * 1000),  # Store as milliseconds
            }

        index = self._index[index_key]
        index["files"].append(file)
        self._write_parquet_index(index)

        if len(index["files"]) >= 100:
            del self._index[index_key]  # Remove index entry if it has too many files

        elapsed = time.time() - start
        logger.debug(
            f"Flushed {file['record_count']} records to {file['path']} "
            f"offsets {file['start_offset']}..{file['end_offset']} "
            f"in {elapsed:.3f}s, {file['file_size_bytes']} bytes"
        )

    def _write_avro_file(
        self,
        batch: Batch,
        key: str,
        records: list[AvroItem],
    ) -> FileMetadata:
        """
        Write records to an Avro file in the configured storage.

        :param batch: Batch metadata for the topic/partition.
        :param key: The decoded key string.
        :param records: List of AvroItem dicts.
        :return: FileMetadata describing the written Avro file.
        """
        start_date = time.strftime("%Y-%m-%d", time.gmtime(batch["min_ts"] // 1000))
        avro_fname = f"ts_{batch['min_ts']}_{batch['max_ts']}_part_{batch['partition']}_off_{batch['start_offset']}_{batch['end_offset']}.avro.{self.avro_compression}"
        avro_path = (
            f"Raw/Topic={batch['topic']}/Key={key}/Start={start_date}/{avro_fname}"
        )
        full_path = f"{self.storage_url.rstrip('/')}/{avro_path}"

        with self._fs.open(full_path, "wb") as f:
            avro_writer(f, AVRO_SCHEMA, records, codec=self.avro_compression)

        # Get file size after closing the file
        file_size: Optional[int] = self._fs.size(full_path)

        file: FileMetadata = {
            "path": avro_path,
            "topic": batch["topic"],
            "key": key,
            "partition": batch["partition"],
            "timestamp_start": batch["min_ts"],
            "timestamp_end": batch["max_ts"],
            "start_offset": batch["start_offset"],
            "end_offset": batch["end_offset"],
            "record_count": len(records),
            "file_size_bytes": 0 if file_size is None else file_size,
            "created_at": round(time.time() * 1000),  # Store as milliseconds
        }
        return file

    def _write_parquet_index(
        self,
        index: IndexFile,
    ) -> None:
        """
        Write or update the Parquet index file for a given key/partition.

        :param index: IndexFile metadata for the key/partition.
        """
        index_path = f"Metadata/Topic={index['topic']}/Key={index['key']}/index_raw_{index['partition']}_{index['start_offset']}.parquet"
        full_index_path = f"{self.storage_url.rstrip('/')}/{index_path}"
        with self._fs.open(full_index_path, "wb") as f:
            pq.write_table(pa.Table.from_pylist(index["files"]), f)

    def _notify_datacatalog(self, topic: str) -> None:
        """
        Notify the Data Catalog API to refresh cache for a given topic, if configured.

        :param topic: The Kafka topic name.
        """
        if not self.datacatalog_api_url:
            return
        logger.debug(f"Notifying Data Catalog for topic: {topic}")
        url = f"{self.datacatalog_api_url}/{topic}/refresh-cache"
        try:
            resp = self._http_session.post(url, timeout=self.datacatalog_timeout_sec)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Data Catalog notification failed: {url} error={e}")
            return
        logger.debug(f"Notified Data Catalog at: {url}")
