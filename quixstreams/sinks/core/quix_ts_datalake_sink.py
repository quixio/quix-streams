"""
Quix Lake Blob Storage Sink

This module provides a sink that writes Kafka batches to blob storage as
Hive-partitioned Parquet files, with optional REST Catalog integration.

Uses quixportal for unified blob storage access (Azure, AWS S3, GCP, MinIO, local).
"""

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[quixdatalake]" '
        "to use QuixTSDataLakeSink"
    ) from exc

from quixstreams.sinks.base import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBatch,
)

from ._blob_storage_client import BlobStorageClient, get_bucket_name
from ._quix_ts_datalake_catalog_client import QuixTSDataLakeCatalogClient

logger = logging.getLogger(__name__)


# Timestamp column mappers for Hive partitioning
TIMESTAMP_COL_MAPPER = {
    "year": lambda col: col.dt.year.astype(str),
    "month": lambda col: col.dt.month.astype(str).str.zfill(2),
    "day": lambda col: col.dt.day.astype(str).str.zfill(2),
    "hour": lambda col: col.dt.hour.astype(str).str.zfill(2),
}


class QuixTSDataLakeSink(BatchingSink):
    """
    Writes Kafka batches directly to blob storage as Hive-partitioned Parquet files,
    then optionally registers the table using the REST Catalog.

    It batches the processed records in memory per topic partition, converts
    them to Parquet format with Hive-style partitioning, and flushes them to
    blob storage at the checkpoint.

    >***NOTE***: QuixTSDataLakeSink can accept only dictionaries.
    > If the record values are not dicts, you need to convert them to dicts before
    > sinking.

    :param s3_prefix: Path prefix for data files (e.g., "data-lake/time-series")
    :param table_name: Table name for registration
    :param workspace_id: Workspace ID for workspace-scoped storage paths
        (auto-injected by platform)
    :param hive_columns: List of columns to use for Hive partitioning. Include
        'year', 'month', 'day', 'hour' to extract these from timestamp_column
    :param timestamp_column: Column containing timestamp to extract time partitions from
    :param catalog_url: Optional REST Catalog URL for table registration
    :param catalog_auth_token: If using REST Catalog, the respective auth token for it
    :param auto_discover: Whether to auto-register table on first write
    :param namespace: Catalog namespace (default: "default")
    :param auto_create_bucket: If True, attempt to create bucket/path in storage if missing
    :param max_workers: Maximum number of parallel upload threads (default: 10)
    :param stream_timeout_ms: Optional silence threshold in milliseconds
        applied to the **whole stream** (all messages regardless of key).
        Must be a positive int. Paired with ``on_stream_timeout``; both must
        be provided to enable the feature.
    :param on_stream_timeout: Optional callback invoked with the stream
        designation (``Callable[[str], None]``) when no messages at all have
        arrived at the sink for at least ``stream_timeout_ms``. The
        designation passed in is the Kafka topic name the sink is attached
        to (captured from the ``topic`` argument of the first ``add()``
        call). Fires exactly once per silence period; the first new message
        after the fire re-arms the timer so the next silence period can
        fire again. Runs synchronously on the sink thread during
        ``flush()``; exceptions are logged and swallowed so a misbehaving
        callback does not kill the sink.

        The feature is **enabled only when both** ``stream_timeout_ms`` is a
        positive int **and** ``on_stream_timeout`` is callable. Any other
        combination (either ``None``, zero, negative, wrong type) disables
        the feature with zero overhead. Keys are **not** tracked
        independently — a single scalar last-seen timestamp covers the
        whole stream.

        *Precision floor:* timeouts fire inside ``flush()``, which is driven
        by the Checkpoint commit interval. Expect fire latency up to
        ``stream_timeout_ms + commit_interval``. For tight timeout detection
        (seconds), lower the Application's ``commit_interval`` accordingly.
        Sub-second timeouts are supported by the API but will not fire
        faster than the flush cadence.

        *Restart behaviour:* tracking state is in-memory only. On process
        restart ``_last_seen_ms`` is reset to ``None``; the sink will not
        fire until it has seen at least one message and then gone silent
        past the threshold.

        *Backpressure:* ``on_paused()`` does not touch ``_last_seen_ms`` or
        ``_fired``. Timeout reflects "when did we last see data in the
        stream", independent of write success.
    :param on_client_connect_success: An optional callback made after successful
        client authentication, primarily for additional logging.
    :param on_client_connect_failure: An optional callback made after failed
        client authentication (which should raise an Exception).
        Callback should accept the raised Exception as an argument.
        Callback must resolve (or propagate/re-raise) the Exception.
    """

    def __init__(
        self,
        s3_prefix: str,
        table_name: str,
        workspace_id: str = "",
        hive_columns: Optional[List[str]] = None,
        timestamp_column: str = "ts_ms",
        catalog_url: Optional[str] = None,
        catalog_auth_token: Optional[str] = None,
        auto_discover: bool = True,
        namespace: str = "default",
        auto_create_bucket: bool = True,
        max_workers: int = 10,
        stream_timeout_ms: Optional[int] = None,
        on_stream_timeout: Optional[Callable[[str], None]] = None,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self.s3_prefix = s3_prefix
        self.table_name = table_name
        self.workspace_id = workspace_id
        self.hive_columns = hive_columns or []
        self.timestamp_column = timestamp_column
        self._catalog = (
            QuixTSDataLakeCatalogClient(catalog_url, catalog_auth_token)
            if catalog_url
            else None
        )
        self.auto_discover = auto_discover
        self.namespace = namespace
        self.table_registered = False

        # Blob storage client and bucket name will be initialized in setup()
        self._blob_client: Optional[BlobStorageClient] = None
        self._s3_bucket: Optional[str] = None
        self._ts_hive_columns = {"year", "month", "day", "hour"} & set(
            self.hive_columns
        )
        self._auto_create_bucket = auto_create_bucket
        self._max_workers = max_workers

        # Batch upload tracking
        self._pending_futures: List[Dict[str, Any]] = []

        # Stream-timeout tracking (opt-in, whole-stream, single scalar
        # state). Feature is enabled iff both scalar params are usable;
        # mismatched pairs raise ValueError. Disabled path is zero-overhead:
        # ``_stream_timeout_enabled`` stays ``False`` and the tracker state
        # fields are never allocated.
        self._stream_timeout_enabled: bool = False
        self._validate_stream_timeout_params(stream_timeout_ms, on_stream_timeout)
        timeout_valid = (
            isinstance(stream_timeout_ms, int)
            and not isinstance(stream_timeout_ms, bool)
            and stream_timeout_ms > 0
        )
        if timeout_valid and callable(on_stream_timeout):
            self._stream_timeout_enabled = True
            self._stream_timeout_ms: int = stream_timeout_ms
            self._on_stream_timeout: Callable[[str], None] = on_stream_timeout
            self._last_seen_ms: Optional[int] = None
            self._fired: bool = False
            self._stream_name: Optional[str] = None
            # v5 loader banner — confirms deployment is running v5 (whole-stream
            # scalar state) vs. v4 (per-key dict) vs. v3 (registration dict).
            # If the deployed pod logs "QuixTSDataLakeSink STREAM-TIMEOUT v5"
            # then the scalar path is live and the callback will always receive
            # the input-topic name, never a per-record key.
            logger.info(
                "QuixTSDataLakeSink STREAM-TIMEOUT v5 loaded "
                "(scalar state: last_seen_ms=%s, fired=%s, stream_name=%s, "
                "threshold_ms=%s); callback receives input-topic name once "
                "per silence period, never a per-record key.",
                self._last_seen_ms,
                self._fired,
                self._stream_name,
                self._stream_timeout_ms,
            )

    @staticmethod
    def _validate_stream_timeout_params(
        stream_timeout_ms: Any, on_stream_timeout: Any
    ) -> None:
        """Validate the two scalar stream-timeout params (spec §6.1).

        Policy:
        - Both ``None`` → disabled silently, no error.
        - Exactly one supplied in a non-``None`` form → raise with the pair
          message so operators catch "set the threshold but forgot the
          callback" loudly at construction time.
        - ``stream_timeout_ms`` provided alongside a callable callback but
          not a positive int → raise with the threshold message.
        - ``on_stream_timeout`` provided alongside a positive int threshold
          but not callable → raise with the callback message.
        """
        # Both defaults: disabled silently.
        if stream_timeout_ms is None and on_stream_timeout is None:
            return

        # Mismatched pair (exactly one is None): fail loud.
        if (stream_timeout_ms is None) != (on_stream_timeout is None):
            raise ValueError(
                "stream_timeout_ms and on_stream_timeout must both be "
                "provided to enable stream-timeout tracking; got "
                f"stream_timeout_ms={stream_timeout_ms!r}, "
                f"on_stream_timeout={on_stream_timeout!r}"
            )

        # Both supplied: validate each.
        # ``bool`` is a subclass of ``int`` in Python; reject explicitly.
        if (
            not isinstance(stream_timeout_ms, int)
            or isinstance(stream_timeout_ms, bool)
            or stream_timeout_ms <= 0
        ):
            raise ValueError(
                "stream_timeout_ms must be a positive int (milliseconds)"
            )
        if not callable(on_stream_timeout):
            raise ValueError("on_stream_timeout must be callable")

    @property
    def s3_bucket(self) -> str:
        """Get the S3 bucket name (extracted from quixportal config)."""
        if self._s3_bucket is None:
            raise RuntimeError("s3_bucket not initialized. Call setup() first.")
        return self._s3_bucket

    # ------------------------------------------------------------------
    # Stream-timeout tracking
    # ------------------------------------------------------------------
    #
    # The three hooks below (``add``, ``flush``, ``on_paused``) layer an
    # opt-in whole-stream silence detector on top of the parent
    # ``BatchingSink``. State is a single last-seen millisecond timestamp,
    # a single ``_fired`` boolean, and the stream designation (captured
    # once from the first message's ``topic`` argument). When the feature
    # is disabled each hook short-circuits after the ``super()`` call with
    # a single boolean check.
    #
    # Concurrency: ``add()`` and ``flush()`` are both called on the sink
    # thread by the quixstreams runtime; no locking on the tracker state.

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: Any,
        topic: str,
        partition: int,
        offset: int,
    ):
        """Accumulate the record into the parent batch, then (if enabled)
        stamp the whole-stream last-seen monotonic timestamp and clear any
        prior "fired" marker so the next silence period can fire again.

        The first message also captures ``topic`` as the stream designation
        passed to ``on_stream_timeout`` on fire.
        """
        super().add(value, key, timestamp, headers, topic, partition, offset)
        if not self._stream_timeout_enabled:
            return
        self._last_seen_ms = time.monotonic_ns() // 1_000_000
        self._fired = False
        if self._stream_name is None:
            self._stream_name = topic

    def flush(self):
        """Flush the parent batch, then (if enabled) check whether the
        whole-stream silence has exceeded the threshold and fire the
        callback.

        The timeout check runs *after* the parquet write so a callback can
        trust that everything up to this moment has been committed.
        """
        super().flush()
        if self._stream_timeout_enabled:
            self._check_timeouts()

    def on_paused(self):
        """Inherit the parent ``on_paused()`` behaviour — drop the pending
        batches but **do not touch** ``_last_seen_ms`` or ``_fired``.

        Backpressure means the destination rejected a batch, not that the
        messages were never seen. The timeout should still fire based on
        stream silence, independent of write success.
        """
        super().on_paused()
        # intentional no-op on tracker state

    def _check_timeouts(self) -> None:
        """If the whole-stream silence exceeds ``_stream_timeout_ms`` and
        we have not already fired for this silence period, invoke the
        callback exactly once with the stream designation.

        Callback exceptions are logged and swallowed — a misbehaving
        callback must not abort the flush or kill the sink. The
        ``finally`` branch records the fire even on exception so a
        persistently raising callback is not retried every commit.
        """
        if self._last_seen_ms is None or self._fired:
            return
        now_ms = time.monotonic_ns() // 1_000_000
        if now_ms - self._last_seen_ms >= self._stream_timeout_ms:
            designation = self._stream_name or ""
            # v5 fire banner — makes the callback argument explicit in logs so
            # a mismatch between "what the sink passes" and "what downstream
            # observes" can be diagnosed without redeploying.
            logger.info(
                "QuixTSDataLakeSink STREAM-TIMEOUT v5 FIRE: "
                "stream_name=%r (input-topic), silence_ms=%s, threshold_ms=%s",
                designation,
                now_ms - self._last_seen_ms,
                self._stream_timeout_ms,
            )
            try:
                self._on_stream_timeout(designation)
            except Exception:
                logger.exception("on_stream_timeout callback raised")
            finally:
                self._fired = True

    def setup(self):
        """Initialize blob storage client and test connection."""
        logger.info("Starting Quix Lake Blob Storage Sink...")

        # Extract bucket name from quixportal configuration
        self._s3_bucket = get_bucket_name()

        # Log storage target with workspace path if set
        storage_path = (
            f"{self.workspace_id}/{self.s3_prefix}"
            if self.workspace_id
            else self.s3_prefix
        )
        logger.info(
            f"Storage Target: s3://{self._s3_bucket}/{storage_path}/{self.table_name}"
        )
        logger.info(f"Partitioning: hive_columns={self.hive_columns}")

        if self._catalog and self.auto_discover:
            logger.info("Table will be auto-registered in REST Catalog on first write")

        try:
            # Initialize BlobStorageClient via quixportal
            # workspace_id is passed as base_path to scope all operations to the workspace
            self._blob_client = BlobStorageClient(
                base_path=self.workspace_id,
                max_workers=self._max_workers,
            )

            # Confirm storage connection
            self._ensure_bucket()

            # Test Catalog connection if configured
            if self._catalog:
                response = self._catalog.get("/health", timeout=5)
                response.raise_for_status()
                logger.info(
                    "Successfully connected to REST Catalog at %s", self._catalog
                )

            # Check if table already exists and validate partition strategy
            self._validate_existing_table_structure()

        except Exception as e:
            logger.error("Failed to setup blob storage connection: %s", e)
            raise

    def _ensure_bucket(self):
        """Ensure the blob storage path is accessible."""
        if not self._blob_client.ensure_path_exists(
            auto_create=self._auto_create_bucket
        ):
            raise RuntimeError("Failed to access blob storage")
        logger.info("Successfully connected to blob storage")

    def write(self, batch: SinkBatch):
        """Write batch directly to blob storage."""
        # Register table before first write if auto-discover is enabled
        if self.auto_discover and not self.table_registered and self._catalog:
            self._register_table()

        attempts = 3
        while attempts:
            start = time.perf_counter()
            try:
                self._write_batch(batch)
                elapsed_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    "Wrote %d rows to blob storage in %.1f ms", batch.size, elapsed_ms
                )
                return
            except Exception as exc:
                attempts -= 1
                if attempts == 0:
                    raise
                logger.warning("Write failed (%s) - retrying...", exc)
                time.sleep(3)

    def _write_batch(self, batch: SinkBatch):
        """Convert batch to Parquet and write to blob storage with Hive partitioning."""
        if not batch:
            return

        # Convert batch to list of dictionaries
        rows = []
        for item in batch:
            row = item.value.copy()
            # Add timestamp and key if not present
            # This ensures we have a timestamp column for time-based partitioning
            if self.timestamp_column not in row:
                row[self.timestamp_column] = item.timestamp
            row["__key"] = item.key
            rows.append(row)

        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(rows)

        # Add time-based partition columns (year/month/day/hour) if they're specified in hive_columns
        # These are extracted from the timestamp_column
        if self._ts_hive_columns:
            df = self._add_timestamp_columns(df)

        # Use only the explicitly specified partition columns
        if partition_columns := self.hive_columns.copy():
            # Group by partition columns and write each partition separately
            # This creates the Hive-style directory structure: col1=val1/col2=val2/file.parquet
            for group_values, group_df in df.groupby(partition_columns):
                # Ensure group_values is always a tuple for consistent handling
                if not isinstance(group_values, tuple):
                    group_values = (group_values,)

                # Build storage key with Hive partitioning (col=value format)
                partition_parts = [
                    f"{col}={val}" for col, val in zip(partition_columns, group_values)
                ]
                storage_key = (
                    f"{self.s3_prefix}/{self.table_name}/"
                    + "/".join(partition_parts)
                    + f"/data_{uuid.uuid4().hex}.parquet"
                )

                # Remove partition columns from data (Hive style - partition values are in the path, not the data)
                data_df = group_df.drop(columns=partition_columns, errors="ignore")

                # Write to blob storage
                self._write_parquet_to_storage(
                    data_df, storage_key, partition_columns, group_values
                )
        else:
            # No partitioning - write as single file directly under table directory
            storage_key = (
                f"{self.s3_prefix}/{self.table_name}/data_{uuid.uuid4().hex}.parquet"
            )
            self._write_parquet_to_storage(df, storage_key, [], ())

        # Wait for all uploads to complete and register files in catalog
        self._finalize_writes()

    def _write_parquet_to_storage(
        self,
        df: pd.DataFrame,
        storage_key: str,
        partition_columns: List[str],
        partition_values: tuple,
    ):
        """Write a DataFrame to blob storage as Parquet."""
        # Convert to Arrow table and prepare buffer
        self._null_empty_dicts(df)
        table = pa.Table.from_pandas(df)

        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)
        parquet_bytes = buf.getvalue().to_pybytes()

        # Submit async upload
        if self._blob_client is None:
            raise RuntimeError("BlobStorageClient not initialized. Call setup() first.")
        future = self._blob_client.put_object_async(storage_key, parquet_bytes)

        self._pending_futures.append(
            {
                "future": future,
                "key": storage_key,
                "row_count": len(df),
                "file_size": len(parquet_bytes),
                "partition_columns": partition_columns,
                "partition_values": partition_values,
            }
        )

    def _finalize_writes(self):
        """Wait for all pending uploads to complete and register files in catalog."""
        if not self._pending_futures:
            return

        count = len(self._pending_futures)
        logger.debug(f"Waiting for {count} upload(s) to complete...")

        try:
            # Wait for all uploads to complete, collecting the first error
            first_error = None
            for item in self._pending_futures:
                try:
                    item["future"].result()
                    logger.debug(
                        "Uploaded %d rows to %s", item["row_count"], item["key"]
                    )
                except Exception as e:
                    logger.error("Failed to upload %s: %s", item["key"], e)
                    if first_error is None:
                        first_error = e

            if first_error is not None:
                raise first_error

            logger.info(f"Successfully uploaded {count} file(s)")

            # Register all files in catalog manifest if configured
            if self._catalog and self.table_registered:
                self._register_files_in_manifest()
        finally:
            self._pending_futures.clear()

    def _null_empty_dicts(self, df: pd.DataFrame):
        """
        Convert empty dictionaries to null values before writing to Parquet.

        Parquet format has limitations with empty maps/structs - they cannot be written
        properly and will cause serialization errors. This method scans all columns
        that contain dictionaries and replaces empty dicts ({}) with None/null values.

        This is done in-place to avoid copying the DataFrame.
        """
        for col in df.columns:
            # Check if column contains any dictionary values
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                # Replace empty dicts with None; keeps non-empty dicts as-is
                df[col] = df[col].apply(lambda x: x or None)

    def _register_table(self):
        """Register the table in REST Catalog."""
        if not self._catalog:
            return

        # First check if table already exists
        check_response = self._catalog.get(
            f"/namespaces/{self.namespace}/tables/{self.table_name}",
            timeout=5,
        )

        if check_response.status_code == 200:
            logger.info("Table '%s' already exists in catalog", self.table_name)
            self.table_registered = True
            # Validate partition strategy matches
            self._validate_partition_strategy(check_response.json())
            return

        # Table doesn't exist, create it
        # Note: Location must be full S3 URI for catalog (API uses this with DuckDB)
        # Include workspace_id in the path if set (for workspace-scoped storage)
        if self.workspace_id:
            location = f"s3://{self.s3_bucket}/{self.workspace_id}/{self.s3_prefix}/{self.table_name}"
        else:
            location = f"s3://{self.s3_bucket}/{self.s3_prefix}/{self.table_name}"

        # Define partition spec based on configuration
        # For dynamic partition discovery, create table without partition spec
        # The partition spec will be set when first files are added
        partition_spec = []  # Empty spec for dynamic discovery

        # Create table with minimal schema (will be inferred from data)
        create_response = self._catalog.put(
            f"/namespaces/{self.namespace}/tables/{self.table_name}",
            json={
                "location": location,
                "partition_spec": partition_spec,
                "properties": {
                    "created_by": "quixstreams-quix-lake-sink",
                    "auto_discovered": "false",
                    "expected_partitions": self.hive_columns.copy(),
                },
            },
            timeout=30,
        )

        if create_response.status_code in [200, 201]:
            logger.info(
                "Successfully created table '%s' in REST Catalog. Partitions will be set dynamically to: %s",
                self.table_name,
                self.hive_columns,
            )
            self.table_registered = True
        else:
            raise RuntimeError(
                f"Failed to create table '{self.table_name}' in REST Catalog: "
                f"{create_response.status_code} {create_response.text}"
            )

    def _add_timestamp_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add timestamp-based columns (year/month/day/hour) for time-based partitioning.

        This method extracts time components from the timestamp column and adds them
        as separate columns that can be used for Hive partitioning.
        """
        # Convert to datetime if needed (handles numeric timestamps)
        if not pd.api.types.is_datetime64_any_dtype(df[self.timestamp_column]):
            sample_value = float(
                df[self.timestamp_column].iloc[0]
                if not df[self.timestamp_column].empty
                else 0
            )

            # Auto-detect timestamp unit by inspecting the magnitude of the value
            # Typical timestamp ranges:
            # - Seconds: ~1.7e9 (since epoch 1970)
            # - Milliseconds: ~1.7e12
            # - Microseconds: ~1.7e15
            # - Nanoseconds: ~1.7e18
            if sample_value > 1e17:
                # Nanoseconds (Java/Kafka timestamps)
                df[self.timestamp_column] = pd.to_datetime(
                    df[self.timestamp_column], unit="ns"
                )
            elif sample_value > 1e14:
                # Microseconds
                df[self.timestamp_column] = pd.to_datetime(
                    df[self.timestamp_column], unit="us"
                )
            elif sample_value > 1e11:
                # Milliseconds (common in JavaScript/Kafka)
                df[self.timestamp_column] = pd.to_datetime(
                    df[self.timestamp_column], unit="ms"
                )
            else:
                # Seconds (Unix timestamp)
                df[self.timestamp_column] = pd.to_datetime(
                    df[self.timestamp_column], unit="s"
                )

        # Extract time-based columns (year, month, day, hour) from the timestamp
        timestamp_col = df[self.timestamp_column]

        # Only add columns that are specified in _ts_hive_columns
        # TIMESTAMP_COL_MAPPER handles proper formatting (e.g., zero-padding for months/days)
        for col in self._ts_hive_columns:
            df[col] = TIMESTAMP_COL_MAPPER[col](timestamp_col)

        return df

    def _validate_partition_strategy(self, table_metadata: Dict[str, Any]):
        """Validate that the sink's partition strategy matches the existing table."""
        existing_partition_spec = table_metadata.get("partition_spec", [])

        # Build expected partition spec from sink configuration
        expected_partition_spec = self.hive_columns.copy()

        # Special case: If table has no partition spec yet (empty list),
        # it will be set when first files are added
        if not existing_partition_spec:
            logger.info(
                "Table '%s' has no partition spec yet. Will be set to %s on first write.",
                self.table_name,
                expected_partition_spec,
            )
            return

        # Check if partition strategies match
        if set(existing_partition_spec) != set(expected_partition_spec):
            error_msg = (
                f"Partition strategy mismatch for table '{self.table_name}'. "
                f"Existing table has partitions: {existing_partition_spec}, "
                f"but sink is configured with: {expected_partition_spec}. "
                "This would corrupt the folder structure. Please ensure the sink partition "
                "configuration matches the existing table."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Also check the order of partitions
        if existing_partition_spec != expected_partition_spec:
            warning_msg = (
                f"Partition column order differs for table '{self.table_name}'. "
                f"Existing: {existing_partition_spec}, Configured: {expected_partition_spec}. "
                "While this won't corrupt data, it may lead to suboptimal query performance."
            )
            logger.warning(warning_msg)

    def _validate_existing_table_structure(self):
        """
        Check if table already exists in storage and validate partition structure.

        This prevents data corruption by ensuring that if a table already exists,
        the sink's partition configuration matches what's already on disk.
        """
        table_prefix = f"{self.s3_prefix}/{self.table_name}/"

        # List objects to see if table exists (sample first 100 files)
        objects = self._blob_client.list_objects(prefix=table_prefix, max_keys=100)

        if not objects:
            # Table doesn't exist yet, no validation needed
            return

        # Detect existing partition columns from directory structure
        # We parse the paths to extract partition columns from Hive-style paths
        detected_partition_columns = []
        for obj in objects:
            key = obj["Key"]
            if key.endswith(".parquet"):
                # Extract path after table prefix
                relative_path = (
                    key[len(table_prefix) :] if key.startswith(table_prefix) else key
                )
                path_parts = relative_path.split("/")

                # Look for Hive-style partitions (col=value format)
                for part in path_parts[:-1]:  # Exclude filename
                    if "=" in part:
                        # Extract column name from "col=value"
                        col_name = part.split("=")[0]
                        # Maintain order of first appearance
                        if col_name not in detected_partition_columns:
                            detected_partition_columns.append(col_name)

        if detected_partition_columns:
            # Build expected partition spec from sink configuration
            expected_partition_spec = self.hive_columns.copy()

            # Check if partition strategies match
            # Using set comparison to ignore order first
            if set(detected_partition_columns) != set(expected_partition_spec):
                error_msg = (
                    f"Partition strategy mismatch for table '{self.table_name}'. "
                    f"Existing table in storage has partitions: {detected_partition_columns}, "
                    f"but sink is configured with: {expected_partition_spec}. "
                    "This would corrupt the folder structure. Please ensure the sink partition "
                    "configuration matches the existing table."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            logger.info(
                "Validated partition strategy for existing table '%s'. Partitions: %s",
                self.table_name,
                detected_partition_columns,
            )

    def _register_files_in_manifest(self):
        """Register multiple newly written files in the catalog manifest."""
        if not (file_items := self._pending_futures):
            return

        # Build file entries for all files
        file_entries = []
        for item in file_items:
            storage_key = item["key"]
            row_count = item["row_count"]
            file_size = item["file_size"]
            partition_columns = item["partition_columns"]
            partition_values = item["partition_values"]

            # Build file path as full S3 URI for catalog (API uses this with DuckDB)
            # Include workspace_id if set (for workspace-scoped storage)
            if self.workspace_id:
                file_path = f"s3://{self.s3_bucket}/{self.workspace_id}/{storage_key}"
            else:
                file_path = f"s3://{self.s3_bucket}/{storage_key}"

            # Build partition values dict
            partition_dict = {}
            if partition_columns and partition_values:
                for col, val in zip(partition_columns, partition_values):
                    partition_dict[col] = str(val)

            # Create file entry
            file_entries.append(
                {
                    "file_path": file_path,
                    "file_size": file_size,
                    "last_modified": datetime.now(tz=timezone.utc).isoformat(),
                    "partition_values": partition_dict,
                    "row_count": row_count,
                }
            )

        # Send all files to catalog in a single request
        response = self._catalog.post(
            f"/namespaces/{self.namespace}/tables/{self.table_name}/manifest/add-files",
            json={"files": file_entries},
            timeout=10,
        )

        if response.status_code == 200:
            logger.info(f"Registered {len(file_entries)} file(s) in catalog manifest")
        else:
            raise RuntimeError(
                f"Failed to register files in catalog manifest: "
                f"{response.status_code} {response.text}"
            )

    def cleanup(self):
        """Cleanup resources when sink is stopped."""
        if self._blob_client:
            self._blob_client.shutdown()
