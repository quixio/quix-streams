"""
Quix Lake Blob Storage Sink

This module provides a sink that writes Kafka batches to blob storage as
Hive-partitioned Parquet files, with optional REST Catalog integration.

Uses quixportal for unified blob storage access (Azure, AWS S3, GCP, MinIO, local).
"""

import logging
import math
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.compute as pc
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
from .stream_timeout_tracker import StreamTimeoutTracker

logger = logging.getLogger(__name__)


# Timestamp column mappers for Hive partitioning
TIMESTAMP_COL_MAPPER = {
    "year": lambda col: col.dt.year.astype(str),
    "month": lambda col: col.dt.month.astype(str).str.zfill(2),
    "day": lambda col: col.dt.day.astype(str).str.zfill(2),
    "hour": lambda col: col.dt.hour.astype(str).str.zfill(2),
}

# On-disk path segment used when a partition column value is NULL.
# Unified with the reading side (quix-ts-datalake catalog + API + UI):
# every writer in the stack emits this exact string, every reader maps
# it back to SQL NULL. Double-underscore prefix/suffix keeps the
# sentinel from colliding with real user values like the bare string
# ``"None"`` or Spark's ``__HIVE_DEFAULT_PARTITION__``, both of which
# the readers also accept for backward compatibility on existing data.
HIVE_NULL_PARTITION = "__None__"

# Loggers that emit one INFO record per HTTP round-trip — useful for low-
# level SDK debugging, but pure noise for a sink that performs hundreds of
# blob operations per minute (the Hive partition tree probe alone). The
# sink mutes them by default; pass `silence_azure_http_logs=False` to
# preserve them.
_CHATTY_HTTP_LOGGERS = (
    "azure",
    "azure.core",
    "azure.core.pipeline.policies.http_logging_policy",
    "azure.storage",
    "adlfs",
    "botocore",
    "boto3",
    "s3transfer",
)


def silence_chatty_loggers() -> None:
    """Mute per-request HTTP logging from the cloud-storage SDKs used by
    this sink (Azure SDK + adlfs, botocore/boto3 + s3transfer).

    Safe to call from application code at any point. Levels are raised to
    WARNING, so anything actually noteworthy (auth failures, retries,
    throttling, server errors) still propagates. Call after configuring
    your own logging (e.g. after instantiating quixstreams.Application)
    so the framework's logging setup does not reset these levels.
    """
    for name in _CHATTY_HTTP_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)


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
        'year', 'month', 'day', 'hour' to extract these from timestamp_column.
        Prefix an entry with ``~`` to make it a VIRTUAL partition: it appears in
        the partition tree and is filterable, but is NOT written as a physical
        ``key=value/`` folder and does not split files (a file keeps every value
        of it). E.g. ``["year", "month", "~driver"]`` folders by year/month and
        exposes ``driver`` as a virtual level. Virtual columns stay in the
        parquet data so queries can still filter rows by them.
    :param timestamp_column: Column containing timestamp to extract time partitions from
    :param sort_column: Optional column recorded on the table (properties.sort_column)
        that compaction orders files by, so ORDER BY / time-range queries can skip
        files and stream. When None, the lakehouse falls back to timestamp_column.
    :param catalog_url: Optional REST Catalog URL for table registration
    :param catalog_auth_token: If using REST Catalog, the respective auth token for it
    :param auto_discover: Whether to auto-register table on first write
    :param namespace: Catalog namespace (default: "default")
    :param auto_create_bucket: If True, attempt to create bucket/path in storage if missing
    :param max_workers: Maximum number of parallel upload threads (default: 10)
    :param stats_columns: Optional list of column names to compute per-file
        min/max statistics ("zone maps") for. These are sent to the REST
        Catalog with each file and let the query layer skip files whose value
        range cannot satisfy a WHERE/ORDER BY on the column. ``None`` (default)
        computes stats for every numeric and timestamp column in each written
        file (cheap — the batch is already in memory). Pass an explicit list to
        restrict the set and bound catalog storage on very wide tables. String
        columns are not supported here — use ``auto_index_max_cardinality`` for
        those.
    :param auto_index_max_cardinality: Auto-index low-cardinality non-numeric
        columns (strings/bools/categoricals — the ones ``stats_columns`` can't
        cover) so ``WHERE col = 'x'`` prunes files without marking the column
        virtual (``~``). For each written file, any such column whose distinct
        count in that file is <= this value has its distinct values recorded in
        the catalog's file_virtual_values index; a column over the cap is skipped
        for that file only (safe — the file is kept and rows are filtered at read
        time). Default 100; set 0 to disable. Only worthwhile for CLUSTERED
        columns (each file holds a small, distinct subset) — an un-clustered
        column costs storage for no pruning, so keep the cap modest on wide tables.
    :param stream_timeout_ms: Optional **per-key** silence threshold in
        milliseconds. Paired with ``on_stream_timeout``; both must be
        provided to enable the feature. See
        :class:`quixstreams.sinks.core.stream_timeout_tracker.StreamTimeoutTracker`
        for the full behavioural contract (per-key tracking, fire-and-evict
        semantics, re-arm on next record, 3x TTL safety sweep,
        background check cadence, and zero-overhead disabled path).
    :param on_stream_timeout: Optional callback
        ``Callable[[str], None]`` invoked once per silence period per
        Kafka message key. See ``stream_timeout_ms`` above.
    :param silence_azure_http_logs: If True (default), raise the log levels of
        the Azure SDK / adlfs / botocore HTTP-logging loggers to WARNING during
        setup(). These libraries log one INFO record per HTTP round-trip with
        the full URL and headers, which buries the sink's own logs under
        hundreds of lines per minute of partition probing. Set to False to
        keep the verbose request/response logs (useful for low-level SDK
        debugging).
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
        sort_column: Optional[str] = None,
        catalog_url: Optional[str] = None,
        catalog_auth_token: Optional[str] = None,
        auto_discover: bool = True,
        namespace: str = "default",
        auto_create_bucket: bool = True,
        max_workers: int = 10,
        stats_columns: Optional[List[str]] = None,
        auto_index_max_cardinality: int = 100,
        stream_timeout_ms: Optional[int] = None,
        on_stream_timeout: Optional[Callable[[Any], None]] = None,
        silence_azure_http_logs: bool = True,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        _check_interval_ms: Optional[int] = None,
    ):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self.s3_prefix = s3_prefix
        self.table_name = table_name
        self.workspace_id = workspace_id
        # A ``~``-prefixed entry in hive_columns marks a VIRTUAL partition: it
        # appears in the partition tree and is filterable, but is NOT written as
        # a physical ``key=value/`` folder and does NOT group/split files (a
        # single file keeps every value of it). We split the incoming list into:
        #   * self.hive_columns          — physical columns (grouped + foldered)
        #   * self._virtual_columns      — virtual columns (indexed, kept in data)
        #   * self._partition_spec_order — full tree order (names, no prefix)
        _raw_hive = hive_columns or []
        self._virtual_columns = [c[1:] for c in _raw_hive if c.startswith("~")]
        self.hive_columns = [c for c in _raw_hive if not c.startswith("~")]
        self._partition_spec_order = [
            c[1:] if c.startswith("~") else c for c in _raw_hive
        ]
        self.timestamp_column = timestamp_column
        # Preferred ordering column recorded on the table (properties.sort_column).
        # Compaction writes files ordered by it so ORDER BY / range queries can
        # skip files and stream. When None, the lakehouse falls back to the
        # timestamp column automatically.
        self.sort_column = sort_column or None
        self._catalog = (
            QuixTSDataLakeCatalogClient(catalog_url, catalog_auth_token)
            if catalog_url
            else None
        )
        self.auto_discover = auto_discover
        self.namespace = namespace
        self.table_registered = False

        # Columns to compute per-file min/max zone maps for (data-skipping at
        # query time). ``None`` (default) -> every numeric / timestamp column
        # in each written file, which is nearly free here because the batch is
        # already an in-memory Arrow table. Pass an explicit list to restrict
        # the set (e.g. just the timestamp column) and bound catalog stats-row
        # growth on very wide tables. Stats ride along with each add-files
        # entry as ``column_stats`` and are consumed by the catalog's pruning.
        self._stats_columns = set(stats_columns) if stats_columns else None

        # Auto-indexing of low-cardinality NON-numeric columns (strings, bools,
        # categoricals) that column_stats can't prune. For each written file we
        # record the distinct values of every such column whose per-file distinct
        # count is <= this cap into the catalog's file_virtual_values index, so
        # ``WHERE col = 'x'`` prunes files even without marking the column
        # virtual (``~``). PER-COLUMN + PER-FILE: a column over the cap in a
        # given file is simply skipped for that file (never taints other columns
        # or other files). 0 disables. Numeric/timestamp columns are excluded —
        # they already prune via column_stats. NOTE: only pays off for CLUSTERED
        # columns (each file holds a small, distinct subset); an un-clustered
        # column costs storage for no pruning, so keep the cap modest and prefer
        # an explicit set on very wide tables.
        self._auto_index_max_cardinality = max(0, int(auto_index_max_cardinality))

        # Blob storage client and bucket name will be initialized in setup()
        self._blob_client: Optional[BlobStorageClient] = None
        self._s3_bucket: Optional[str] = None
        self._ts_hive_columns = {"year", "month", "day", "hour"} & set(
            self.hive_columns
        )
        self._auto_create_bucket = auto_create_bucket
        self._max_workers = max_workers
        self._silence_azure_http_logs = silence_azure_http_logs

        # Batch upload tracking
        self._pending_futures: List[Dict[str, Any]] = []

        # Stream-timeout tracking (opt-in, per-key silence detector).
        # All state, threading, and validation live inside the
        # StreamTimeoutTracker — the sink composes it and exposes it
        # via integration hooks in add/flush/setup/cleanup below. See
        # :mod:`quixstreams.sinks.core.stream_timeout_tracker` for the
        # behavioural contract. Disabled pair -> tracker is allocated
        # but ``tracker.enabled`` is False and every method is a
        # zero-overhead no-op.
        self._timeout = StreamTimeoutTracker(
            stream_timeout_ms=stream_timeout_ms,
            on_stream_timeout=on_stream_timeout,
            check_interval_ms=_check_interval_ms,
            thread_name="QuixTSDataLakeSink-timeout-check",
            logger=logger,
        )

    @property
    def s3_bucket(self) -> str:
        """Get the S3 bucket name (extracted from quixportal config)."""
        if self._s3_bucket is None:
            raise RuntimeError("s3_bucket not initialized. Call setup() first.")
        return self._s3_bucket

    # ------------------------------------------------------------------
    # Stream-timeout integration
    # ------------------------------------------------------------------
    #
    # All behaviour lives in ``self._timeout``
    # (:class:`quixstreams.sinks.core.stream_timeout_tracker.StreamTimeoutTracker`).
    # The three hooks below wire the sink lifecycle into the tracker:
    # ``add`` -> ``touch``, ``flush`` -> ``check_now``,
    # ``setup`` -> ``start``, ``cleanup`` -> ``stop``. ``on_paused`` is
    # intentionally a no-op on tracker state (backpressure means the
    # destination rejected a batch, not that the messages were never
    # seen; per-key silence timers continue from their last-seen
    # stamp regardless of write success).

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
        """Accumulate the record, then refresh the per-key last-seen
        stamp via the tracker.
        """
        super().add(value, key, timestamp, headers, topic, partition, offset)
        self._timeout.touch(key, topic=topic, partition=partition, offset=offset)

    def flush(self):
        """Flush the parent batch, then run a timeout check."""
        super().flush()
        self._timeout.check_now()

    def on_paused(self):
        """Inherit parent ``on_paused()`` — do **not** touch tracker state."""
        super().on_paused()
        # intentional no-op on tracker state

    def setup(self):
        """Initialize blob storage client and test connection."""
        logger.info("Starting Quix Lake Blob Storage Sink...")

        # Done in setup() rather than __init__ so it runs after the host
        # application (typically quixstreams.Application) has configured
        # logging; otherwise the framework's setup would reset these levels
        # back to whatever the global log level is.
        if self._silence_azure_http_logs:
            silence_chatty_loggers()

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

        # Start the background timeout-check thread AFTER the blob
        # client is healthy, so a blob-setup failure tears down cleanly
        # without leaving an orphan timer thread running.
        self._timeout.start()

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
                rows_written = self._write_batch(batch)
                elapsed_ms = (time.perf_counter() - start) * 1000
                # Log the actually-written count, not batch.size. They are
                # equal in normal operation, but reporting the real number
                # makes any future silent-drop regression visible in the log.
                logger.info(
                    "Wrote %d rows to blob storage in %.1f ms",
                    rows_written,
                    elapsed_ms,
                )
                return
            except Exception as exc:
                attempts -= 1
                if attempts == 0:
                    raise
                logger.warning("Write failed (%s) - retrying...", exc)
                time.sleep(3)

    def _write_batch(self, batch: SinkBatch) -> int:
        """Convert batch to Parquet and write to blob storage with Hive partitioning.

        Returns the number of rows actually grouped and written to storage.
        Equals batch.size in normal operation; reported by write() so any
        future silent-drop regression is visible in the log instead of
        being papered over with the input count.
        """
        if not batch:
            return 0

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
            # Rows where a partition column is NaN/None would be silently
            # discarded by pandas.groupby's default dropna=True — the for-
            # loop below would simply not iterate over them and they would
            # never reach storage, even though write() would still report
            # success using batch.size. Route them into a single Hive-NULL
            # bucket instead so the data lands somewhere queryable.
            for col in partition_columns:
                if col in df.columns:
                    df[col] = df[col].fillna(HIVE_NULL_PARTITION)

            # Group by partition columns and write each partition separately
            # This creates the Hive-style directory structure: col1=val1/col2=val2/file.parquet
            rows_written = 0
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
                rows_written += len(group_df)
        else:
            # No partitioning - write as single file directly under table directory
            storage_key = (
                f"{self.s3_prefix}/{self.table_name}/data_{uuid.uuid4().hex}.parquet"
            )
            self._write_parquet_to_storage(df, storage_key, [], ())
            rows_written = len(df)

        # Wait for all uploads to complete and register files in catalog
        self._finalize_writes()
        return rows_written

    @staticmethod
    def _safe_float_min(value: Any) -> float:
        """Largest float <= value. Widening the low bound downward guarantees
        the stored zone map is a *superset* of the real range, so float
        rounding of large ints/decimals (e.g. nanosecond epochs beyond 2**53)
        can only cost pruning, never wrongly skip a matching file."""
        f = float(value)
        while f > value:
            f = math.nextafter(f, float("-inf"))
        return f

    @staticmethod
    def _safe_float_max(value: Any) -> float:
        """Smallest float >= value (see _safe_float_min for the safety rationale)."""
        f = float(value)
        while f < value:
            f = math.nextafter(f, float("inf"))
        return f

    def _compute_column_stats(self, table: "pa.Table") -> Dict[str, Dict[str, Any]]:
        """Compute per-column min/max/null_count for a written Arrow table.

        Numeric columns (int/float/decimal) are reported as ``type="numeric"``
        with float bounds (floored min / ceiled max for safety); timestamp and
        date columns as ``type="timestamp"`` with ISO-8601 bounds. Everything
        else (strings, structs, the ``__key`` column) is skipped. Respects
        ``self._stats_columns`` when set. Runs on the in-memory batch, so it is
        just vectorised min/max — no re-read of the parquet footer.
        """
        stats: Dict[str, Dict[str, Any]] = {}
        for field in table.schema:
            name = field.name
            if name == "__key":
                continue
            if self._stats_columns is not None and name not in self._stats_columns:
                continue

            t = field.type
            if pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t):
                vtype = "numeric"
            elif pa.types.is_timestamp(t) or pa.types.is_date(t):
                vtype = "timestamp"
            else:
                continue

            column = table.column(name)
            null_count = column.null_count
            if len(column) - null_count == 0:
                # All-null column: no usable bound, skip (keeps files unpruned).
                continue

            try:
                mm = pc.min_max(column)
                vmin = mm["min"].as_py()
                vmax = mm["max"].as_py()
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Skipping stats for column %s: %s", name, exc)
                continue
            if vmin is None or vmax is None:
                continue

            if vtype == "numeric":
                vmin = self._safe_float_min(vmin)
                vmax = self._safe_float_max(vmax)
            else:  # timestamp / date
                vmin = vmin.isoformat()
                vmax = vmax.isoformat()

            stats[name] = {
                "type": vtype,
                "min": vmin,
                "max": vmax,
                "null_count": int(null_count),
                "value_count": int(len(column) - null_count),
            }
        return stats

    def _compute_partition_combinations(
        self,
        df: pd.DataFrame,
        partition_columns: List[str],
        partition_values: tuple,
        max_distinct: int = 50000,
    ) -> List[Dict[str, str]]:
        """Distinct full partition tuples (physical + virtual) in this file, for
        the catalog's ``table_partition_combinations`` dictionary.

        Records which virtual values actually CO-OCCUR: the file's physical
        partition values (constant per file) combined with each distinct
        virtual-value tuple (``df[virtual].drop_duplicates()``). The catalog
        stores the DISTINCT set across files (table_partition_combinations — the
        sole virtual index), so the tree shows only real combinations and
        multi-column queries prune. Skipped for a file with more than
        ``max_distinct`` distinct tuples (a high-cardinality guard).
        """
        if not self._virtual_columns:
            return []
        present = [c for c in self._virtual_columns if c in df.columns]
        if not present:
            return []
        physical: Dict[str, str] = {}
        for col, val in zip(partition_columns or [], partition_values or ()):
            if val is not None:
                physical[str(col)] = str(val)
        try:
            distinct = df[present].drop_duplicates()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Skipping partition combinations: %s", exc)
            return []
        if len(distinct) > max_distinct:
            logger.warning(
                "File has %d distinct virtual combinations (> %d) — skipping the "
                "combination index for this file.",
                len(distinct), max_distinct,
            )
            return []
        combos: List[Dict[str, str]] = []
        for row in distinct.itertuples(index=False):
            combo = dict(physical)
            for col, v in zip(present, row):
                if v is None or (isinstance(v, float) and v != v):  # NULL / NaN
                    continue
                combo[str(col)] = str(v)
            combos.append(combo)
        return combos

    def _compute_virtual_values(
        self, df: pd.DataFrame, max_distinct: int = 50000
    ) -> Dict[str, List[str]]:
        """Distinct values of each virtual column present in THIS file, for the
        catalog's per-file virtual index (``file_virtual_values``) that powers
        query PRUNING at file grain. Returns ``{col: [distinct values]}``.

        Returned ONLY when EVERY virtual column is fully covered (present in the
        file and within ``max_distinct``). The catalog marks a file
        ``virtual_indexed`` with a single flag (not per-column), so a partially
        covered file must stay un-indexed — otherwise a query on the missing
        column would wrongly prune it. When we return ``{}`` the file is left
        un-indexed (safety: never pruned; DuckDB still row-filters).
        """
        if not self._virtual_columns:
            return {}
        out: Dict[str, List[str]] = {}
        for col in self._virtual_columns:
            if col not in df.columns:
                return {}   # not all virtual columns present -> don't index
            try:
                vals = df[col].dropna().unique()
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Skipping virtual values (col %s): %s", col, exc)
                return {}
            if len(vals) > max_distinct:
                logger.warning(
                    "Virtual column %s has %d distinct values (> %d) in a file — "
                    "leaving the file un-indexed (safety).",
                    col, len(vals), max_distinct,
                )
                return {}
            out[str(col)] = [
                str(v) for v in vals
                if v is not None and not (isinstance(v, float) and v != v)
            ]
        return out

    def _compute_auto_index_values(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Per-file distinct values of low-cardinality NON-numeric columns for
        the catalog's file_virtual_values PRUNING index — the "auto-index" lane.

        Covers exactly the columns column_stats can't (strings, bools,
        categoricals); numeric/timestamp are skipped (they prune via min/max).
        PER-COLUMN, PER-FILE: each eligible column is included independently iff
        its distinct count in THIS file is within
        ``self._auto_index_max_cardinality``; an over-cap column is skipped for
        this file only (safe — the catalog keeps such files and DuckDB
        row-filters). Virtual (``~``) and physical partition columns are excluded
        (handled elsewhere / not in the data). Returns ``{col: [values]}`` or {}.
        """
        cap = self._auto_index_max_cardinality
        if cap <= 0:
            return {}
        skip = set(self._virtual_columns) | set(self.hive_columns) | {"__key", self.timestamp_column}
        out: Dict[str, List[str]] = {}
        for col in df.columns:
            if col in skip:
                continue
            s = df[col]
            # Only column kinds column_stats does NOT already cover.
            is_indexable = (
                pd.api.types.is_string_dtype(s)
                or pd.api.types.is_object_dtype(s)
                or pd.api.types.is_bool_dtype(s)
                or isinstance(s.dtype, pd.CategoricalDtype)
            )
            if not is_indexable:
                continue
            try:
                vals = s.dropna().unique()
            except Exception as exc:  # pragma: no cover - defensive (unhashable)
                logger.debug("Skipping auto-index (col %s): %s", col, exc)
                continue
            if len(vals) == 0 or len(vals) > cap:
                continue  # nothing to index / over cap -> skip THIS column for THIS file
            # Skip complex/object values (dicts, lists) — only scalar categoricals.
            if any(isinstance(v, (dict, list, set, tuple)) for v in vals):
                continue
            out[str(col)] = sorted({str(v) for v in vals})
        return out

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

        # Compute per-column min/max zone maps from the in-memory table BEFORE
        # serialising — nearly free, and avoids re-reading the parquet footer
        # from storage later. Carried through _pending_futures into the catalog
        # add-files call (see _register_files_in_manifest).
        column_stats = self._compute_column_stats(table)
        # Distinct full partition tuples (physical + virtual) that CO-OCCUR in
        # this file, for the catalog's combination dictionary — the sole virtual
        # index (exact tree + query pruning). Deduplicated across the batch in
        # _register_files_in_manifest.
        partition_combinations = self._compute_partition_combinations(
            df, partition_columns, partition_values
        )
        # Per-file distinct values of each virtual column -> the catalog's
        # file_virtual_values index (file-grain query pruning). Empty unless all
        # virtual columns are covered (see _compute_virtual_values).
        virtual_values = self._compute_virtual_values(df)
        # Auto-index lane: per-file distinct values of low-cardinality
        # non-numeric columns (not marked virtual) so their equality filters also
        # prune. Per-column/per-file, independent of the virtual all-or-nothing.
        indexed_values = self._compute_auto_index_values(df)

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
                "column_stats": column_stats,
                "partition_combinations": partition_combinations,
                "virtual_values": virtual_values,
                "indexed_values": indexed_values,
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

        # Physical-only tables keep the historical dynamic-discovery behaviour
        # (empty spec; the catalog derives it from the first files' paths). When
        # any VIRTUAL column is configured it can't be discovered from paths, so
        # we send the full intended tree order up front and declare which
        # entries are virtual in properties.
        properties = {
            "created_by": "quixstreams-quix-lake-sink",
            "auto_discovered": "false",
            "expected_partitions": self._partition_spec_order.copy(),
        }
        if self._virtual_columns:
            partition_spec = self._partition_spec_order.copy()
            properties["virtual_partitions"] = self._virtual_columns.copy()
        else:
            partition_spec = []  # Empty spec for dynamic discovery

        # Record the ordering columns so lakehouse compaction can write
        # time-ordered, skippable files. sort_column (when set) takes precedence;
        # timestamp_column is the automatic fallback, so persist it too.
        if self.timestamp_column:
            properties["timestamp_column"] = self.timestamp_column
        if self.sort_column:
            properties["sort_column"] = self.sort_column

        # Create table with minimal schema (will be inferred from data)
        create_response = self._catalog.put(
            f"/namespaces/{self.namespace}/tables/{self.table_name}",
            json={
                "location": location,
                "partition_spec": partition_spec,
                "properties": properties,
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
        as separate columns that can be used for Hive partitioning. The source
        ``timestamp_column`` is **not** mutated — derivation happens on a local
        datetime64 series. Preserving its dtype is part of the sink's contract
        with readers (in particular, ``ts_ms`` is the system-injected Kafka
        timestamp and must always land in parquet as int64 ms, regardless of
        whether time-based hive partitioning is configured).
        """
        # Build a datetime64 view of the timestamp column to extract from.
        # Never write this back into ``df`` — that would change the dtype of
        # the source column in the parquet output.
        timestamp_col = df[self.timestamp_column]
        if not pd.api.types.is_datetime64_any_dtype(timestamp_col):
            sample_value = float(
                timestamp_col.iloc[0] if not timestamp_col.empty else 0
            )

            # Auto-detect timestamp unit by inspecting the magnitude of the value
            # Typical timestamp ranges:
            # - Seconds: ~1.7e9 (since epoch 1970)
            # - Milliseconds: ~1.7e12
            # - Microseconds: ~1.7e15
            # - Nanoseconds: ~1.7e18
            if sample_value > 1e17:
                unit = "ns"  # Nanoseconds (Java/Kafka timestamps)
            elif sample_value > 1e14:
                unit = "us"  # Microseconds
            elif sample_value > 1e11:
                unit = "ms"  # Milliseconds (common in JavaScript/Kafka)
            else:
                unit = "s"  # Seconds (Unix timestamp)
            timestamp_col = pd.to_datetime(timestamp_col, unit=unit)

        # Only add columns that are specified in _ts_hive_columns
        # TIMESTAMP_COL_MAPPER handles proper formatting (e.g., zero-padding for months/days)
        for col in self._ts_hive_columns:
            df[col] = TIMESTAMP_COL_MAPPER[col](timestamp_col)

        return df

    def _validate_partition_strategy(self, table_metadata: Dict[str, Any]):
        """Validate that the sink's partition strategy matches the existing table."""
        existing_partition_spec = table_metadata.get("partition_spec", [])

        # Build expected partition spec from sink configuration. Use the FULL tree
        # order (physical + virtual, `~` stripped) — that's what the sink registers
        # for a table with virtual columns (partition_spec = physical + virtual).
        # Comparing against hive_columns (physical only) here would see the virtual
        # columns as a spurious mismatch and wrongly reject the sink on RESTART to
        # an existing table.
        expected_partition_spec = self._partition_spec_order.copy()

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
            column_stats = item.get("column_stats") or {}

            # Build file path as full S3 URI for catalog (API uses this with DuckDB)
            # Include workspace_id if set (for workspace-scoped storage)
            if self.workspace_id:
                file_path = f"s3://{self.s3_bucket}/{self.workspace_id}/{storage_key}"
            else:
                file_path = f"s3://{self.s3_bucket}/{storage_key}"

            # Build partition values dict.
            # _write_batch fillna()'s NaN partition values with HIVE_NULL_PARTITION
            # (the on-disk sentinel — see the constant near the top) so they
            # survive groupby and land in a single ``col=__None__`` directory on
            # disk. The catalog receives the same literal string — including
            # the ``__None__`` sentinel for NULL buckets — so the manifest
            # row, the on-disk path, and what DuckDB's
            # ``hive_partitioning=true`` exposes at query time all agree.
            # Equality filters then resolve end-to-end without any sentinel
            # translation in the lake.
            partition_dict: Dict[str, str] = {}
            if partition_columns and partition_values:
                for col, val in zip(partition_columns, partition_values):
                    partition_dict[col] = str(val)

            # Create file entry
            entry = {
                "file_path": file_path,
                "file_size": file_size,
                "last_modified": datetime.now(tz=timezone.utc).isoformat(),
                "partition_values": partition_dict,
                "row_count": row_count,
            }
            # Attach per-column zone maps when computed (catalog stores them in
            # column_stats for query-time file pruning). Omit the key entirely
            # when empty so older catalogs simply ignore the absent field.
            if column_stats:
                entry["column_stats"] = column_stats
            # Per-file virtual values -> file_virtual_values (file-grain pruning).
            # Present only when all virtual columns are covered; the catalog marks
            # the file virtual_indexed and prunes on it. Omitted otherwise (file
            # stays un-indexed -> safety-kept).
            virtual_values = item.get("virtual_values") or {}
            if virtual_values:
                entry["virtual_values"] = virtual_values
            # Auto-index lane -> also file_virtual_values, but PER-COLUMN
            # (prune-only, not tree). Independent of virtual_values, so a file
            # can carry these even when it isn't fully virtual-indexed.
            indexed_values = item.get("indexed_values") or {}
            if indexed_values:
                entry["indexed_values"] = indexed_values
            file_entries.append(entry)

        # Aggregate the DISTINCT full partition tuples across this batch for the
        # catalog's combination dictionary. The same tuples repeat across files,
        # so the deduped set stays small; the catalog upserts them (additive) so
        # the tree/pruning stay exact for newly-ingested data without a reindex.
        combo_seen: set = set()
        partition_combinations: List[Dict[str, str]] = []
        for item in file_items:
            for combo in item.get("partition_combinations") or []:
                key = tuple(sorted(combo.items()))
                if key not in combo_seen:
                    combo_seen.add(key)
                    partition_combinations.append(combo)

        # Send all files to catalog in a single request
        body: Dict[str, Any] = {"files": file_entries}
        # Omitted when empty so older catalogs simply ignore the absent field.
        if partition_combinations:
            body["partition_combinations"] = partition_combinations
        response = self._catalog.post(
            f"/namespaces/{self.namespace}/tables/{self.table_name}/manifest/add-files",
            json=body,
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
        # Signal the background timer to exit its loop. No-op when the
        # timeout feature is disabled.
        self._timeout.stop()
        if self._blob_client:
            self._blob_client.shutdown()
