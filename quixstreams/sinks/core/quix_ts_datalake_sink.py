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


# Timestamp parts extracted from ``timestamp_column`` for Hive partitioning,
# as the strftime format that produces each one. ``%m``/``%d``/``%H`` are
# zero-padded by strftime itself, which is what the readers expect
# (``month=01``, never ``month=1``).
TIMESTAMP_PART_FORMAT = {
    "year": "%Y",
    "month": "%m",
    "day": "%d",
    "hour": "%H",
}

# How many leading values the epoch-unit detection will read through looking
# for a non-null sample. A column with more leading nulls than this falls back
# to Arrow's own min(), so detection never degrades into a row-by-row scan of a
# million-row flush.
_EPOCH_SAMPLE_SCAN_LIMIT = 1000

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
    :param query_api_url: Optional lake query API URL. When set, every successful
        manifest registration is followed by a ``files-added`` notification to
        ``{query_api_url}/tables/{table_name}/files-added`` so the API can push
        the new files to its subscribers. A failed notification is logged as a
        WARNING and never fails the flush.
    :param query_api_auth_token: Bearer token for the query API notification
        (the platform injects ``Quix__Lakehouse__Query__AuthToken``; pass it here)
    :param auto_discover: Whether to auto-register table on first write
    :param namespace: Catalog namespace (default: "default")
    :param auto_create_bucket: If True, attempt to create bucket/path in storage if missing
    :param max_workers: Maximum number of parallel upload threads (default: 10)
    :param row_group_rows: Maximum rows per Parquet ROW GROUP in the files this
        sink writes (default 1,000,000, the same as the lakehouse compaction's
        ``COMPACTION_ROW_GROUP_ROWS``). A reader pays about one storage range
        request per row group, so many small groups make every query slow on a
        high-latency storage path, while one huge group forfeits intra-file
        skipping and inflates reader memory. A flush smaller than this is, as
        always, a single row group.
    :param stats_columns: Optional list of column names to compute per-file
        min/max statistics ("zone maps") for. These are sent to the REST
        Catalog with each file and let the query layer skip files whose value
        range cannot satisfy a WHERE/ORDER BY on the column. ``None`` (default)
        computes stats for every numeric and timestamp column in each written
        file (cheap — the batch is already in memory). Pass an explicit list to
        restrict the set and bound catalog storage on very wide tables.
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

    # ---- Parquet row-group size ----------------------------------------------
    # Same rule as the lakehouse compaction/repartition rewrites: a reader needs
    # about one storage range request PER ROW GROUP (column chunks are coalesced
    # within a group, never across), so a file cut into many small groups costs
    # a round-trip storm on a high-latency storage path, while one enormous
    # group forfeits intra-file skipping and inflates reader memory. The size is
    # a fixed row count (default 1,000,000 = the lakehouse's
    # COMPACTION_ROW_GROUP_ROWS default and ~pyarrow's own default), so small
    # flushes are a single group and only large flushes are split.
    ROW_GROUP_ROWS_DEFAULT = 1_000_000

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
        query_api_url: Optional[str] = None,
        query_api_auth_token: Optional[str] = None,
        auto_discover: bool = True,
        namespace: str = "default",
        auto_create_bucket: bool = True,
        max_workers: int = 10,
        stats_columns: Optional[List[str]] = None,
        row_group_rows: Optional[int] = None,
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
        # The table's folder, as a blob key relative to this sink's blob client
        # (i.e. inside the workspace when one is set). Configured as
        # ``<s3_prefix>/<table_name>``, but REPLACED by the location the catalog
        # already holds for the table when the two disagree — see
        # _adopt_catalog_location. Everything the sink writes (data files, their
        # .vidx/ sidecars) hangs off this one value, so honouring a registered
        # location is a single assignment rather than a rule each write path has
        # to remember.
        self._table_root = "/".join(
            p for p in (s3_prefix.strip("/") if s3_prefix else "", table_name) if p
        )
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
        self._query_api = (
            QuixTSDataLakeCatalogClient(query_api_url, query_api_auth_token)
            if query_api_url
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

        # Parquet row-group size: a fixed, configurable row count.
        if row_group_rows is not None and int(row_group_rows) < 1:
            raise ValueError("row_group_rows must be >= 1")
        self._row_group_rows = (
            int(row_group_rows)
            if row_group_rows is not None
            else self.ROW_GROUP_ROWS_DEFAULT
        )

        # WHERE the data comes from, registered in the catalog alongside the
        # files (see _source_descriptor): the topic of the batch being written,
        # and the workspace it was consumed in. Set per batch in write(); the
        # set remembers which sources this process has already announced so the
        # standalone registration costs one request per topic, not per flush.
        self._batch_source: Optional[Dict[str, Any]] = None
        self._registered_sources: set = set()

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
        # Virtual-index SIDECAR uploads (one per data file, written to .vidx/).
        # Tracked separately from data files so _finalize_writes waits on them but
        # does NOT register them in the manifest (they are metadata, not data).
        self._pending_sidecar_futures: List[Dict[str, Any]] = []

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

        logger.info(f"Storage Target: {self.table_location}")
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
                # The catalog is the authority on where the table lives: if it
                # already holds a location, write THERE instead of at the
                # configured prefix (see _adopt_catalog_location).
                self._adopt_registered_location()

            # Check if table already exists and validate partition strategy
            self._validate_existing_table_structure()

        except Exception as e:
            logger.error("Failed to setup blob storage connection: %s", e)
            raise

        # Start the background timeout-check thread AFTER the blob
        # client is healthy, so a blob-setup failure tears down cleanly
        # without leaving an orphan timer thread running.
        self._timeout.start()

    @property
    def table_location(self) -> str:
        """The table's folder as a full blob URI — what the catalog records as
        the table ``location``, and what every file path registered in the
        manifest is prefixed with."""
        parts = [self.s3_bucket]
        if self.workspace_id:
            parts.append(self.workspace_id)
        parts.append(self._table_root)
        return "s3://" + "/".join(parts)

    def _table_root_from_location(self, location: str) -> str:
        """Parse a catalog table ``location`` into a blob key relative to this
        sink's blob client (which is scoped to ``workspace_id``).

        Raises ValueError when the location cannot be written to by this sink —
        a different bucket, or another workspace's folder. Guessing there would
        mean writing a table's data to a path nothing reads, which is the exact
        failure this method exists to prevent, so it fails loudly instead.
        """
        path = location.strip()
        if "://" in path:
            path = path.split("://", 1)[1]
        path = path.strip("/")
        bucket, _, remainder = path.partition("/")
        if not remainder:
            raise ValueError(
                f"Table '{self.table_name}' is registered at '{location}', which names no "
                f"folder inside a bucket; refusing to write."
            )
        if bucket != self.s3_bucket:
            raise ValueError(
                f"Table '{self.table_name}' is registered in bucket '{bucket}' but this sink "
                f"writes to '{self.s3_bucket}'. Point the sink at the same storage, or use a "
                f"different table name — writing to two buckets would split the table."
            )
        if self.workspace_id:
            prefix = f"{self.workspace_id}/"
            if not remainder.startswith(prefix):
                raise ValueError(
                    f"Table '{self.table_name}' is registered at '{location}', outside this "
                    f"sink's workspace folder '{self.workspace_id}'. Writing there would put "
                    f"the data where this deployment cannot read it back."
                )
            remainder = remainder[len(prefix) :]
        remainder = remainder.strip("/")
        if not remainder:
            raise ValueError(
                f"Table '{self.table_name}' is registered at '{location}', which is the "
                f"workspace root rather than a table folder; refusing to write."
            )
        return remainder

    def _adopt_catalog_location(self, table_metadata: Dict[str, Any]) -> bool:
        """Write to the location the catalog already holds for this table.

        A table's location is where every file in its manifest lives. A second
        writer joining an existing table (a redeploy with a different prefix, a
        differently-configured second sink, a table that was registered by
        something else entirely) must therefore follow the registered location
        rather than start a second folder the readers know nothing about — and
        the catalog, for its part, refuses to be re-pointed by such a writer.

        Returns True when the sink's write path changed.
        """
        location = (
            (table_metadata or {}).get("location")
            if isinstance(table_metadata, dict)
            else None
        )
        if not isinstance(location, str) or not location.strip():
            return False
        root = self._table_root_from_location(location)
        if root == self._table_root:
            return False
        logger.warning(
            "Table '%s' is registered at %s; writing there instead of the configured "
            "%s (the catalog's location wins, so the data lands where the table is read "
            "from).",
            self.table_name,
            location,
            self.table_location,
        )
        self._table_root = root
        return True

    def _adopt_registered_location(self) -> None:
        """Read the table's registered location at startup, if it has one."""
        if self._catalog is None:
            return
        try:
            response = self._catalog.get(
                f"/namespaces/{self.namespace}/tables/{self.table_name}", timeout=10
            )
        except Exception as e:
            # A catalog hiccup must not stop the sink; the configured path is
            # also what a brand-new table would get. _register_table checks
            # again (and adopts) before the first write.
            logger.warning(
                "Could not read the registered location of table '%s' (%s); "
                "continuing with %s",
                self.table_name,
                e,
                self.table_location,
            )
            return
        if response.status_code == 200:
            self._adopt_catalog_location(response.json())

    def _source_descriptor(self, batch: SinkBatch) -> Dict[str, Any]:
        """Where the batch came from, in the shape the catalog's
        ``table_sources`` registry takes. One row per (type, workspace, topic),
        so a table fed by several topics keeps one entry per topic instead of
        the writers overwriting each other."""
        properties = {"sink": "quixstreams-quix-ts-datalake-sink"}
        try:  # lazy: avoids importing the package root from inside it
            from quixstreams import __version__

            properties["quixstreams_version"] = __version__
        except Exception as e:  # pragma: no cover - defensive
            logger.debug("Could not read the QuixStreams version: %s", e)
        return {
            "source_type": "kafka",
            "workspace_id": self.workspace_id or "",
            "topic": batch.topic,
            "properties": properties,
        }

    def _register_source(self) -> None:
        """Announce this batch's topic as a source of the table, once per topic
        per process. The per-flush counters ride along with add-files instead
        (no extra request); this call is what makes a topic visible as a source
        even before — or without — any file of its own."""
        source = self._batch_source
        if not self._catalog or not source:
            return
        identity = (source["source_type"], source["workspace_id"], source["topic"])
        if identity in self._registered_sources:
            return
        try:
            response = self._catalog.post(
                f"/namespaces/{self.namespace}/tables/{self.table_name}/sources",
                json={"source": source},
                timeout=10,
            )
            if response.status_code == 200:
                self._registered_sources.add(identity)
                logger.info(
                    "Registered source topic '%s' (workspace '%s') for table '%s'",
                    source["topic"],
                    source["workspace_id"],
                    self.table_name,
                )
            elif response.status_code == 501:
                # Older catalog without the sources registry: stop asking.
                self._registered_sources.add(identity)
                logger.info(
                    "Catalog does not support the table-sources registry; "
                    "skipping source registration"
                )
            else:
                logger.warning(
                    "Could not register source topic '%s' for table '%s': %s %s",
                    source["topic"],
                    self.table_name,
                    response.status_code,
                    response.text[:200],
                )
        except Exception as e:
            # Provenance is metadata: never fail a write over it.
            logger.warning(
                "Could not register source topic '%s' for table '%s': %s",
                source["topic"],
                self.table_name,
                e,
            )

    def _ensure_bucket(self):
        """Ensure the blob storage path is accessible."""
        if not self._blob_client.ensure_path_exists(
            auto_create=self._auto_create_bucket
        ):
            raise RuntimeError("Failed to access blob storage")
        logger.info("Successfully connected to blob storage")

    def write(self, batch: SinkBatch):
        """Write batch directly to blob storage."""
        # WHERE this data comes from, for the catalog's source registry. Set
        # before registration so a brand-new table's first flush records its
        # topic too.
        self._batch_source = self._source_descriptor(batch)

        # Register table before first write if auto-discover is enabled
        if self.auto_discover and not self.table_registered and self._catalog:
            self._register_table()
        # Only once the table is known to the catalog: the same condition under
        # which the files themselves are registered (_finalize_writes), so a
        # sink told not to auto-discover does not ask about a table that is not
        # there yet on every flush.
        if self.table_registered:
            self._register_source()

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

        Arrow all the way: the batch is assembled column-wise into one
        ``pa.Table`` (:meth:`_batch_to_arrow`), split into Hive partitions by a
        dictionary-encode + stable sort of the partition key
        (:meth:`_partition_groups`), and each group is serialised straight from
        Arrow. No DataFrame is built at any point, so a flush costs ONE
        materialisation of the data instead of the three the pandas path needed
        (a dict per row, an object array per column, then the Arrow conversion).

        Returns the number of rows actually grouped and written to storage.
        Equals batch.size in normal operation; reported by write() so any
        future silent-drop regression is visible in the log instead of
        being papered over with the input count.
        """
        if not batch:
            return 0

        table = self._batch_to_arrow(batch)
        if not table.num_rows:
            return 0

        # Add time-based partition columns (year/month/day/hour) if they're
        # specified in hive_columns. These are extracted from timestamp_column.
        if self._ts_hive_columns:
            table = self._add_timestamp_columns(table)

        # Use only the explicitly specified partition columns
        if partition_columns := self.hive_columns.copy():
            # Group by partition columns and write each partition separately.
            # This creates the Hive-style directory structure:
            # col1=val1/col2=val2/file.parquet. A row whose partition value is
            # NULL (or whose partition column is absent from the record) lands
            # in the ``col=__None__`` bucket rather than being dropped — the
            # pandas path needed an explicit fillna() for this because
            # groupby(dropna=True) silently discarded such rows while write()
            # still reported success using batch.size.
            rows_written = 0
            for partition_values, group in self._partition_groups(
                table, partition_columns
            ):
                # Build storage key with Hive partitioning (col=value format)
                partition_parts = [
                    f"{col}={val}"
                    for col, val in zip(partition_columns, partition_values)
                ]
                storage_key = (
                    f"{self._table_root}/"
                    + "/".join(partition_parts)
                    + f"/data_{uuid.uuid4().hex}.parquet"
                )

                # Remove partition columns from data (Hive style - partition
                # values are in the path, not the data)
                present = [c for c in partition_columns if c in group.column_names]
                data = group.drop_columns(present) if present else group

                # Write to blob storage
                self._write_parquet_to_storage(
                    data, storage_key, partition_columns, partition_values
                )
                rows_written += group.num_rows
        else:
            # No partitioning - write as single file directly under table directory
            storage_key = f"{self._table_root}/data_{uuid.uuid4().hex}.parquet"
            self._write_parquet_to_storage(table, storage_key, [], ())
            rows_written = table.num_rows

        # Wait for all uploads to complete and register files in catalog
        self._finalize_writes()
        return rows_written

    def _batch_to_arrow(self, batch: SinkBatch) -> "pa.Table":
        """Assemble the batch into an Arrow table COLUMN-WISE, in one pass.

        The shape is the one the pandas path produced: the union of every
        record's keys in first-seen order, NULL where a record does not carry a
        key, plus the sink's ``timestamp_column`` (Kafka's own timestamp when
        the record has none) and ``__key``.

        Three things that used to cost a separate pass (or a surprise) happen
        here:

        * an EMPTY dict becomes NULL. Parquet cannot store an empty
          struct/map, so the write would fail — this is what the old
          ``_null_empty_dicts`` DataFrame scan was for.
        * a key missing from a record is padded with NULL, not with pandas'
          NaN, so an integer column stays an integer column instead of being
          widened to float the moment one record omits it.
        * the record's own dict is never copied or mutated. The pandas path
          copied every value dict just to stamp two extra fields into it.
        """
        columns: Dict[str, List[Any]] = {}
        ts_column = self.timestamp_column
        rows = 0

        for item in batch:
            value = item.value
            for name, v in value.items():
                col = columns.get(name)
                if col is None:
                    col = columns[name] = [None] * rows
                elif len(col) < rows:
                    # Absent from one or more earlier records in this batch.
                    col.extend([None] * (rows - len(col)))
                # Parquet has no empty struct/map; store NULL instead.
                col.append(None if isinstance(v, dict) and not v else v)

            # The timestamp column is only injected when the record has none:
            # a record carrying its own timestamp keeps it.
            if ts_column not in value:
                col = columns.get(ts_column)
                if col is None:
                    col = columns[ts_column] = [None] * rows
                elif len(col) < rows:
                    col.extend([None] * (rows - len(col)))
                col.append(item.timestamp)

            # The Kafka message key always wins over a field of the same name,
            # as it did when this was a dict assignment.
            col = columns.get("__key")
            if col is None:
                col = columns["__key"] = [None] * rows
            if len(col) > rows:
                col[rows] = item.key
            else:
                if len(col) < rows:
                    col.extend([None] * (rows - len(col)))
                col.append(item.key)

            rows += 1

        arrays: Dict[str, Any] = {}
        for name, col in columns.items():
            if len(col) < rows:
                col.extend([None] * (rows - len(col)))
            arrays[name] = pa.array(col)
        return pa.table(arrays)

    @staticmethod
    def _first_non_null(column) -> Any:
        """First non-null value of an Arrow (chunked) array, or None.

        ``null_count`` is footer-cheap metadata, so a column without nulls
        (the normal case) is answered by a single scalar read.
        """
        chunks = column.chunks if isinstance(column, pa.ChunkedArray) else [column]
        scanned = 0
        for chunk in chunks:
            if not len(chunk):
                continue
            if chunk.null_count == 0:
                return chunk[0].as_py()
            for i in range(len(chunk)):
                v = chunk[i].as_py()
                if v is not None:
                    return v
                scanned += 1
                if scanned >= _EPOCH_SAMPLE_SCAN_LIMIT:
                    # Pathologically many leading nulls: let Arrow find a value.
                    agg = pc.min(column)
                    return agg.as_py() if agg.is_valid else None
        return None

    def _timestamp_as_arrow(self, column):
        """The timestamp column as an Arrow timestamp array, ready for strftime.

        * a timestamp column is used as it stands (a tz-aware one formats in its
          own zone, exactly as ``.dt`` did);
        * a date column is widened to a timestamp;
        * an ISO-8601 string column is parsed (the pandas path crashed on one);
        * a numeric column is an epoch whose UNIT is detected from the magnitude
          of the first non-null value, with the same thresholds the pandas path
          used (>1e17 ns, >1e14 us, >1e11 ms, else s).
        """
        t = column.type
        if pa.types.is_timestamp(t):
            return column
        if pa.types.is_date(t):
            return pc.cast(column, pa.timestamp("ms"))
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            return pc.cast(column, pa.timestamp("us"))
        if column.null_count == len(column):
            # Nothing to detect. Every part is NULL, which the partition key
            # turns into the Hive-NULL bucket like any other missing value.
            return pc.cast(column, pa.timestamp("ms"))

        sample = self._first_non_null(column)
        sample = float(sample) if sample is not None else 0.0
        if sample > 1e17:
            unit = "ns"  # Nanoseconds (Java/Kafka timestamps)
        elif sample > 1e14:
            unit = "us"  # Microseconds
        elif sample > 1e11:
            unit = "ms"  # Milliseconds (common in JavaScript/Kafka)
        else:
            unit = "s"  # Seconds (Unix timestamp)

        if not pa.types.is_integer(t):
            # A float epoch truncates to whole units, as to_datetime(unit=) did.
            column = pc.cast(column, pa.int64(), safe=False)
        return pc.cast(column, pa.timestamp(unit))

    def _add_timestamp_columns(self, table: "pa.Table") -> "pa.Table":
        """
        Add timestamp-based columns (year/month/day/hour) for time-based partitioning.

        This method extracts time components from the timestamp column and adds them
        as separate columns that can be used for Hive partitioning. The source
        ``timestamp_column`` is **not** mutated — derivation happens on a local
        timestamp view of it. Preserving its type is part of the sink's contract
        with readers (in particular, ``ts_ms`` is the system-injected Kafka
        timestamp and must always land in parquet as int64 ms, regardless of
        whether time-based hive partitioning is configured).

        Parts are derived in the configured partition order, so the table's
        column order does not depend on Python's per-process set iteration.
        """
        ts = self._timestamp_as_arrow(table.column(self.timestamp_column))

        for col in self._partition_spec_order:
            if col not in self._ts_hive_columns:
                continue
            # strftime zero-pads month/day/hour; year stays as-is.
            part = pc.strftime(ts, format=TIMESTAMP_PART_FORMAT[col])
            index = table.schema.get_field_index(col)
            if index >= 0:
                table = table.set_column(index, col, part)
            else:
                table = table.append_column(col, part)
        return table

    def _partition_key_strings(self, table: "pa.Table", column: str):
        """One Hive path segment value per row, as a string array with NULLs
        replaced by the :data:`HIVE_NULL_PARTITION` sentinel.

        String and integer columns are cast by Arrow. Float, bool and timestamp
        columns go through Python's ``str()`` because that is what the pandas
        groupby produced, and a partition folder must not be renamed by a sink
        upgrade (Arrow would write ``5`` where pandas wrote ``5.0``, and
        ``true`` where it wrote ``True``, splitting one value into two folders).
        A float NaN counts as missing, as it did under fillna().
        """
        rows = table.num_rows
        if column not in table.column_names:
            # The column is in hive_columns but no record in this batch carries
            # it: everything belongs to the NULL bucket. (The pandas path raised
            # KeyError here and the flush failed after three attempts.)
            logger.warning(
                "Partition column '%s' is missing from every record in this batch; "
                "writing to %s=%s",
                column,
                column,
                HIVE_NULL_PARTITION,
            )
            return pa.array([HIVE_NULL_PARTITION] * rows, type=pa.string())

        arr = table.column(column).combine_chunks()
        t = arr.type
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            strings = arr
        elif (
            pa.types.is_integer(t)
            or pa.types.is_date(t)
            or pa.types.is_decimal(t)
            or pa.types.is_null(t)
        ):
            strings = pc.cast(arr, pa.string())
        elif pa.types.is_floating(t):
            strings = pa.array(
                [None if v is None or v != v else str(v) for v in arr.to_pylist()],
                type=pa.string(),
            )
        else:
            strings = pa.array(
                [None if v is None else str(v) for v in arr.to_pylist()],
                type=pa.string(),
            )
        return pc.fill_null(strings, HIVE_NULL_PARTITION)

    def _partition_groups(self, table: "pa.Table", partition_columns: List[str]):
        """Yield ``(partition_values, group_table)`` once per distinct partition
        tuple in the batch.

        One dictionary-encode of the joined key, one stable ``sort_indices``,
        then one ``take`` per group — so the batch is scanned a constant number
        of times whatever the number of partitions, and row order WITHIN a
        partition is the order the records arrived in (which keeps a
        time-ordered topic's files time-ordered, and their zone maps tight).
        The common case of a flush that belongs to a single partition takes no
        copy at all.
        """
        key_arrays = [self._partition_key_strings(table, c) for c in partition_columns]
        if len(key_arrays) == 1:
            keys = key_arrays[0]
        else:
            keys = pc.binary_join_element_wise(*key_arrays, "\x1f")

        encoded = keys.dictionary_encode()
        if len(encoded.dictionary) <= 1:
            # Single partition: no reordering, no copy of the batch.
            yield tuple(a[0].as_py() for a in key_arrays), table
            return

        codes = encoded.indices
        order = pc.sort_indices(codes)
        sorted_codes = codes.take(order)
        # Run boundaries: where the sorted code changes, a new partition starts.
        changes = pc.indices_nonzero(
            pc.not_equal(
                sorted_codes.slice(1), sorted_codes.slice(0, len(sorted_codes) - 1)
            )
        ).to_pylist()
        starts = [0] + [i + 1 for i in changes]

        for n, start in enumerate(starts):
            end = starts[n + 1] if n + 1 < len(starts) else len(order)
            indices = order.slice(start, end - start)
            first_row = indices[0].as_py()
            values = tuple(a[first_row].as_py() for a in key_arrays)
            yield values, table.take(indices)

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
            if (
                pa.types.is_integer(t)
                or pa.types.is_floating(t)
                or pa.types.is_decimal(t)
            ):
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

    def _write_parquet_to_storage(
        self,
        table: "pa.Table",
        storage_key: str,
        partition_columns: List[str],
        partition_values: tuple,
    ):
        """Write one Arrow table to blob storage as Parquet.

        The table arrives ready to serialise (:meth:`_batch_to_arrow` already
        nulled empty dicts and :meth:`_write_batch` dropped the partition
        columns), so nothing here touches the data. Notably the file carries
        exactly the record's own columns: the pandas path wrote an extra
        ``__index_level_0__`` column into every file of a partitioned table,
        because a group's index is no longer a RangeIndex and
        ``Table.from_pandas`` serialises such an index as a column.
        """
        # Compute per-column min/max zone maps from the in-memory table BEFORE
        # serialising — nearly free, and avoids re-reading the parquet footer
        # from storage later. Carried through _pending_futures into the catalog
        # add-files call (see _register_files_in_manifest).
        column_stats = self._compute_column_stats(table)

        buf = pa.BufferOutputStream()
        pq.write_table(table, buf, row_group_size=self._row_group_rows)
        parquet_bytes = buf.getvalue().to_pybytes()
        if table.num_rows > self._row_group_rows:
            logger.debug(
                "Wrote %s with %d row groups of <= %d rows",
                storage_key,
                -(-table.num_rows // self._row_group_rows),
                self._row_group_rows,
            )

        # Submit async upload
        if self._blob_client is None:
            raise RuntimeError("BlobStorageClient not initialized. Call setup() first.")
        future = self._blob_client.put_object_async(storage_key, parquet_bytes)

        self._pending_futures.append(
            {
                "future": future,
                "key": storage_key,
                "row_count": table.num_rows,
                "file_size": len(parquet_bytes),
                "partition_columns": partition_columns,
                "partition_values": partition_values,
                "column_stats": column_stats,
            }
        )

        # The VIRTUAL index for this file goes to a blob ``.vidx/`` sidecar
        # (navigation reads it via DuckDB) — the SOLE virtual index; nothing
        # virtual is sent to Postgres anymore. column_stats (zone maps) still go
        # to the catalog above.
        self._write_virtual_sidecar(
            table, storage_key, partition_columns, partition_values
        )

    def _sidecar_key(self, storage_key: str) -> str:
        """Blob key of a data file's virtual-index sidecar: in a ``.vidx/``
        SUBFOLDER of the data file's own Hive partition folder (data
        ``.../region=EU/data_<uuid>.parquet`` -> index
        ``.../region=EU/.vidx/data_<uuid>.parquet``). Co-located with the data so
        compaction rewrites a partition's data and its index together, but in a
        dedicated dot-dir: a ``.vidx`` folder has no ``key=value`` and so is
        naturally skipped by partition discovery, keeping sidecars out of the data
        set. Navigation globs ``.../<physical path>/.vidx/*.parquet``."""
        folder, _, basename = storage_key.rpartition("/")
        return f"{folder}/.vidx/{basename}" if folder else f".vidx/{basename}"

    def _write_virtual_sidecar(
        self,
        table: "pa.Table",
        storage_key: str,
        partition_columns: List[str],
        partition_values: tuple,
    ):
        """Write this data file's virtual-index sidecar Parquet to ``.vidx/``.

        Content: one row per DISTINCT tuple of the table's VIRTUAL columns present
        in this file, with the file's PHYSICAL partition values added as constant
        columns. Carrying the physical columns in the content means readers need no
        ``hive_partitioning`` — a single ``read_parquet('{root}/.../.vidx/*')``
        yields full (physical + virtual) tuples that navigation aggregates
        (``SELECT DISTINCT <col> WHERE <ancestors>``) into the folder tree. The
        index is navigation-only (no query pruning), so no per-file back-reference
        is stored. No-op when the table has no virtual columns or none are present
        in this file. Never raises (the index is a hint, not the data).
        """
        present = [c for c in self._virtual_columns if c in table.column_names]
        if not present or self._blob_client is None:
            return
        try:
            # DISTINCT tuples, straight out of Arrow's hash aggregation.
            vtable = table.select(present).group_by(present).aggregate([])
            if not vtable.num_rows:
                return
            # Physical partition values are constant for this file (one Hive
            # folder) -> add them as constant columns so the sidecar holds the
            # FULL partition tuple.
            for col, val in zip(partition_columns or [], partition_values or ()):
                if col in vtable.column_names:
                    continue
                vtable = vtable.append_column(
                    col, pa.array([str(val)] * vtable.num_rows, type=pa.string())
                )
            buf = pa.BufferOutputStream()
            pq.write_table(vtable, buf)
            sidecar_key = self._sidecar_key(storage_key)
            future = self._blob_client.put_object_async(
                sidecar_key, buf.getvalue().to_pybytes()
            )
            self._pending_sidecar_futures.append(
                {"future": future, "key": sidecar_key, "row_count": vtable.num_rows}
            )
        except Exception as e:
            logger.warning("Failed to write virtual sidecar for %s: %s", storage_key, e)

    def _finalize_writes(self):
        """Wait for all pending uploads to complete and register files in catalog."""
        if not self._pending_futures:
            # Still drain any sidecars (defensive; normally paired with data).
            self._await_sidecar_uploads()
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

            # Virtual-index sidecars ride alongside the data files. Wait for them
            # too so the tree is queryable the moment the batch is acknowledged
            # (a sidecar failure only degrades the index, never blocks the data).
            self._await_sidecar_uploads()

            # Register all files in catalog manifest if configured
            if self._catalog and self.table_registered:
                self._register_files_in_manifest()
        finally:
            self._pending_futures.clear()

    def _await_sidecar_uploads(self):
        """Block on the virtual-index sidecar uploads. Best-effort: a failed
        sidecar is logged but never raised — the data is already safe and the
        index self-heals on the next write/reindex."""
        if not self._pending_sidecar_futures:
            return
        try:
            ok = 0
            for item in self._pending_sidecar_futures:
                try:
                    item["future"].result()
                    ok += 1
                except Exception as e:
                    logger.warning(
                        "Virtual sidecar upload failed (%s): %s", item["key"], e
                    )
            logger.debug(
                "Uploaded %d/%d virtual sidecar(s)",
                ok,
                len(self._pending_sidecar_futures),
            )
        finally:
            self._pending_sidecar_futures.clear()

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
            metadata = check_response.json()
            logger.info("Table '%s' already exists in catalog", self.table_name)
            self.table_registered = True
            # Validate partition strategy matches
            self._validate_partition_strategy(metadata)
            # Honour the table's registered location (a second writer joining an
            # existing table must not start a second folder), and re-check the
            # structure of the folder we are actually going to write into.
            if self._adopt_catalog_location(metadata):
                self._validate_existing_table_structure()
            # Keep the table's ORDERING / stats declarations in step with this
            # sink's configuration (they drive lakehouse compaction).
            self._sync_table_properties(metadata)
            return

        # Table doesn't exist, create it
        # Note: Location must be full S3 URI for catalog (API uses this with DuckDB)
        # Include workspace_id in the path if set (for workspace-scoped storage)
        location = self.table_location

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
        # The zone-map columns this sink was told to compute. Recorded on the
        # table because the lakehouse's own rewrites (compaction, repartition,
        # the stats backfill) read footers and would otherwise record a zone map
        # for EVERY numeric/timestamp column — re-introducing on a 2,000-signal
        # table exactly the stats the operator excluded here.
        if self._stats_columns is not None:
            properties["stats_columns"] = sorted(self._stats_columns)

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
            # Two sinks can reach this point for the same new table at the same
            # moment. The catalog keeps the location of whoever won and reports
            # the conflict; the loser follows it instead of writing into a
            # folder the table does not point at.
            try:
                created = create_response.json()
            except ValueError:
                created = {}
            if not isinstance(created, dict):
                created = {}
            if created.get("location_conflict") and self._adopt_catalog_location(
                created
            ):
                self._validate_existing_table_structure()
        else:
            raise RuntimeError(
                f"Failed to create table '{self.table_name}' in REST Catalog: "
                f"{create_response.status_code} {create_response.text}"
            )

    def _sync_table_properties(self, table_metadata: Dict[str, Any]) -> None:
        """Bring an EXISTING table's declarations in step with this sink.

        Two different kinds of value, two different rules:

        * ``stats_columns`` — the zone-map column set — is OWNED by the sink:
          it is the writer that computes those statistics, so this is the only
          place it is decided, and it is pushed whenever it differs (including
          back to "no restriction"). Without it on the table, the lakehouse's
          own rewrites read footers and record a zone map for every numeric and
          timestamp column, undoing the restriction on the next compaction.
        * ``timestamp_column`` / ``sort_column`` are FILLED IN only when the
          table has none. They are also settable by an operator in the lakehouse
          console, and a sink restart must not overwrite a deliberate choice
          made there.

        One PATCH, only when something actually differs, so a restart against an
        unchanged table costs nothing.
        """
        if not self._catalog:
            return
        properties = (
            table_metadata.get("properties")
            if isinstance(table_metadata, dict)
            else None
        )
        if not isinstance(properties, dict):
            properties = {}
        patch: Dict[str, Any] = {}

        desired_stats = (
            sorted(self._stats_columns) if self._stats_columns is not None else None
        )
        stored_stats = properties.get("stats_columns")
        if isinstance(stored_stats, str):  # tolerate a comma-joined legacy value
            stored_stats = [c.strip() for c in stored_stats.split(",") if c.strip()]
        if desired_stats != (sorted(stored_stats) if stored_stats else None):
            patch["stats_columns"] = desired_stats

        for key, value in (
            ("timestamp_column", self.timestamp_column),
            ("sort_column", self.sort_column),
        ):
            if value and not properties.get(key):
                patch[key] = value

        if not patch:
            return
        try:
            response = self._catalog.patch(
                f"/namespaces/{self.namespace}/tables/{self.table_name}/properties",
                json={"properties": patch},
                timeout=10,
            )
            if response.status_code == 200:
                logger.info("Updated table '%s' properties: %s", self.table_name, patch)
            elif response.status_code == 501:
                logger.info(
                    "Catalog backend does not support property updates; "
                    "leaving %s unset",
                    sorted(patch),
                )
            else:
                logger.warning(
                    "Could not update properties %s on table '%s': %s %s",
                    sorted(patch),
                    self.table_name,
                    response.status_code,
                    response.text[:200],
                )
        except Exception as e:
            # Declarations are metadata: a failure must not stop ingestion.
            logger.warning(
                "Could not update properties %s on table '%s': %s",
                sorted(patch),
                self.table_name,
                e,
            )

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
        table_prefix = f"{self._table_root}/"

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
            # NOTE: the VIRTUAL index (per-file virtual values + co-occurrence
            # tuples) is no longer sent to the catalog/Postgres — it lives entirely
            # in the blob ``.vidx/`` sidecars written by _write_virtual_sidecar.
            # Only column_stats (zone maps) still ride along in the manifest.
            file_entries.append(entry)

        # Send all files to catalog in a single request (files + column_stats
        # only; the virtual index is in blob sidecars now, not Postgres).
        # ``source`` rides along in the SAME request — the catalog records which
        # topic/workspace produced these files, and bumps that source's file and
        # row totals, in the transaction that registers them. An older catalog
        # ignores the field.
        body: Dict[str, Any] = {"files": file_entries}
        if self._batch_source:
            body["source"] = self._batch_source
        response = self._catalog.post(
            f"/namespaces/{self.namespace}/tables/{self.table_name}/manifest/add-files",
            json=body,
            timeout=10,
        )

        if response.status_code == 200:
            logger.info(f"Registered {len(file_entries)} file(s) in catalog manifest")
            self._notify_query_api(file_entries)
        else:
            raise RuntimeError(
                f"Failed to register files in catalog manifest: "
                f"{response.status_code} {response.text}"
            )

    def _notify_query_api(self, file_entries: List[Dict[str, Any]]) -> None:
        """Tell the query API which files were just registered.

        Best effort: the catalog is the source of truth and the flush already
        succeeded, so any failure here is logged and swallowed. Zone maps
        (``column_stats``) are large and useless to subscribers, so they stay out.
        """
        if self._query_api is None:
            return
        files = [
            {key: value for key, value in entry.items() if key != "column_stats"}
            for entry in file_entries
        ]
        body: Dict[str, Any] = {"namespace": self.namespace, "files": files}
        path = f"/tables/{self.table_name}/files-added"
        try:
            response = self._query_api.post(path, json=body, timeout=5)
        except Exception as exc:
            logger.warning(
                f"Query API notify failed for table {self.table_name} "
                f"({len(files)} file(s)): {exc}"
            )
            return
        if response.status_code >= 300:
            logger.warning(
                f"Query API notify rejected for table {self.table_name} "
                f"({len(files)} file(s)): {response.status_code} {response.text}"
            )
            return
        logger.debug(
            f"Notified query API of {len(files)} file(s) in table {self.table_name}"
        )

    def cleanup(self):
        """Cleanup resources when sink is stopped."""
        # Signal the background timer to exit its loop. No-op when the
        # timeout feature is disabled.
        self._timeout.stop()
        if self._blob_client:
            self._blob_client.shutdown()
