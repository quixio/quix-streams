"""
Quix Lake Blob Storage Sink

This module provides a sink that writes Kafka batches to blob storage as
Hive-partitioned Parquet files, with optional REST Catalog integration.

Uses quixportal for unified blob storage access (Azure, AWS S3, GCP, MinIO, local).
"""

import logging
import threading
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
    :param stream_timeout_ms: Optional **per-key** silence threshold in
        milliseconds. Must be a positive int. Paired with
        ``on_stream_timeout``; both must be provided to enable the feature.
        One "stream" = one Kafka message key; the threshold applies
        independently to each key the sink observes.
    :param on_stream_timeout: Optional callback invoked with a stream
        designation (``Callable[[str], None]``) whenever a particular
        Kafka message key has been silent for at least
        ``stream_timeout_ms``. The designation passed in is the message
        key decoded as a UTF-8 string. Fires **exactly once per silence
        period per key** and then evicts that key's tracking entry; if
        the same key starts producing again, it is treated as a fresh
        stream and can fire again on its next silence. Runs
        synchronously on the sink thread during ``flush()``; exceptions
        are logged and swallowed so a misbehaving callback does not kill
        the sink.

        The feature is **enabled only when both** ``stream_timeout_ms`` is a
        positive int **and** ``on_stream_timeout`` is callable. Any other
        combination (either ``None``, zero, negative, wrong type) disables
        the feature with zero overhead: no per-key dict is allocated, no
        check loop runs.

        *Per-key semantics:* silence is tracked per Kafka message key
        in a single in-memory dict ``{key: last_seen_ms}``. Every
        ``add()`` refreshes that key's entry; ``flush()`` collects all
        keys whose silence ≥ threshold, fires each exactly once, and
        evicts them. A TTL safety sweep also evicts any entry older
        than ``3 × stream_timeout_ms`` without firing, as a belt-and-
        braces bound on dict growth.

        *Precision floor:* timeouts fire from a background daemon
        thread on a periodic cadence (``_check_interval_ms``, defaulting
        to ``max(100, min(1000, stream_timeout_ms // 5))`` ms), and also
        at the end of ``flush()`` as a belt-and-suspenders secondary
        trigger. Expect fire latency up to roughly
        ``stream_timeout_ms + _check_interval_ms`` — e.g. ~61 s with the
        default 60 s threshold and 1000 ms check interval. This is a
        change from earlier drafts: a purely flush-driven cadence
        missed keys whose silence began at or after the last message,
        because ``BatchingSink`` stops calling ``flush()`` once there
        are no batches left. A periodic timer fixes that.

        *Restart / rebalance behaviour:* tracking state is in-memory
        only. On process restart or partition rebalance the dict for
        the affected keys is lost; keys that remain dormant will not
        fire for the current silence cycle. Keys that resume publishing
        after restart/rebalance are fresh streams and will fire normally
        on their next silence.

        *Backpressure:* ``on_paused()`` does not touch the per-key
        tracking dict. Timeout reflects "when did we last see data from
        this key", independent of write success.

        *Concurrency:* ``add()`` and ``flush()`` run on the consumer
        thread; a background daemon thread also drives the periodic
        check (see *Precision floor*). The per-key tracking dict is
        guarded by a ``threading.Lock`` held only across tiny dict
        operations — the user callback is invoked OUTSIDE the lock so
        a blocking callback cannot stall ``add()``. Because Kafka maps
        one key to one partition, and one partition is owned by one
        consumer replica at a time, the same key cannot be stamped
        from two replicas simultaneously — no cross-replica coordinator
        is required.
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
        _check_interval_ms: Optional[int] = None,
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

        # Stream-timeout tracking (opt-in, per-key silence detector).
        # Feature is enabled iff both scalar params are usable; mismatched
        # pairs raise ValueError. Disabled path is zero-overhead:
        # ``_stream_timeout_enabled`` stays ``False`` and the per-key dict
        # is never allocated.
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
            # Per-key last-seen timestamps. Allocated only when enabled.
            # Keys are the decoded Kafka message keys (see
            # ``_decode_stream_name``); values are wall-clock ms from
            # ``_now_ms()``. Bounded in practice by eviction-on-fire and
            # the 3x TTL safety sweep inside ``_check_timeouts()``.
            self._last_seen_by_key: dict[str, int] = {}

            # Background timer-thread machinery (spec v6 §7.6, REVISED
            # 2026-04-22). Local QA proved the flush-only cadence is
            # insufficient: when the input topic goes fully silent,
            # BatchingSink stops calling flush() (no batch to flush), so
            # keys whose silence begins at or after the last message
            # never get checked. A periodic daemon thread now drives
            # _check_timeouts() on a wall-clock cadence; flush() still
            # calls it as a belt-and-suspenders secondary trigger.
            self._stop: threading.Event = threading.Event()
            # Guards self._last_seen_by_key against concurrent mutation
            # from the consumer thread (add/flush) and the timer thread
            # (periodic check). Critical sections are tiny (dict op + a
            # few comparisons); the callback is explicitly invoked
            # OUTSIDE the lock so user code cannot stall the consumer.
            self._tracker_lock: threading.Lock = threading.Lock()
            # Cadence for the periodic check. Default: stream_timeout_ms
            # // 5 clamped to [100, 1000] ms. Overridable via the hidden
            # _check_interval_ms kwarg for tests only (no env var, no
            # public API). A positive int override wins; anything else
            # falls back to the computed default.
            if (
                isinstance(_check_interval_ms, int)
                and not isinstance(_check_interval_ms, bool)
                and _check_interval_ms > 0
            ):
                self._check_interval_ms: int = _check_interval_ms
            else:
                self._check_interval_ms = max(
                    100, min(1000, stream_timeout_ms // 5)
                )
            # Thread placeholder; the thread is started in setup() after
            # the blob client init so a blob failure still tears down
            # cleanly without leaving an orphan timer thread running.
            self._timer_thread: Optional[threading.Thread] = None

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
    # Stream-timeout tracking (per-key silence detector — v6)
    # ------------------------------------------------------------------
    #
    # The three hooks below (``add``, ``flush``, ``on_paused``) layer an
    # opt-in per-key silence detector on top of the parent ``BatchingSink``.
    # State is a single dict ``{stream_name: last_seen_ms}`` where
    # ``stream_name`` is the decoded Kafka message key. ``add()`` refreshes
    # the entry; ``flush()`` runs ``_check_timeouts()`` which collects
    # timed-out keys in one pass and then fires + evicts them. Eviction is
    # the fire marker — there is no separate ``_fired`` flag. A re-activated
    # key is treated as a fresh stream with a new baseline.
    #
    # Feature gate: if ``_stream_timeout_enabled`` is False (because the
    # constructor received an unusable pair, a zero/None threshold, or a
    # non-callable callback), each hook short-circuits after the ``super()``
    # call with a single boolean check. No dict is allocated, no loop runs.
    #
    # Concurrency (updated v6 §7.6, REVISED 2026-04-22): ``add()`` and
    # ``flush()`` run on the consumer/sink thread, but a dedicated
    # background daemon thread also drives ``_check_timeouts()`` on a
    # periodic cadence (spec v6 §7.6 — the flush-only cadence missed
    # keys whose silence began at/after the last message). The per-key
    # dict is therefore guarded by ``self._tracker_lock``; critical
    # sections are tiny (dict op + comparisons) and the user callback
    # is intentionally invoked OUTSIDE the lock so arbitrarily-long
    # callbacks cannot stall the consumer thread. Kafka still
    # guarantees one key -> one partition -> one replica at any moment,
    # so a given stream name cannot be stamped from two replicas
    # simultaneously — no cross-replica coordinator is required.

    def _now_ms(self) -> int:
        """Return the current wall-clock time in milliseconds.

        All references to "now" inside silence detection, firing, and the
        TTL sweep route through this method so tests can override it with
        a monkeypatched instance attribute to drive deterministic
        timelines without ``time.sleep`` or ``freezegun``.
        """
        return int(time.time() * 1000)

    @staticmethod
    def _decode_stream_name(key: Any) -> Optional[str]:
        """Decode a Kafka message key into a stream-name string.

        Policy (spec v6 §6.3):
        - ``None`` → return ``None`` (null-keyed record; not tracked).
        - ``bytes`` / ``bytearray`` → UTF-8 decode; on ``UnicodeDecodeError``
          return ``None`` and a WARNING is logged by the caller.
        - ``str`` → return as-is.
        - anything else → ``str(key)``.
        """
        if key is None:
            return None
        if isinstance(key, (bytes, bytearray)):
            try:
                return bytes(key).decode("utf-8")
            except UnicodeDecodeError:
                return None
        if isinstance(key, str):
            return key
        return str(key)

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
        refresh the per-key last-seen timestamp for this message's key.

        A record whose key is ``None`` or whose bytes are not valid UTF-8
        is still sunk normally, but is skipped for timeout tracking; a
        WARNING is logged for undecodable bytes (every occurrence — no
        dedup, per spec §8.7 O-2).
        """
        super().add(value, key, timestamp, headers, topic, partition, offset)
        if not self._stream_timeout_enabled:
            return
        stream_name = self._decode_stream_name(key)
        if stream_name is None:
            if key is not None:
                # Undecodable bytes (UTF-8 failure). Null keys are silently
                # skipped; bad bytes are loud.
                logger.warning(
                    "Skipping stream-timeout tracking for record on topic "
                    "%r partition %d offset %d: key %r is not valid UTF-8",
                    topic, partition, offset, key,
                )
            return
        # Lock-guarded write: the timer thread may be reading/mutating
        # _last_seen_by_key concurrently. Critical section is a single
        # dict assignment.
        now_ms = self._now_ms()
        with self._tracker_lock:
            self._last_seen_by_key[stream_name] = now_ms

    def flush(self):
        """Flush the parent batch, then (if enabled) check whether any key
        has been silent past the threshold and fire + evict.

        The timeout check runs *after* the parquet write so a callback can
        trust that everything up to this moment has been committed.
        """
        super().flush()
        if self._stream_timeout_enabled:
            self._check_timeouts()

    def on_paused(self):
        """Inherit the parent ``on_paused()`` behaviour — drop the pending
        batches but **do not touch** the per-key tracking dict.

        Backpressure means the destination rejected a batch, not that the
        messages were never seen. Per-key silence timers continue from the
        same last-seen stamp regardless of write success.
        """
        super().on_paused()
        # intentional no-op on tracker state

    def _check_timeouts(self) -> None:
        """Fire keys whose silence >= threshold; also TTL-sweep any
        stragglers older than ``3 x stream_timeout_ms``.

        Concurrency: callable from two threads — the consumer thread
        (via ``flush()``) and the background timer thread (via
        ``_timeout_check_loop``). The snapshot pass acquires
        ``self._tracker_lock`` to collect keys to fire/drop; user
        callbacks run OUTSIDE the lock so a blocking / misbehaving
        callback cannot stall the consumer thread's ``add()`` path.

        Eviction policy: a fired key is evicted from the tracker ONLY
        AFTER its callback returns successfully. If the callback raises,
        the key remains tracked and will fire again on the next check
        cycle — giving the caller retry semantics on transient failure.
        The ``3x`` TTL sweep is the backstop that eventually drops a
        key whose callback keeps failing. TTL-sweep evictions happen
        inside the snapshot lock (no callback involved).
        """
        now_ms = self._now_ms()
        timeout = self._stream_timeout_ms
        ttl_evict = 3 * timeout

        # --- Critical section: snapshot + TTL-sweep evict under the lock. ---
        to_fire: list[tuple[str, int]] = []
        to_drop_silently: list[str] = []
        with self._tracker_lock:
            if not self._last_seen_by_key:
                return
            for stream_name, last_seen in self._last_seen_by_key.items():
                silence = now_ms - last_seen
                if silence >= ttl_evict:
                    # Should have fired already at 1x; safety-sweep fallback.
                    to_drop_silently.append(stream_name)
                elif silence >= timeout:
                    to_fire.append((stream_name, silence))
            # TTL-sweep entries have no callback, so evict them in-lock
            # right now. ``to_fire`` entries are NOT evicted here — they
            # are evicted below, only after the callback succeeds.
            for stream_name in to_drop_silently:
                self._last_seen_by_key.pop(stream_name, None)
        # --- End of critical section. Callbacks run unlocked below. ---

        for stream_name, silence in to_fire:
            logger.info(
                "Stream %r timed out (silence %d ms >= threshold %d ms)",
                stream_name, silence, timeout,
            )
            try:
                self._on_stream_timeout(stream_name)
            except Exception:
                logger.exception(
                    "on_stream_timeout callback raised for %r", stream_name
                )
                # Leave entry in tracker; retry on next check cycle.
                continue
            # Callback succeeded → evict. Lock-guarded for safe dict
            # mutation against a concurrent add() on the consumer thread.
            with self._tracker_lock:
                self._last_seen_by_key.pop(stream_name, None)

        for stream_name in to_drop_silently:
            logger.warning(
                "Stream %r dropped by TTL sweep (silence >= 3x threshold %d ms)",
                stream_name, timeout,
            )

    def _timeout_check_loop(self) -> None:
        """Periodic timeout-check loop for the background daemon thread.

        Runs ``_check_timeouts()`` every ``self._check_interval_ms``
        milliseconds until ``self._stop`` is set. A raising check must
        NOT kill the thread (else silent keys would go untracked
        forever), so the body is wrapped in a broad ``try/except`` that
        logs and continues.
        """
        while not self._stop.is_set():
            try:
                self._check_timeouts()
            except Exception:
                logger.exception("Periodic stream-timeout check raised")
            # Event.wait returns early if the stop event fires — allows
            # a prompt shutdown from cleanup()/tests without waiting out
            # the full interval.
            self._stop.wait(self._check_interval_ms / 1000)

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

        # Start the background timeout-check thread AFTER the blob
        # client is healthy, so a blob-setup failure tears down cleanly
        # without leaving an orphan timer thread running. daemon=True
        # means the thread dies with the process; no join required.
        if self._stream_timeout_enabled:
            self._timer_thread = threading.Thread(
                target=self._timeout_check_loop,
                daemon=True,
                name="QuixTSDataLakeSink-timeout-check",
            )
            self._timer_thread.start()
            logger.info(
                "Started stream-timeout check thread "
                "(interval=%d ms, threshold=%d ms)",
                self._check_interval_ms, self._stream_timeout_ms,
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
        # Signal the background timer to exit its loop. The thread is
        # daemon=True so this is not strictly required for process exit,
        # but it lets tests and any future explicit-shutdown paths tear
        # the loop down promptly instead of waiting out the interval.
        if self._stream_timeout_enabled and self._stop is not None:
            self._stop.set()
        if self._blob_client:
            self._blob_client.shutdown()
