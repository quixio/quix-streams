import contextlib
import logging
import time
import zlib
from typing import Any, Dict, List, Optional, Tuple

from quixstreams.sources.base import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    StatefulSource,
)

from .mysql_helper import MySqlCdcError, MySqlHelper, is_connection_error
from .snapshot import is_checkpointable_key

__all__ = ("MySqlCdcError", "MySqlCdcSource")

logger = logging.getLogger(__name__)

# Replication client ids are derived into this range. The floor of 1000 keeps derived
# ids clear of the low, hand-written ids (`server-id = 1`) that MySQL servers and
# tutorials use, so a connector never collides with the server it replicates from.
_SERVER_ID_MIN = 1000
_SERVER_ID_MAX = 2**31 - 1

# MySQL accepts any 32-bit unsigned value as an explicit server id.
_SERVER_ID_LIMIT = 2**32 - 1

# Consecutive connection failures tolerated before the source gives up and lets the
# platform restart it from the last committed position.
_MAX_RECONNECT_ATTEMPTS = 5


def _derive_server_id(name: str, database: str, table: str) -> int:
    """
    Derive a stable replication client id from the source's identity.

    Deterministic on purpose: reconnecting with the same id makes MySQL evict this
    deployment's own stale replica connection instead of accumulating ghost replicas
    until the server times them out, and two sources with different names or tables
    get distinct ids without anyone configuring them.
    """
    seed = f"{name}|{database}|{table}".encode()
    return _SERVER_ID_MIN + zlib.crc32(seed) % (_SERVER_ID_MAX - _SERVER_ID_MIN)


class MySqlCdcSource(StatefulSource):
    """
    NOTE: Requires `pip install quixstreams[mysql]` to work.

    This source reads row-level changes from a MySQL table using binary log
    replication and produces them to a Kafka topic, optionally preceded by an initial
    snapshot of the table's current contents.

    Provides "at-least-once" guarantees: the binlog position is only advanced after
    the messages it covers have been flushed to Kafka, so no change is lost, but a
    crash between the data flush and the position commit replays the last batch.
    Downstream consumers must treat change events as idempotent and deduplicate on the
    primary-key columns inside `columnvalues`/`oldkeys` together with `kind`.

    Every message is keyed `"<database>.<table>"`, so all changes to one table land on
    one partition and stay in binlog order. The key is not the row identity.

    Example Usage:

    ```python
    from quixstreams import Application
    from quixstreams.sources.community.mysql_cdc import MySqlCdcSource


    source = MySqlCdcSource(
        host="localhost",
        port=3306,
        user="cdc_user",
        password="cdc_password",
        database="test_database",
        table="test_table",
        initial_snapshot=True,
    )

    app = Application(
        broker_address="localhost:9092",
        consumer_group="mysql-cdc",
    )

    sdf = app.dataframe(source=source).print(metadata=True)
    # YOUR LOGIC HERE!

    if __name__ == "__main__":
        app.run()
    ```
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        table: str,
        port: int = 3306,
        server_id: Optional[int] = None,
        initial_snapshot: bool = False,
        snapshot_host: Optional[str] = None,
        snapshot_batch_size: int = 1000,
        force_snapshot: bool = False,
        commit_interval: float = 5.0,
        max_buffer_size: int = 1000,
        poll_interval: float = 0.1,
        retry_backoff_secs: float = 5.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param host: MySQL server hostname to replicate from.
        :param user: MySQL username. Needs REPLICATION SLAVE and REPLICATION CLIENT,
            plus SELECT on the table when the initial snapshot is enabled.
        :param password: MySQL password.
        :param database: database (schema) containing the table.
        :param table: table to stream changes from.
        :param port: MySQL server port.
            Default - `3306`.
        :param server_id: replication client id announced to MySQL. Must be unique
            across every replica and CDC client connected to the same server.
            Default - `None`, meaning a value derived from `name`, `database` and
            `table` in the range [1000, 2**31-1].
        :param initial_snapshot: snapshot the table's current contents before
            streaming changes. Requires the table to have a PRIMARY KEY.
            Default - `False`.
        :param snapshot_host: read the initial snapshot from this host instead of
            `host`, typically a read replica. The binlog stream still runs against
            `host`, starting from the coordinates that replica has already executed.
            Default - `None` (snapshot from `host`).
        :param snapshot_batch_size: rows per snapshot page.
            Default - `1000`.
        :param force_snapshot: re-run the initial snapshot even if one has already
            completed. This is static configuration, so it re-snapshots on every
            restart until it is turned off.
            Default - `False`.
        :param commit_interval: how often (seconds) to produce the buffered changes and
            commit the binlog position they cover.
            Default - `5.0`.
        :param max_buffer_size: commit early once this many changes are buffered, which
            bounds memory while catching up after downtime.
            Default - `1000`.
        :param poll_interval: how long (seconds) to idle when the binlog stream had
            nothing to read.
            Default - `0.1`.
        :param retry_backoff_secs: maximum backoff (seconds) between attempts to
            rebuild the binlog stream after a connection failure.
            Default - `5.0`.
        :param name: the source unique name. It is used to generate the default topic
            name, the state store name and the derived `server_id`; renaming a source
            therefore resets its committed position.
            Default - `mysql_cdc_<database>_<table>`.
        :param shutdown_timeout: Time in second the application waits for the source to
            gracefully shutdown.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        source_name = name or f"mysql_cdc_{database}_{table}"
        super().__init__(
            name=source_name,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        if server_id is not None and not 1 <= server_id <= _SERVER_ID_LIMIT:
            raise MySqlCdcError(
                f"server_id must be between 1 and {_SERVER_ID_LIMIT}, got {server_id}"
            )
        if snapshot_batch_size < 1:
            raise MySqlCdcError(
                f"snapshot_batch_size must be at least 1, got {snapshot_batch_size}"
            )
        if max_buffer_size < 1:
            raise MySqlCdcError(
                f"max_buffer_size must be at least 1, got {max_buffer_size}"
            )

        self._database = database
        self._table = table
        self._table_name = f"{database}.{table}"
        self._server_id = (
            server_id
            if server_id is not None
            else _derive_server_id(source_name, database, table)
        )

        self._helper = MySqlHelper(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            table=table,
            snapshot_host=snapshot_host or host,
        )

        self._initial_snapshot = initial_snapshot
        self._snapshot_batch_size = snapshot_batch_size
        self._force_snapshot = force_snapshot
        self._commit_interval = commit_interval
        self._max_buffer_size = max_buffer_size
        self._poll_interval = poll_interval
        self._retry_backoff_secs = retry_backoff_secs

        # State keys stay qualified by database and table so that reusing a source name
        # for a different table starts clean instead of resuming a foreign position.
        self._position_key = f"binlog_position_{database}_{table}"
        self._snapshot_completed_key = f"snapshot_completed_{database}_{table}"
        self._snapshot_progress_key = f"snapshot_progress_{database}_{table}"

        self._buffer: List[Dict[str, Any]] = []
        self._pending_position: Optional[Tuple[str, int]] = None
        self._committed_position: Optional[Tuple[str, int]] = None
        self._last_commit_at = time.monotonic()
        self._stream: Any = None

    def setup(self) -> None:
        """
        Validate the MySQL server and the table before the source starts.

        Failures propagate to `BaseSource._init_client`, which routes them to the
        client-connect failure callback; there is deliberately no logging here, or
        every failure would be reported twice.
        """
        self._helper.validate_server_config(require_primary_key=self._initial_snapshot)
        logger.info(
            "MySQL CDC source %s will replicate %s as server_id=%s",
            self.name,
            self._table_name,
            self._server_id,
        )

    def run(self) -> None:
        """
        Resolve the starting position, run the snapshot if needed, then stream changes.

        The order matters: the position is resolved and committed *before* any snapshot
        row is read, so every change made while the snapshot runs is still ahead of the
        stream. Overlap produces duplicates; a gap is impossible.
        """
        logger.info("Starting MySQL CDC source for %s", self._table_name)
        try:
            snapshot_needed = self._is_snapshot_needed()
            log_file, log_pos = self._resolve_start_position(snapshot_needed)

            if snapshot_needed:
                self._run_initial_snapshot()
                if not self.running:
                    logger.info(
                        "Stopped during the initial snapshot of %s; it will resume on "
                        "the next start",
                        self._table_name,
                    )
                    return

            self._stream = self._helper.create_binlog_stream(
                server_id=self._server_id, log_file=log_file, log_pos=log_pos
            )
            logger.info(
                "Streaming the MySQL binlog for %s from %s:%s",
                self._table_name,
                log_file,
                log_pos,
            )

            self._stream_changes()

            # Unconditional final drain: no interval gate, no "only if the buffer is
            # non-empty" check. It runs only after a clean loop exit, and if it raises
            # the error propagates - a swallowed shutdown flush is indistinguishable
            # from losing the batch.
            self._commit_batch(timeout=self.shutdown_timeout / 4)
        finally:
            self._close_stream()

    def stop(self) -> None:
        """
        Ask the run loop to finish.

        This is called from the subprocess's signal handler, so it must not touch the
        producer, the state store or the MySQL connection: `run()` may be in the middle
        of using all three. It only flips the `running` flag; the final drain and the
        cleanup happen at the end of `run()`.
        """
        logger.info("Stopping MySQL CDC source for %s", self._table_name)
        super().stop()

    # ------------------------------------------------------------------- start-up

    def _is_snapshot_needed(self) -> bool:
        if not self._initial_snapshot:
            logger.info(
                "Initial snapshot is disabled for %s - streaming binlog changes only",
                self._table_name,
            )
            return False
        if self._force_snapshot:
            logger.info(
                "force_snapshot is set - re-running the initial snapshot of %s",
                self._table_name,
            )
            return True
        if self.state.get(self._snapshot_completed_key):
            logger.info(
                "Initial snapshot of %s already completed - skipping", self._table_name
            )
            return False
        return True

    def _resolve_start_position(self, snapshot_needed: bool) -> Tuple[str, int]:
        """
        Return the binlog coordinates to stream from, anchoring them on a cold start.

        A stored position always wins. Otherwise the coordinates are taken from the
        host the snapshot will be read from (`fetch_snapshot_start_position`) and
        committed immediately: committing an anchor before anything has been produced
        can only cause a replay, never a loss, and without it a crash before the first
        event would resume from "now" and drop the whole window.
        """
        stored = self.state.get(self._position_key)
        if stored is not None:
            position = (str(stored["log_file"]), int(stored["log_pos"]))
            logger.info(
                "Resuming %s from the committed binlog position %s:%s",
                self._table_name,
                position[0],
                position[1],
            )
            self._committed_position = position
            return position

        position = (
            self._helper.fetch_snapshot_start_position()
            if snapshot_needed
            else self._helper.fetch_start_position()
        )
        self.state.set(self._position_key, self._position_value(position))
        self.flush()
        self._committed_position = position
        logger.info(
            "Anchored the binlog position for %s at %s:%s",
            self._table_name,
            position[0],
            position[1],
        )
        return position

    @staticmethod
    def _position_value(position: Tuple[str, int]) -> Dict[str, Any]:
        return {
            "log_file": position[0],
            "log_pos": position[1],
            "committed_at": time.time(),
        }

    # ------------------------------------------------------------------- snapshot

    def _run_initial_snapshot(self) -> None:
        """
        Produce the table's current contents, one keyset page at a time.

        Each page is produced, flushed and only then checkpointed, so an interrupted
        snapshot resumes at the last committed key instead of replaying the whole
        table. Rows changed while this runs are emitted twice - once here as
        `snapshot_insert` and again as a binlog `insert`/`update` - which is the
        duplication the at-least-once guarantee allows.
        """
        start_after = None
        rows_produced = 0

        if self._force_snapshot:
            state = self.state
            state.delete(self._snapshot_completed_key)
            state.delete(self._snapshot_progress_key)
            self.flush()
        else:
            progress = self.state.get(self._snapshot_progress_key)
            if progress:
                start_after = list(progress["last_key"])
                rows_produced = int(progress.get("rows", 0))
                logger.info(
                    "Resuming the initial snapshot of %s after key %s (%s rows "
                    "already produced)",
                    self._table_name,
                    start_after,
                    rows_produced,
                )

        batches = self._helper.perform_initial_snapshot(
            batch_size=self._snapshot_batch_size, start_after=start_after
        )
        checkpointing_warned = False
        with contextlib.closing(batches) as pages:
            for changes, last_key in pages:
                for change in changes:
                    msg = self.serialize(key=self._table_name, value=change)
                    self.produce(key=msg.key, value=msg.value)
                self.flush()
                rows_produced += len(changes)

                if is_checkpointable_key(last_key):
                    self.state.set(
                        self._snapshot_progress_key,
                        {
                            "last_key": list(last_key),
                            "rows": rows_produced,
                            "updated_at": time.time(),
                        },
                    )
                    self.flush()
                elif not checkpointing_warned:
                    checkpointing_warned = True
                    logger.info(
                        "Primary key of %s is not an int/str combination, so snapshot "
                        "progress cannot be stored as JSON without changing how it "
                        "compares in SQL; an interrupted snapshot will restart from "
                        "the beginning",
                        self._table_name,
                    )

                if not self.running:
                    return

        self.state.set(
            self._snapshot_completed_key,
            {"completed_at": time.time(), "rows": rows_produced},
        )
        self.state.delete(self._snapshot_progress_key)
        self.flush()
        logger.info(
            "Initial snapshot of %s completed - %s rows produced",
            self._table_name,
            rows_produced,
        )

    # --------------------------------------------------------------------- stream

    def _stream_changes(self) -> None:
        """
        Read and commit binlog changes until the source is asked to stop.

        Only connection errors are retried, and only a bounded number of times: a
        blanket retry would hide the errors that actually need a human (a revoked
        grant, a purged binlog position) behind an endless loop. Everything else
        propagates, the process exits non-zero, and the platform restarts it from the
        last committed position.
        """
        failures = 0
        while self.running:
            try:
                self._poll_once()
            except Exception as exc:
                if not is_connection_error(exc):
                    raise
                failures += 1
                if failures >= _MAX_RECONNECT_ATTEMPTS:
                    logger.error(
                        "The MySQL connection for %s failed %s times in a row; giving "
                        "up so the last committed position is replayed on restart",
                        self._table_name,
                        failures,
                    )
                    raise
                backoff = min(self._retry_backoff_secs, 2.0 ** (failures - 1))
                logger.warning(
                    "Lost the MySQL connection for %s (%s); reconnecting in %.1fs "
                    "(attempt %s/%s)",
                    self._table_name,
                    exc,
                    backoff,
                    failures,
                    _MAX_RECONNECT_ATTEMPTS,
                )
                self._sleep(backoff)
                if self.running:
                    self._reconnect_stream()
            else:
                failures = 0

    def _poll_once(self) -> None:
        changes, position = self._helper.read_changes(
            self._stream, max_rows=self._max_buffer_size
        )
        if changes:
            self._buffer.extend(changes)
        if position is not None and position != self._committed_position:
            self._pending_position = position

        if self._should_commit():
            self._commit_batch()
        elif not changes:
            # Idle only when the stream had nothing. Sleeping after a full batch would
            # cap throughput at max_buffer_size / poll_interval.
            self._sleep(self._poll_interval)

    def _should_commit(self) -> bool:
        if not self._buffer and self._pending_position is None:
            return False
        if len(self._buffer) >= self._max_buffer_size:
            return True
        return time.monotonic() - self._last_commit_at >= self._commit_interval

    def _commit_batch(self, timeout: Optional[float] = None) -> None:
        """
        Produce the buffered changes, then commit the position they cover.

        The two flushes are the point of this method. `StatefulSource.flush()`
        publishes the state changelog message through the same producer as the data,
        and librdkafka gives no cross-topic delivery-order guarantee, so a single flush
        could land the new position while the data it covers is still queued - the
        exact loss this connector had. Producing the position only after the data flush
        has returned removes that window: a crash can replay a batch, never skip one.

        :param timeout: producer flush timeout (seconds) passed to both flushes.
        """
        if not self._buffer and self._pending_position is None:
            self._last_commit_at = time.monotonic()
            return

        for change in self._buffer:
            msg = self.serialize(key=self._table_name, value=change)
            self.produce(key=msg.key, value=msg.value)

        self.flush(timeout)
        produced = len(self._buffer)
        self._buffer.clear()

        if self._pending_position is not None:
            # `self.state` is invalidated by every flush(), so it is read again here
            # rather than cached anywhere.
            self.state.set(
                self._position_key, self._position_value(self._pending_position)
            )
            self.flush(timeout)
            self._committed_position = self._pending_position
            self._pending_position = None

        self._last_commit_at = time.monotonic()
        logger.debug(
            "Committed %s change(s) for %s at %s",
            produced,
            self._table_name,
            self._committed_position,
        )

    def _reconnect_stream(self) -> None:
        """
        Rebuild the binlog stream at the last committed position.

        Everything read since that commit is dropped: the server will send those events
        again from the committed position, so keeping the buffer would only duplicate
        them, and committing `_pending_position` without producing the buffer would
        skip them entirely.
        """
        self._close_stream()
        self._buffer.clear()
        self._pending_position = None

        if self._committed_position is None:
            raise MySqlCdcError(
                "Cannot reconnect the binlog stream: no position has been committed "
                f"for {self._table_name}"
            )
        log_file, log_pos = self._committed_position
        self._stream = self._helper.create_binlog_stream(
            server_id=self._server_id, log_file=log_file, log_pos=log_pos
        )
        logger.info(
            "Reconnected the binlog stream for %s at %s:%s",
            self._table_name,
            log_file,
            log_pos,
        )

    def _close_stream(self) -> None:
        if self._stream is None:
            return
        stream, self._stream = self._stream, None
        try:
            stream.close()
        except Exception:
            # Swallowing is correct here only because this runs in `run()`'s finally
            # block: a failure closing an already-broken socket would otherwise mask
            # the exception that brought the source down. It is logged with its
            # traceback, so nothing is hidden.
            logger.warning(
                "Error while closing the binlog stream for %s",
                self._table_name,
                exc_info=True,
            )

    def _sleep(self, duration: float) -> None:
        """Sleep in short slices so a stop() during the wait is noticed promptly."""
        deadline = time.monotonic() + duration
        while self.running:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.1, remaining))
