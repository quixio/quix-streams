import contextlib
import logging
import time
import zlib
from typing import Any, Dict, List, Optional, Tuple

from quixstreams.models.topics import Topic
from quixstreams.sources.base import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    StatefulSource,
)

from .config import ConnectionTimeouts, MySqlCdcError, TlsConfig, require_positive
from .failures import BinlogErrorKind, ReconnectPolicy, classify_error
from .mysql_helper import MySqlHelper
from .progress import SourceProgress
from .reader import BinlogReader
from .retention import SnapshotAnchor

__all__ = ("MySqlCdcError", "MySqlCdcSource")

logger = logging.getLogger(__name__)

# Derived ids start at 1000 to stay clear of the hand-written `server-id = 1` a MySQL
# server and its tutorials use.
_SERVER_ID_MIN = 1000
_SERVER_ID_MAX = 2**31 - 1

# MySQL accepts any 32-bit unsigned value as an explicit server id.
_SERVER_ID_LIMIT = 2**32 - 1


def _derive_server_id(name: str, database: str, table: str) -> int:
    """
    Derive a stable replication client id from the source's identity.

    :return: an id in [_SERVER_ID_MIN, _SERVER_ID_MAX], a pure function of the inputs.
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

    Every message is keyed with the string `"<database>.<table>"`, so all changes to one
    table land on one partition and stay in binlog order. The key is not the row
    identity.

    Run it with **exactly one replica**: the replication client id is derived from
    `name`, `database` and `table`, so every replica of one deployment derives the same
    id and MySQL evicts them in turn.

    Supported servers are MySQL 8.0, 8.4 and 9.x. `log_bin` must be on and
    `binlog_format` must be `ROW`. `binlog_row_metadata` ships as `MINIMAL` and is
    raised to `FULL` by the source itself at start-up, which needs
    SYSTEM_VARIABLES_ADMIN; `binlog_row_image` already ships `FULL` and is raised only
    where it has been lowered. Writers connected before that keep writing partial row
    images until they reconnect, because `binlog_row_image` has session scope; the
    source stops on the first such event instead of publishing a change with columns
    missing. The account needs SELECT on the whole table, not on some of its columns.
    The connection is TLS-encrypted by default.

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
        tls_enabled: bool = True,
        tls_ca: Optional[str] = None,
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param host: MySQL server hostname to replicate from.
        :param user: MySQL username. Needs REPLICATION SLAVE, REPLICATION CLIENT and
            SELECT on the whole table - SELECT is required whether or not the initial
            snapshot is enabled, and a column-level grant is rejected at start-up
            because it hides the rest of the table's columns from the snapshot. It also
            needs SYSTEM_VARIABLES_ADMIN unless the server already has
            `binlog_row_metadata` and `binlog_row_image` set to FULL.
        :param password: MySQL password.
        :param database: database (schema) containing the table.
        :param table: table to stream changes from.
        :param port: MySQL server port.
            Default - `3306`.
        :param server_id: replication client id announced to MySQL. Must be unique
            across every replica and CDC client connected to the same server. The
            derived default is a pure function of `name`, `database` and `table`, so it
            is identical across replicas of one deployment: run this source with exactly
            one replica, and give two deployments against the same server distinct
            `name` values (or distinct `server_id` values).
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
            completed, re-anchoring the binlog position with it. A forced snapshot that
            is interrupted is continued on the next start, at the position it first
            anchored, rather than started again. Requires `initial_snapshot=True`. This
            is static configuration, so it re-snapshots on every restart until it is
            turned off.
            Default - `False`.
        :param commit_interval: how often (seconds) to produce the buffered changes and
            commit the binlog position they cover.
            Default - `5.0`.
        :param max_buffer_size: commit early once this many changes are buffered, which
            bounds memory while catching up after downtime. A binlog event carrying more
            rows than the remaining room is split across reads rather than buffered
            whole.
            Default - `1000`.
        :param poll_interval: how long (seconds) to idle when the binlog stream had
            nothing to read. Consecutive empty polls double this, up to
            `commit_interval`, and the first change resets it. Must be greater than 0.
            Default - `0.1`.
        :param retry_backoff_secs: maximum backoff (seconds) between attempts to
            rebuild the binlog stream after a connection failure. Must be greater than 0.
            Default - `5.0`.
        :param tls_enabled: require an encrypted connection to MySQL. `False` connects
            in plaintext, which is only appropriate on a trusted network.
            Default - `True`.
        :param tls_ca: path to a PEM CA bundle. Giving one turns verification on - both
            the server's certificate chain and its hostname are then checked. Without it
            the connection is encrypted but the server is not authenticated.
            Default - `None`.
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
        :raises MySqlCdcError: for any invalid or contradictory configuration.
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
        require_positive("commit_interval", commit_interval)
        require_positive("poll_interval", poll_interval)
        require_positive("retry_backoff_secs", retry_backoff_secs)
        require_positive("shutdown_timeout", shutdown_timeout)
        if force_snapshot and not initial_snapshot:
            raise MySqlCdcError(
                "force_snapshot=True requires initial_snapshot=True: there is no "
                "snapshot to force. Enable initial_snapshot, or drop force_snapshot."
            )

        tls = TlsConfig(enabled=tls_enabled, ca=tls_ca)

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
            tls=tls,
            timeouts=ConnectionTimeouts.derive(
                commit_interval=commit_interval,
                retry_backoff_secs=retry_backoff_secs,
                shutdown_timeout=shutdown_timeout,
            ),
        )
        self._tls = tls

        self._initial_snapshot = initial_snapshot
        self._snapshot_batch_size = snapshot_batch_size
        self._force_snapshot = force_snapshot
        self._commit_interval = commit_interval
        self._max_buffer_size = max_buffer_size
        self._poll_interval = poll_interval

        self._progress = SourceProgress(self, database=database, table=table)

        self._buffer: List[Dict[str, Any]] = []
        self._pending_position: Optional[Tuple[str, int]] = None
        self._committed_position: Optional[Tuple[str, int]] = None
        self._last_commit_at = time.monotonic()
        self._stream: Optional[BinlogReader] = None

        self._idle_interval = poll_interval
        self._idle_interval_cap = commit_interval
        self._retries = ReconnectPolicy(
            table_name=self._table_name,
            server_id=self._server_id,
            max_backoff=retry_backoff_secs,
        )

    def default_topic(self) -> Topic:
        """
        :return: a topic named after the source, with string keys and JSON values.
        """
        return Topic(
            name=self.name,
            key_serializer="str",
            key_deserializer="str",
            value_serializer="json",
            value_deserializer="json",
        )

    def setup(self) -> None:
        """
        Validate the MySQL server and the table, and set the row-image globals.

        :raises MySqlCdcError: for anything that must hold before streaming and does not.
        """
        self._helper.validate_server_config(
            require_primary_key=self._initial_snapshot, server_id=self._server_id
        )
        logger.info(
            "MySQL CDC source %s will replicate %s as server_id=%s",
            self.name,
            self._table_name,
            self._server_id,
        )
        logger.info("%s", self._tls.describe())

    def run(self) -> None:
        """Resolve the starting position, snapshot if needed, then stream changes."""
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
            self._commit_batch(timeout=self.shutdown_timeout / 4)
        finally:
            self._close_stream()

    def stop(self) -> None:
        """
        Ask the run loop to finish.

        Called from the subprocess's signal handler, so it touches neither the producer,
        the state store nor the MySQL connection - `run()` may be using all three.
        """
        logger.info("Stopping MySQL CDC source for %s", self._table_name)
        super().stop()

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
        if self._progress.snapshot_completed():
            logger.info(
                "Initial snapshot of %s already completed - skipping", self._table_name
            )
            return False
        return True

    def _resolve_start_position(self, snapshot_needed: bool) -> Tuple[str, int]:
        """
        Return the binlog coordinates to stream from, committing them on a cold start.

        :param snapshot_needed: read the coordinates from the snapshot host instead.
        """
        stored = self._progress.position()
        if self._force_snapshot and not self._resuming_forced_snapshot(stored):
            self._discard_state(
                "force_snapshot is set; both are about to be re-anchored"
            )
            stored = None
        if stored is not None:
            logger.info(
                "Resuming %s from the committed binlog position %s:%s",
                self._table_name,
                stored[0],
                stored[1],
            )
            self._committed_position = stored
            return stored

        position = (
            self._helper.fetch_snapshot_start_position()
            if snapshot_needed
            else self._helper.fetch_start_position()
        )
        self._progress.store_position(position)
        self._committed_position = position
        logger.info(
            "Anchored the binlog position for %s at %s:%s",
            self._table_name,
            position[0],
            position[1],
        )
        return position

    def _resuming_forced_snapshot(self, stored: Optional[Tuple[str, int]]) -> bool:
        """
        Decide whether a forced snapshot continues the one a restart interrupted.

        Re-anchoring instead would move the position past the changes made to the pages
        already produced, and those changes would never reach the topic.

        :param stored: the committed position, or None if there is none.
        :return: True when the run resumes, having dropped only the completed marker.
        """
        if stored is None or not self._progress.has_snapshot_progress():
            return False
        self._progress.drop_snapshot_completed()
        logger.info(
            "force_snapshot is set and the previous snapshot of %s was interrupted, so "
            "it continues from its stored progress at the position it anchored (%s:%s) "
            "rather than starting again",
            self._table_name,
            stored[0],
            stored[1],
        )
        return True

    def _discard_state(self, reason: str) -> None:
        """
        Clear the position and both snapshot keys, so the next start is a cold one.

        :param reason: what made the stored state unusable, for the log line.
        """
        self._progress.discard(reason)
        self._committed_position = None

    def _run_initial_snapshot(self) -> None:
        """Produce the table's current contents, one keyset page at a time."""
        plan = self._helper.plan_snapshot()
        start_after, rows_produced = self._progress.resume_point(plan.pk_columns)
        anchor = SnapshotAnchor(
            helper=self._helper,
            table_name=self._table_name,
            position=self._committed_position,
            anchored_at=self._progress.anchored_at(),
        )

        batches = self._helper.perform_initial_snapshot(
            plan=plan,
            batch_size=self._snapshot_batch_size,
            start_after=start_after,
        )
        checkpointing_warned = False
        with contextlib.closing(batches) as pages:
            for changes, last_key in pages:
                for change in changes:
                    msg = self.serialize(key=self._table_name, value=change)
                    self.produce(key=msg.key, value=msg.value)
                self.flush()
                rows_produced += len(changes)

                checkpointed = self._progress.checkpoint(
                    last_key, plan.pk_columns, rows_produced
                )
                if not checkpointed and not checkpointing_warned:
                    checkpointing_warned = True
                    logger.info(
                        "The primary key of %s holds a value this source cannot store "
                        "as JSON, so snapshot progress is not checkpointed and an "
                        "interrupted snapshot will restart from the beginning",
                        self._table_name,
                    )

                if not self.running:
                    return
                self._check_snapshot_anchor(anchor, rows_produced, plan.estimated_rows)

        self._progress.mark_snapshot_completed(rows_produced)
        logger.info(
            "Initial snapshot of %s completed - %s rows produced",
            self._table_name,
            rows_produced,
        )

    def _check_snapshot_anchor(
        self, anchor: SnapshotAnchor, rows_produced: int, estimated_rows: Optional[int]
    ) -> None:
        """
        :raises MySqlCdcError: if the position the snapshot anchored has been purged,
            having first discarded the state that points at it.
        """
        try:
            anchor.check(rows_produced, estimated_rows)
        except MySqlCdcError:
            self._discard_state(
                "the binlog position the initial snapshot anchored has been purged, so "
                "the snapshot has to be taken again"
            )
            raise

    def _stream_changes(self) -> None:
        """
        Read and commit binlog changes until the source is asked to stop.

        :raises: whatever `classify_error()` calls PURGED or FATAL, and whatever the
            `ReconnectPolicy` raises once a bound trips.
        """
        reconnect_needed = False
        while self.running:
            try:
                if reconnect_needed:
                    self._reconnect_stream()
                    reconnect_needed = False
                self._poll_once()
            except Exception as exc:
                kind = classify_error(exc)
                if kind is BinlogErrorKind.PURGED:
                    raise self._purged_position_failure() from exc
                if kind is BinlogErrorKind.FATAL:
                    raise
                if kind is BinlogErrorKind.COLLISION:
                    self._retries.note_collision(exc)
                self._sleep(self._retries.note_failure(exc))
                reconnect_needed = True
            else:
                self._retries.note_success()

    def _purged_position_failure(self) -> MySqlCdcError:
        """
        Build the error for a position the server no longer holds, discarding the state
        that points at it when the source can re-read the table by itself.
        """
        position = self._committed_position
        if self._initial_snapshot:
            self._discard_state(
                "the committed binlog position has been purged, so the table has to be "
                "read again"
            )
            recovery = (
                "Its stored position and snapshot progress have been discarded, so "
                "starting the source again re-reads the table and re-anchors, with no "
                "configuration change and no force_snapshot."
            )
        else:
            recovery = (
                "The stored position has been kept, because with initial_snapshot=False "
                "there is nothing to re-read and discarding it would skip the gap "
                "silently. If the table has a PRIMARY KEY, restart the source with "
                "initial_snapshot=True and force_snapshot=True to re-read it and "
                "re-anchor the position, then turn both off again. If it has none there "
                "is no snapshot to take: add a primary key and do the above, or accept "
                "the gap and give the source a different `name`, which starts it from "
                "the server's current position with a state store of its own."
            )
        return MySqlCdcError(
            "MySQL no longer holds the binlog position committed for "
            f"{self._table_name} ({position}): the file has been purged, and the "
            f"changes it held are gone from the server. {recovery} Raise "
            "binlog_expire_logs_seconds so it exceeds the longest downtime you expect."
        )

    def _poll_once(self) -> None:
        changes, position = self._stream.read_changes(
            max_rows=max(1, self._max_buffer_size - len(self._buffer)),
            max_seconds=self._commit_interval,
            should_continue=lambda: self.running,
        )
        if changes:
            self._buffer.extend(changes)
            self._idle_interval = self._poll_interval
        if position is not None and position != self._committed_position:
            self._pending_position = position

        if self._should_commit():
            self._commit_batch()
            return

        # A drained non-blocking dump ends in an EOF packet, on which
        # `BinLogStreamReader.fetchone` closes both of its connections and reopens them
        # on the next read, so every poll past this point costs two connections.
        if self._buffer or self._pending_position is not None:
            self._sleep(self._commit_due_in())
            return
        self._sleep(self._idle_interval)
        self._idle_interval = min(self._idle_interval * 2, self._idle_interval_cap)

    def _commit_due_in(self) -> float:
        """:return: seconds until the buffered changes are due to be committed."""
        elapsed = time.monotonic() - self._last_commit_at
        return max(0.0, self._commit_interval - elapsed)

    def _should_commit(self) -> bool:
        if not self._buffer and self._pending_position is None:
            return False
        if len(self._buffer) >= self._max_buffer_size:
            return True
        return time.monotonic() - self._last_commit_at >= self._commit_interval

    def _commit_batch(self, timeout: Optional[float] = None) -> None:
        """
        Produce the buffered changes, flush, then commit the position they cover.

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
            self._progress.store_position(self._pending_position, timeout)
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
        Rebuild the stream at the last committed position, dropping everything read
        since that commit.
        """
        self._close_stream()
        self._buffer.clear()
        self._pending_position = None

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
        """Close the stream if there is one, logging rather than raising on failure."""
        if self._stream is None:
            return
        stream, self._stream = self._stream, None
        try:
            stream.close()
        except Exception:
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
