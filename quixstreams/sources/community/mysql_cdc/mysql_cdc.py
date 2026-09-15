import contextlib
import logging
import time
import zlib
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from quixstreams.sources.base import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    StatefulSource,
)

from .config import MySqlCdcError, TlsConfig, require_positive
from .mysql_helper import BinlogErrorKind, MySqlHelper, classify_error
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

# A second, longer bound that one successful poll cannot clear. Consecutive-failure
# counting is right for backoff and wrong for diagnosis: a source that reconnects every
# few seconds forever looks healthy to it, because every reconnect is followed by a
# poll that works. This one counts reconnects in a sliding window instead.
_RECONNECT_WINDOW_SECS = 600.0
_MAX_RECONNECTS_PER_WINDOW = 20

# Collisions get their own, much tighter bound. A `server_id` collision during a rolling
# redeploy clears within seconds, when the outgoing process exits; a collision caused by
# a second CDC client configured with the same id never clears, and every reconnect in
# between evicts the other client in turn.
_MAX_COLLISIONS = 3

# Ceiling for the idle backoff, so the first event after a quiet period is never delayed
# by more than a second (or the commit cadence, if that is shorter).
_IDLE_INTERVAL_CAP = 1.0


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

    Run it with **exactly one replica**. The replication client id MySQL requires is
    derived from `name`, `database` and `table`, so every replica of one deployment
    derives the same id and MySQL evicts them in turn; the source reports that and exits
    rather than reconnecting forever.

    The server must have `binlog_format=ROW` and `binlog_row_metadata=FULL`. The second
    is not the MySQL default and is checked at start-up: without it the binlog carries no
    column names, no ENUM/SET values, no character sets and no integer signedness, which
    cannot be recovered client-side. `allow_minimal_row_metadata=True` is the escape
    hatch for a MySQL 5.7 server or a text-only table; read its `:param:` before using
    it. The connection is TLS-encrypted by default; see the `tls_*` parameters.

    A given MySQL value is encoded identically whether it came from the snapshot or from
    the binlog - `values.py` holds that contract, including JSON as canonical JSON text,
    SET as a sorted comma-joined string, BIT as a bit string and TIMESTAMP in UTC.

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
        tls_cert: Optional[str] = None,
        tls_key: Optional[str] = None,
        tls_verify_cert: Optional[bool] = None,
        tls_verify_identity: bool = False,
        allow_minimal_row_metadata: bool = False,
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param host: MySQL server hostname to replicate from.
        :param user: MySQL username. Needs REPLICATION SLAVE, REPLICATION CLIENT and
            SELECT on the table - SELECT is required whether or not the initial
            snapshot is enabled, because MySQL refuses the connection to `database`
            without it.
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
            completed, and re-anchor the binlog position along with it - which makes it
            the supported recovery from a position the server has purged. Requires
            `initial_snapshot=True`. This is static configuration, so it re-snapshots on
            every restart until it is turned off.
            Default - `False`.
        :param commit_interval: how often (seconds) to produce the buffered changes and
            commit the binlog position they cover.
            Default - `5.0`.
        :param max_buffer_size: commit early once this many changes are buffered, which
            bounds memory while catching up after downtime.
            Default - `1000`.
        :param poll_interval: how long (seconds) to idle when the binlog stream had
            nothing to read. Consecutive empty polls double this, up to one second (or
            `commit_interval` if that is shorter), and the first change resets it; a
            quiet table therefore costs a handful of connections per minute instead of
            twenty per second, at the price of up to a second of extra latency on the
            first event after a quiet period. Must be greater than 0.
            Default - `0.1`.
        :param retry_backoff_secs: maximum backoff (seconds) between attempts to
            rebuild the binlog stream after a connection failure. Must be greater than 0.
            Default - `5.0`.
        :param tls_enabled: require an encrypted connection to MySQL. `False` connects
            in plaintext, which is only appropriate on a trusted network.
            Default - `True`.
        :param tls_ca: path to a PEM CA bundle. Giving one turns server-certificate
            verification on; without it the connection is encrypted but the server is
            not authenticated.
            Default - `None`.
        :param tls_cert: path to a PEM client certificate, for mutual TLS.
            Default - `None`.
        :param tls_key: path to the PEM private key for `tls_cert`.
            Default - `None`.
        :param tls_verify_cert: verify the server certificate. `None` means "verify if
            `tls_ca` was given"; `True` without `tls_ca` is rejected.
            Default - `None`.
        :param tls_verify_identity: also check that the certificate matches `host`.
            Requires verification to be on.
            Default - `False`.
        :param allow_minimal_row_metadata: start even when the server cannot provide
            `binlog_row_metadata=FULL`, which MySQL 5.7 cannot at all and stock MySQL
            8.x does not by default. Only safe for a table with no ENUM, SET or UNSIGNED
            columns and no column whose bytes are not UTF-8: without FULL metadata ENUM
            and SET values arrive as `None`, UNSIGNED integers decode as signed
            (`INT UNSIGNED 4294967295` arrives as `-1`), and a non-UTF-8 column raises
            `UnicodeDecodeError` and stops the source. The source logs a WARNING naming
            all three on every start.
            Default - `False` (refuse to start).
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
        # Every one of these is a divisor or a sleep duration somewhere below;
        # poll_interval=0 in particular turns the read loop into a hot loop, so it is
        # rejected rather than clamped to something the user did not ask for.
        require_positive("commit_interval", commit_interval)
        require_positive("poll_interval", poll_interval)
        require_positive("retry_backoff_secs", retry_backoff_secs)
        require_positive("shutdown_timeout", shutdown_timeout)
        if force_snapshot and not initial_snapshot:
            raise MySqlCdcError(
                "force_snapshot=True requires initial_snapshot=True: there is no "
                "snapshot to force. Enable initial_snapshot, or drop force_snapshot."
            )

        tls = TlsConfig(
            enabled=tls_enabled,
            ca=tls_ca,
            cert=tls_cert,
            key=tls_key,
            verify_cert=tls_verify_cert,
            verify_identity=tls_verify_identity,
        )
        tls.validate()

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
            allow_minimal_row_metadata=allow_minimal_row_metadata,
        )
        self._tls = tls

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

        self._idle_interval = poll_interval
        self._idle_interval_cap = min(_IDLE_INTERVAL_CAP, commit_interval)
        self._reconnects: Deque[float] = deque()
        self._collisions = 0

    def setup(self) -> None:
        """
        Validate the MySQL server and the table before the source starts.

        Failures propagate to `BaseSource._init_client`, which routes them to the
        client-connect failure callback; there is deliberately no logging here, or
        every failure would be reported twice.
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

        A stored position normally wins. The exception is a forced snapshot, which
        discards it: the whole table is about to be republished, so resuming the old
        position would republish the table *and* keep failing on a position the server
        may no longer hold. That combination is what made `force_snapshot=True` useless
        as the documented recovery from a purged binlog.

        Otherwise the coordinates are taken from the host the snapshot will be read from
        (`fetch_snapshot_start_position`) and committed immediately: committing an anchor
        before anything has been produced can only cause a replay, never a loss, and
        without it a crash before the first event would resume from "now" and drop the
        whole window.
        """
        if self._force_snapshot and snapshot_needed:
            self._reset_snapshot_state()
        else:
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

    def _reset_snapshot_state(self) -> None:
        """
        Clear all three state keys before a forced snapshot reads anything.

        All three, in one place, before the new anchor is written. Deleting the two
        snapshot keys inside `_run_initial_snapshot()` - where this used to live - would
        run *after* the anchor had been committed, and leaving the position key alone
        was the actual defect: the source republished the whole table on every restart
        and then died on the same unreachable position.
        """
        state = self.state
        state.delete(self._position_key)
        state.delete(self._snapshot_completed_key)
        state.delete(self._snapshot_progress_key)
        self.flush()
        self._committed_position = None
        logger.info(
            "force_snapshot is set - discarded the stored position and snapshot "
            "progress for %s; both are about to be re-anchored",
            self._table_name,
        )

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

        # No force_snapshot branch here: `_resolve_start_position()` has already cleared
        # every state key this method could read, before the position was re-anchored.
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

        Failures are classified, not lumped together, because "retry it" is the wrong
        answer to three of the four kinds. A purged position and a fatal error propagate
        immediately; a `server_id` collision is retried a few times (a rolling redeploy
        clears one within seconds) and then reported for what it is; only a genuine
        connection failure gets the backoff loop. Everything that propagates exits the
        process non-zero, and the platform restarts it from the last committed position.

        Three bounds run at once, and they are not redundant. `failures` is consecutive
        and drives the backoff. `_collisions` is consecutive and much tighter, because a
        collision that repeats is a configuration error rather than a hiccup.
        `_reconnects` is a sliding window that a successful poll cannot clear, which is
        what catches a source reconnecting forever at a comfortable rate.
        """
        failures = 0
        while self.running:
            try:
                self._poll_once()
            except Exception as exc:
                kind = classify_error(exc)
                if kind is BinlogErrorKind.PURGED:
                    raise self._purged_position_error() from exc
                if kind is BinlogErrorKind.FATAL:
                    raise
                if kind is BinlogErrorKind.COLLISION:
                    self._note_collision(exc)
                else:
                    self._collisions = 0

                failures += 1
                if failures >= _MAX_RECONNECT_ATTEMPTS:
                    logger.error(
                        "The MySQL connection for %s failed %s times in a row; giving "
                        "up so the last committed position is replayed on restart",
                        self._table_name,
                        failures,
                    )
                    raise
                self._note_reconnect()
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

    def _note_collision(self, exc: BaseException) -> None:
        """
        Count a `server_id` collision and give up once they stop being transient.

        Logged at ERROR every time, not just at the bound: a collision means two clients
        are evicting each other, so events are being read twice and the position of each
        is being overwritten by the other. That is worth saying out loud on the first
        occurrence, even if it turns out to be a redeploy that clears itself.
        """
        self._collisions += 1
        logger.error(
            "MySQL evicted the binlog stream for %s because another client announced "
            "server_id=%s (%s). Likely causes: a second replica of this deployment "
            "(this source supports exactly one), another CDC deployment with the same "
            "name/database/table, or an overlapping rolling deploy. Collision %s of %s.",
            self._table_name,
            self._server_id,
            exc,
            self._collisions,
            _MAX_COLLISIONS,
        )
        if self._collisions >= _MAX_COLLISIONS:
            raise MySqlCdcError(
                f"Giving up: server_id={self._server_id} collided {self._collisions} "
                f"times while streaming {self._table_name}. Another replication client "
                "is using the same id, and the two are evicting each other rather than "
                "either making progress. Run this source with exactly one replica, and "
                "give a second deployment against this server a distinct name or an "
                "explicit server_id."
            ) from exc

    def _note_reconnect(self) -> None:
        """
        Record a reconnect and fail if they are piling up over the window.

        This is the bound `failures` cannot provide: `failures` resets to 0 after every
        successful poll, so a source that reconnects every few seconds forever never
        reaches its limit. A deque trimmed to the window does, whatever happens in
        between.
        """
        now = time.monotonic()
        self._reconnects.append(now)
        while self._reconnects and now - self._reconnects[0] > _RECONNECT_WINDOW_SECS:
            self._reconnects.popleft()
        if len(self._reconnects) > _MAX_RECONNECTS_PER_WINDOW:
            raise MySqlCdcError(
                f"The binlog stream for {self._table_name} has been rebuilt "
                f"{len(self._reconnects)} times in the last "
                f"{_RECONNECT_WINDOW_SECS:.0f}s. Individual reconnects kept succeeding, "
                "so the per-attempt limit never tripped, but a source that reconnects "
                "this often is not streaming - check the server's error log, the "
                "network, and whether another client is using server_id="
                f"{self._server_id}."
            )

    def _purged_position_error(self) -> MySqlCdcError:
        """The recovery text for a committed position the server no longer holds."""
        return MySqlCdcError(
            f"MySQL no longer holds the binlog position committed for "
            f"{self._table_name} ({self._committed_position}): the file has been purged. "
            "There is no gap-free recovery from this - the changes in between are gone "
            "from the server. To resume with a full re-read of the table, restart the "
            "source with initial_snapshot=True and force_snapshot=True, which "
            "republishes every row and re-anchors the position, then turn force_snapshot "
            "off again. To prevent a recurrence, raise binlog_expire_logs_seconds on the "
            "server so it exceeds the longest downtime you expect."
        )

    def _poll_once(self) -> None:
        changes, position = self._helper.read_changes(
            self._stream,
            max_rows=self._max_buffer_size,
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
        elif not changes:
            # Idle only when the stream had nothing. Sleeping after a full batch would
            # cap throughput at max_buffer_size / poll_interval.
            #
            # The interval doubles on every empty poll because a non-blocking dump ends
            # with an EOF packet, on which `BinLogStreamReader` closes the stream *and*
            # the control connection (`binlogstream.py:627-629`) and reopens both on the
            # next read - so a quiet table at poll_interval=0.1 costs ~20 connections and
            # ~20 authentications per second, each of them a TLS handshake. Any change
            # resets it, so latency under load is unaffected.
            self._sleep(self._idle_interval)
            self._idle_interval = min(self._idle_interval * 2, self._idle_interval_cap)

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

        `binlog_row_metadata` is re-checked first. It is a dynamic global, so it can be
        lowered to MINIMAL under a running source, and this is the only path cheap enough
        to check it on - one extra query on a path that only runs after something has
        already gone wrong.
        """
        self._helper.require_row_metadata()
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
