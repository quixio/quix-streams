"""
MySQL access layer for the CDC source.

Everything that talks to MySQL lives here: connections, server validation, binlog
coordinates, the binlog stream and the snapshot generator. Nothing in this module
persists anything - the binlog position is owned by `MySqlCdcSource` and stored in
its Quix Streams state store.
"""

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .snapshot import (
    discover_primary_key,
    estimate_row_count,
    iter_snapshot_batches,
    serialize_value,
)

try:
    import pymysql
    from pymysql.cursors import DictCursor
    from pymysql.err import (
        InterfaceError,
        MySQLError,
        OperationalError,
        ProgrammingError,
    )
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )
except ImportError as exc:
    raise ImportError(
        'Packages "pymysql" and "mysql-replication" are missing: '
        "run pip install quixstreams[mysql] to fix it"
    ) from exc


__all__ = ("MySqlCdcError", "MySqlHelper", "is_connection_error")

logger = logging.getLogger(__name__)

# MySQL renamed both status statements: `SHOW MASTER STATUS` became `SHOW BINARY LOG
# STATUS` in 8.4, and `SHOW SLAVE STATUS` became `SHOW REPLICA STATUS` in 8.0.22 (the
# old spelling survived until 8.4). Every spelling is tried in turn, which covers 5.7
# through 8.4; a spelling this server rejects is skipped, never substituted for a
# statement that means something else.
_BINLOG_STATUS_STATEMENTS = ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS")
_REPLICA_STATUS_STATEMENTS = ("SHOW REPLICA STATUS", "SHOW SLAVE STATUS")

# 1227 = ER_SPECIFIC_ACCESS_DENIED_ERROR: the user lacks REPLICATION CLIENT.
_ACCESS_DENIED_ERROR_CODE = 1227

# MySQL errors that arrive as `OperationalError` - the same class a dropped socket
# raises - but that no reconnect can clear, so retrying them only delays the failure
# and hides its cause. All three are credential or privilege failures: the reconnect
# presents exactly the same rejected identity, forever.
#   1044 ER_DBACCESS_DENIED_ERROR        - the grant on the database was revoked.
#   1045 ER_ACCESS_DENIED_ERROR          - wrong password, or the account was dropped.
#   1227 ER_SPECIFIC_ACCESS_DENIED_ERROR - the account lacks REPLICATION SLAVE/CLIENT.
#
# 1236 ER_MASTER_FATAL_ERROR_READING_BINLOG is deliberately NOT here. MySQL reuses it
# for two opposite situations: "Could not find first log file name in binary log index
# file", which is permanent, and "A replica with the same server_uuid/server_id as this
# replica has connected to the source", which is transient and is exactly what a
# redeploying source hits while the server still holds its previous connection. Marking
# 1236 fatal would kill a source that only needed to reconnect. The permanent case is
# still bounded and loud: it exhausts the five reconnect attempts and re-raises with
# MySQL's own message.
_FATAL_MYSQL_ERROR_CODES = frozenset({1044, 1045, _ACCESS_DENIED_ERROR_CODE})

_NO_PRIMARY_KEY_ERROR = (
    "Table {table_name} has no PRIMARY KEY. The initial snapshot requires one for "
    "stable pagination. Add a primary key, or set initial_snapshot=False to stream "
    "binlog changes only."
)


class MySqlCdcError(Exception):
    """Raised for MySQL configuration/validation problems the user must fix."""


def is_connection_error(exc: BaseException) -> bool:
    """
    True for failures a reconnect can plausibly clear.

    Used by the source to decide whether to rebuild the binlog stream or to let the
    error kill the process. A broken socket is retryable; anything else is fatal on
    purpose, because retrying it would hide it behind a reconnect loop.

    The exception class alone cannot decide this. pymysql raises `OperationalError`
    both for a dropped connection and for access-denied errors, so the MySQL error code
    is checked too: the codes in `_FATAL_MYSQL_ERROR_CODES` - a revoked grant, a bad
    password, a missing REPLICATION privilege - are reported as *not* retryable and
    propagate to kill the process.
    """
    if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
        return True
    if not isinstance(exc, (OperationalError, InterfaceError)):
        return False
    return _mysql_error_code(exc) not in _FATAL_MYSQL_ERROR_CODES


def _mysql_error_code(exc: BaseException) -> Optional[int]:
    """Return the MySQL error number a pymysql exception carries, or None."""
    code = exc.args[0] if exc.args else None
    return code if isinstance(code, int) else None


def _first_present(row: Dict[str, Any], *names: str) -> Any:
    """Return the first non-None value among `names` in a DictCursor row."""
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return None


class MySqlHelper:
    """
    Owns every MySQL connection the CDC source needs.

    Each method opens and closes its own short-lived connection. The two exceptions
    are `create_binlog_stream()`, whose returned reader owns its connections, and
    `perform_initial_snapshot()`, a generator that holds one connection open for the
    duration of the table walk.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table: str,
        snapshot_host: str,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._table = table
        self._table_name = f"{database}.{table}"
        self._snapshot_host = snapshot_host

    def connect_mysql(self, override_host: Optional[str] = None) -> Any:
        """Open a new connection to `host`, or to `override_host` when given."""
        return pymysql.connect(
            host=override_host or self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
        )

    # ------------------------------------------------------------------ validation

    def validate_server_config(self, require_primary_key: bool) -> None:
        """
        Check everything that must hold before streaming, failing loudly if it does not.

        :param require_primary_key: when True (the initial snapshot is enabled), also
            require the table to have a PRIMARY KEY to paginate on. Streaming-only
            mode does not need one.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                self._require_binlog_enabled(cursor)
                self._require_row_format(cursor)
                self._warn_on_row_image(cursor)
                self._require_table(cursor)
                if require_primary_key:
                    self.require_primary_key(cursor)
        finally:
            conn.close()

        if self._snapshot_host != self._host:
            # Fail here, on the source's connect path, rather than hours later when
            # the snapshot actually opens this connection.
            self.connect_mysql(override_host=self._snapshot_host).close()
            logger.info("Snapshot host %s is reachable", self._snapshot_host)

    def _require_binlog_enabled(self, cursor: Any) -> None:
        if self._show_variable(cursor, "log_bin") != "ON":
            raise MySqlCdcError(
                f"Binary logging is disabled on {self._host}. CDC requires it: set "
                "log_bin (e.g. log-bin=mysql-bin) in the MySQL configuration and "
                "restart the server."
            )

    def _require_row_format(self, cursor: Any) -> None:
        binlog_format = self._show_variable(cursor, "binlog_format")
        if binlog_format != "ROW":
            raise MySqlCdcError(
                f"binlog_format is {binlog_format!r} on {self._host}, but CDC requires "
                "'ROW'. Any other format carries statements instead of row images, so "
                "this source would stream a binlog it cannot read. Set "
                "binlog_format=ROW in the MySQL configuration."
            )

    def _warn_on_row_image(self, cursor: Any) -> None:
        row_image = self._show_variable(cursor, "binlog_row_image")
        if row_image is not None and row_image != "FULL":
            # Not fatal: MINIMAL still carries the primary key in the "before" image,
            # so change events remain usable, just with partial column sets.
            logger.warning(
                "binlog_row_image is %r on %s; 'FULL' is recommended so update and "
                "delete events carry every column instead of only the primary key",
                row_image,
                self._host,
            )

    def _require_table(self, cursor: Any) -> None:
        cursor.execute(
            "SELECT 1 FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (self._database, self._table),
        )
        if not cursor.fetchone():
            raise MySqlCdcError(
                f"Table {self._table_name} does not exist on {self._host}, or the "
                "configured user cannot see it."
            )

    def require_primary_key(self, cursor: Any) -> List[str]:
        """Return the table's PK columns in index order, raising if it has none."""
        pk_columns = discover_primary_key(cursor, self._database, self._table)
        if not pk_columns:
            raise MySqlCdcError(
                _NO_PRIMARY_KEY_ERROR.format(table_name=self._table_name)
            )
        return pk_columns

    @staticmethod
    def _show_variable(cursor: Any, name: str) -> Optional[str]:
        cursor.execute("SHOW VARIABLES LIKE %s", (name,))
        row = cursor.fetchone()
        return row[1] if row else None

    # ------------------------------------------------------------ binlog positions

    def fetch_start_position(self) -> Tuple[str, int]:
        """
        Return the current binlog coordinates of `host` as `(log_file, log_pos)`.

        Tries both spellings of the statement (see `_BINLOG_STATUS_STATEMENTS`) and
        raises if neither yields a position: the source must never guess a starting
        point, because guessing "now" silently drops everything before it.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                for statement in _BINLOG_STATUS_STATEMENTS:
                    row = self._run_status_statement(cursor, statement)
                    if row:
                        # Both spellings return File, Position as the first columns.
                        return str(row[0]), int(row[1])
        finally:
            conn.close()

        raise MySqlCdcError(
            f"Could not read the binary log position from {self._host}: none of "
            f"{', '.join(_BINLOG_STATUS_STATEMENTS)} returned a row. Binary logging "
            "must be enabled and the user needs the REPLICATION CLIENT privilege."
        )

    def fetch_replica_executed_position(self, host: str) -> Tuple[str, int]:
        """
        Return the primary coordinates a replica has already *executed*.

        Reads `Relay_Source_Log_File`/`Exec_Source_Log_Pos` (MySQL 8.0.22+), falling
        back to the legacy `Relay_Master_Log_File`/`Exec_Master_Log_Pos`. With parallel
        appliers `Exec_*` is a low-water mark, which errs towards replaying changes
        rather than skipping them.
        """
        conn = self.connect_mysql(override_host=host)
        try:
            with conn.cursor(DictCursor) as cursor:
                for statement in _REPLICA_STATUS_STATEMENTS:
                    row = self._run_status_statement(cursor, statement)
                    if not row:
                        continue
                    log_file = _first_present(
                        row, "Relay_Source_Log_File", "Relay_Master_Log_File"
                    )
                    log_pos = _first_present(
                        row, "Exec_Source_Log_Pos", "Exec_Master_Log_Pos"
                    )
                    # A zero Exec_* position means the applier has never run, so there
                    # is no coordinate to start from; fall through to the error below.
                    if log_file and log_pos:
                        return str(log_file), int(log_pos)
        finally:
            conn.close()

        raise MySqlCdcError(
            f"Could not read an executed replication position from snapshot host "
            f"{host}: none of {', '.join(_REPLICA_STATUS_STATEMENTS)} returned "
            "coordinates. Either that host is not a replica of "
            f"{self._host} - point snapshot_host at the primary instead - or the "
            "configured user lacks the REPLICATION CLIENT privilege on it. The "
            "source will not fall back to the primary's current position, because "
            "that would silently drop every change the replica has not applied yet."
        )

    def fetch_snapshot_start_position(self) -> Tuple[str, int]:
        """
        Return the coordinates on `host` to start the binlog stream from, given where
        the snapshot rows will be read.

        Same host: its current position. A separate snapshot host: the primary
        coordinates that replica has already executed. Rows read from the replica
        afterwards reflect a state at or after those coordinates, so streaming the
        primary from there can only duplicate changes, never miss them.
        """
        if self._snapshot_host == self._host:
            return self.fetch_start_position()
        return self.fetch_replica_executed_position(self._snapshot_host)

    @staticmethod
    def _run_status_statement(cursor: Any, statement: str) -> Any:
        """
        Execute a SHOW ... STATUS statement, returning its first row or None.

        None means "this spelling is unusable here", so the caller should try the next
        one and raise if they all come back empty. Two failures qualify: a
        `ProgrammingError`, which is what a server that does not know the statement
        answers, and an access-denied error, which is what a server that knows it but
        will not run it for this user answers. Every other MySQL error - a dropped
        connection above all - propagates, so a broken link is never mistaken for an
        unsupported statement.
        """
        try:
            cursor.execute(statement)
        except ProgrammingError as exc:
            logger.debug("%s is not available on this server: %s", statement, exc)
            return None
        except MySQLError as exc:
            code = exc.args[0] if exc.args else None
            if code != _ACCESS_DENIED_ERROR_CODE:
                raise
            logger.debug("%s is not permitted for this user: %s", statement, exc)
            return None
        return cursor.fetchone()

    # ----------------------------------------------------------------- binlog read

    def create_binlog_stream(
        self, server_id: int, log_file: str, log_pos: int
    ) -> BinLogStreamReader:
        """
        Open a binlog stream positioned at `log_file`:`log_pos`.

        `resume_stream=True` makes the server continue with the event *after*
        `log_pos` (which is the end position of the last processed event), so resuming
        does not duplicate the event at the boundary.
        """
        return BinLogStreamReader(
            connection_settings={
                "host": self._host,
                "port": self._port,
                "user": self._user,
                "passwd": self._password,
            },
            server_id=server_id,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            only_schemas=[self._database],
            only_tables=[self._table],
            resume_stream=True,
            blocking=False,
            log_file=log_file,
            log_pos=log_pos,
        )

    def read_changes(
        self, stream: BinLogStreamReader, max_rows: int
    ) -> Tuple[List[Dict[str, Any]], Optional[Tuple[str, int]]]:
        """
        Read up to `max_rows` row-changes and return them with the position they cover.

        The stream is non-blocking, so this returns as soon as the server has nothing
        more queued. The position is read once, after the loop: `BinLogStreamReader`
        advances `log_pos`/`log_file` for every packet it decodes, including events its
        own `only_events`/`only_tables` filters discard, so committing that position
        skips past uninteresting events instead of re-reading them after a restart.

        The returned position can be non-None alongside an empty change list - the
        normal case for a quiet table in a busy database - and committing it is safe
        precisely because those events were read and deliberately not emitted.
        """
        changes: List[Dict[str, Any]] = []
        for event in stream:
            # The library-side only_schemas/only_tables filter depends on
            # binlog_row_metadata being populated, so re-check here.
            if event.schema == self._database and event.table == self._table:
                changes.extend(self._event_to_changes(event))
            if len(changes) >= max_rows:
                break

        log_file, log_pos = stream.log_file, stream.log_pos
        position = (log_file, log_pos) if log_file and log_pos else None
        return changes, position

    def _event_to_changes(self, event: Any) -> List[Dict[str, Any]]:
        """Convert one row event into the connector's change dicts, one per row."""
        if isinstance(event, WriteRowsEvent):
            return [
                {
                    "kind": "insert",
                    "schema": event.schema,
                    "table": event.table,
                    "columnnames": list(row["values"].keys()),
                    "columnvalues": [
                        serialize_value(value) for value in row["values"].values()
                    ],
                    "oldkeys": {},
                }
                for row in event.rows
            ]

        if isinstance(event, UpdateRowsEvent):
            return [
                {
                    "kind": "update",
                    "schema": event.schema,
                    "table": event.table,
                    "columnnames": list(row["after_values"].keys()),
                    "columnvalues": [
                        serialize_value(value) for value in row["after_values"].values()
                    ],
                    "oldkeys": {
                        "keynames": list(row["before_values"].keys()),
                        "keyvalues": [
                            serialize_value(value)
                            for value in row["before_values"].values()
                        ],
                    },
                }
                for row in event.rows
            ]

        if isinstance(event, DeleteRowsEvent):
            return [
                {
                    "kind": "delete",
                    "schema": event.schema,
                    "table": event.table,
                    "columnnames": [],
                    "columnvalues": [],
                    "oldkeys": {
                        "keynames": list(row["values"].keys()),
                        "keyvalues": [
                            serialize_value(value) for value in row["values"].values()
                        ],
                    },
                }
                for row in event.rows
            ]

        # `only_events` restricts the stream to the three row events handled above;
        # anything else that reaches here carries no rows to emit.
        return []

    # -------------------------------------------------------------------- snapshot

    def perform_initial_snapshot(
        self, batch_size: int, start_after: Optional[List[Any]] = None
    ) -> Iterator[Tuple[List[Dict[str, Any]], List[Any]]]:
        """
        Walk the table on `snapshot_host`, yielding one keyset page at a time.

        The generator owns its connection and closes it when it is exhausted or
        closed, so callers should wrap it in `contextlib.closing()` to release the
        connection promptly when producing a page raises.

        :param batch_size: maximum rows per page.
        :param start_after: primary-key values of the last row committed by a previous
            run; the walk resumes strictly after that row.
        :return: an iterator of `(changes, last_key_values)`.
        """
        conn = self.connect_mysql(override_host=self._snapshot_host)
        try:
            with conn.cursor() as cursor:
                pk_columns = self.require_primary_key(cursor)
                estimated_rows = estimate_row_count(cursor, self._database, self._table)
                logger.info(
                    "Starting initial snapshot of %s from %s: ~%s rows (estimate), "
                    "paginating on %s",
                    self._table_name,
                    self._snapshot_host,
                    estimated_rows if estimated_rows is not None else "unknown",
                    ", ".join(pk_columns),
                )
                yield from iter_snapshot_batches(
                    cursor,
                    database=self._database,
                    table=self._table,
                    pk_columns=pk_columns,
                    batch_size=batch_size,
                    start_after=start_after,
                )
        finally:
            conn.close()
