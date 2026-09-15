"""
MySQL access layer for the CDC source.

Everything that talks to MySQL lives here: connections, server validation, binlog
coordinates, the binlog stream and the snapshot generator. Nothing in this module
persists anything - the binlog position is owned by `MySqlCdcSource` and stored in
its Quix Streams state store.
"""

import logging
import ssl
import time
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

from .config import MySqlCdcError, TlsConfig
from .snapshot import discover_primary_key, estimate_row_count, iter_snapshot_batches
from .values import fetch_column_types, serialize_binlog_values

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
    from pymysqlreplication.constants import FIELD_TYPE
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


__all__ = (
    "BinlogErrorKind",
    "MySqlCdcError",
    "MySqlHelper",
    "classify_error",
    "is_connection_error",
)

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
# and hides its cause.
#   1044 ER_DBACCESS_DENIED_ERROR        - the grant on the database was revoked.
#   1045 ER_ACCESS_DENIED_ERROR          - wrong password, or the account was dropped.
#   1227 ER_SPECIFIC_ACCESS_DENIED_ERROR - the account lacks REPLICATION SLAVE/CLIENT.
#   2026 CR_SSL_CONNECTION_ERROR         - "SSL is required but the server doesn't
#        support it" (`pymysql/connections.py:925-929`). A server without TLS does not
#        grow it between two reconnects; either configure TLS there, or set
#        tls_enabled=False deliberately.
_FATAL_MYSQL_ERROR_CODES = frozenset({1044, 1045, _ACCESS_DENIED_ERROR_CODE, 2026})

# 1236 ER_MASTER_FATAL_ERROR_READING_BINLOG is NOT in that set, because MySQL reuses the
# one code for three situations that need three different answers. The code cannot tell
# them apart; the message can, and `classify_error()` reads it:
#   "...same server_uuid/server_id..."  another client announced our id -> COLLISION
#   "Could not find first log file..."  the committed position is purged -> PURGED
#   anything else                       a genuine read failure           -> RETRYABLE
# Both the 5.7 ("slave"/"master") and the 8.x ("replica"/"source") wordings contain the
# server_uuid/server_id substring, so one marker covers every version in scope.
_BINLOG_READ_ERROR_CODE = 1236
_COLLISION_MARKER = "same server_uuid/server_id"
_PURGED_MARKER = "could not find first log file"

# Every connection the connector opens runs this first. See `connect_mysql()`.
_SESSION_TIME_ZONE = "SET time_zone = '+00:00'"

_NO_PRIMARY_KEY_ERROR = (
    "Table {table_name} has no PRIMARY KEY. The initial snapshot requires one for "
    "stable pagination. Add a primary key, or set initial_snapshot=False to stream "
    "binlog changes only."
)


class BinlogErrorKind(str, Enum):
    """What the source should do about a failure raised while streaming."""

    RETRYABLE = "retryable"
    COLLISION = "collision"
    PURGED = "purged"
    FATAL = "fatal"


def classify_error(exc: BaseException) -> BinlogErrorKind:
    """
    Decide how a streaming failure has to be handled.

    The exception class alone cannot decide this: pymysql raises `OperationalError` for
    a dropped connection, for access-denied and for every flavour of 1236 alike. So the
    MySQL error number is checked, and for 1236 the message too - a collision needs a
    bounded number of reconnects before it is called what it is, while a purged position
    must not be retried at all.

    `ssl.SSLCertVerificationError` is fatal for the same reason a bad password is: the
    next connection presents the same rejected certificate. Other `ssl.SSLError`
    subclasses stay retryable, because a TLS link that dropped is still a link that
    dropped.
    """
    if isinstance(exc, ssl.SSLCertVerificationError):
        return BinlogErrorKind.FATAL
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ssl.SSLError)):
        return BinlogErrorKind.RETRYABLE
    if not isinstance(exc, (OperationalError, InterfaceError)):
        return BinlogErrorKind.FATAL

    code = _mysql_error_code(exc)
    if code in _FATAL_MYSQL_ERROR_CODES:
        return BinlogErrorKind.FATAL
    if code == _BINLOG_READ_ERROR_CODE:
        message = str(exc).lower()
        if _COLLISION_MARKER in message:
            return BinlogErrorKind.COLLISION
        if _PURGED_MARKER in message:
            return BinlogErrorKind.PURGED
    return BinlogErrorKind.RETRYABLE


def is_connection_error(exc: BaseException) -> bool:
    """True for failures a reconnect can plausibly clear: retryable ones and collisions."""
    return classify_error(exc) in (
        BinlogErrorKind.RETRYABLE,
        BinlogErrorKind.COLLISION,
    )


def _mysql_error_code(exc: BaseException) -> Optional[int]:
    """Return the MySQL error number a pymysql exception carries, or None."""
    code = exc.args[0] if exc.args else None
    return code if isinstance(code, int) else None


def _typed_columns(event: Any) -> Tuple[Set[str], Set[str]]:
    """
    Return the event's (JSON column names, FLOAT column names).

    Two of the value contract's rules cannot be applied from the Python value alone. A
    JSON column holding a top-level string arrives as plain `bytes`, identical to a
    VARBINARY, and would be base64-encoded instead of quoted as JSON. A FLOAT arrives as
    a Python float, identical to a DOUBLE, and has to be rounded to the six significant
    digits MySQL's text protocol shows the snapshot path. Both are read from the
    table-map event, which under `binlog_row_metadata=FULL` carries every column.
    """
    json_columns: Set[str] = set()
    float_columns: Set[str] = set()
    for column in event.columns:
        name = column.name
        if not name:
            continue
        if column.type == FIELD_TYPE.JSON:
            json_columns.add(name)
        elif column.type == FIELD_TYPE.FLOAT:
            float_columns.add(name)
    return json_columns, float_columns


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
        tls: TlsConfig,
        allow_minimal_row_metadata: bool = False,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._table = table
        self._table_name = f"{database}.{table}"
        self._snapshot_host = snapshot_host
        self._tls = tls
        self._allow_minimal_row_metadata = allow_minimal_row_metadata

    def connect_mysql(self, override_host: Optional[str] = None) -> Any:
        """
        Open a new connection to `host`, or to `override_host` when given.

        The session time zone is pinned to UTC on every connection the connector opens.
        That is not a preference: the binlog decoder renders TIMESTAMP with
        `datetime.utcfromtimestamp` and cannot be told otherwise, so the snapshot path -
        which renders in the session zone - is the only side that can be moved, and both
        paths have to agree for `values.py`'s contract to hold.
        """
        return pymysql.connect(
            host=override_host or self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
            init_command=_SESSION_TIME_ZONE,
            **self._tls.connect_kwargs(),
        )

    # ------------------------------------------------------------------ validation

    def validate_server_config(self, require_primary_key: bool, server_id: int) -> None:
        """
        Check everything that must hold before streaming, failing loudly if it does not.

        :param require_primary_key: when True (the initial snapshot is enabled), also
            require the table to have a PRIMARY KEY to paginate on. Streaming-only
            mode does not need one.
        :param server_id: the replication client id this source will announce, checked
            against the server's own id.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                self._require_binlog_enabled(cursor)
                self._require_row_format(cursor)
                self._warn_on_row_image(cursor)
                self._require_row_metadata(cursor)
                self._require_distinct_server_id(cursor, server_id)
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

    def require_row_metadata(self) -> None:
        """
        Re-check `binlog_row_metadata` on its own connection.

        Called before every explicit stream rebuild. `binlog_row_metadata` is a dynamic
        global, so a server can be downgraded to MINIMAL under a running source; the
        window between this check and the next one is documented rather than closed,
        because the only way to close it would be a `SHOW VARIABLES` per poll.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                self._require_row_metadata(cursor)
        finally:
            conn.close()

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
            # A warning rather than a refusal, unlike binlog_row_metadata below, and the
            # difference is the point: MINIMAL here yields events that are *partial but
            # honest* - the primary key is still in the before-image and every column
            # that is present is correct. MINIMAL metadata yields events that are wrong.
            logger.warning(
                "binlog_row_image is %r on %s; 'FULL' is recommended so update and "
                "delete events carry every column instead of only the primary key",
                row_image,
                self._host,
            )

    def _require_row_metadata(self, cursor: Any) -> None:
        """
        FULL is the only setting under which the binlog carries usable column metadata.

        `pymysqlreplication` does not degrade gradually: unless the variable reads
        exactly FULL it discards *all* optional table metadata
        (`binlogstream.py:576-596`, `row_event.py:995-1019`), so change events lose
        column names, ENUM and SET dictionaries, column character sets and integer
        signedness together. The consequences are not cosmetic - ENUM and SET values
        arrive as null, UNSIGNED integers decode as signed, and a column whose bytes are
        not UTF-8 raises `UnicodeDecodeError` inside the decoder on every replay of that
        row. Only the column names have a client-side recovery; the rest have none,
        which is why this is a refusal while `_warn_on_row_image` above is only a
        warning.

        `allow_minimal_row_metadata=True` turns the refusal into a warning. It exists
        for one case that is otherwise unserviceable - a MySQL 5.7 server, which has no
        such variable at all - and for a stock 8.x server whose table happens to contain
        none of the four column shapes that break. The warning names the parameter, so a
        log line can always be traced back to the deployment that asked for it.
        """
        row_metadata = self._show_variable(cursor, "binlog_row_metadata")
        if row_metadata == "FULL":
            return
        if self._allow_minimal_row_metadata:
            self._warn_minimal_row_metadata(row_metadata)
            return
        if row_metadata is None:
            raise MySqlCdcError(
                f"{self._host} has no binlog_row_metadata variable, so its binlog "
                "cannot carry the column metadata this source needs (column names, "
                "ENUM/SET values, character sets, integer signedness). That variable "
                "was added in MySQL 8.0.1 and does not exist in MySQL 5.7 or MariaDB. "
                "Either use MySQL 8.0.1+ with binlog_row_metadata=FULL, or set "
                "allow_minimal_row_metadata=True if this table has no ENUM, SET, "
                "UNSIGNED or non-UTF-8 columns - see the connector docs for exactly "
                "what degrades."
            )
        raise MySqlCdcError(
            f"binlog_row_metadata is {row_metadata!r} on {self._host}, but this "
            "source requires 'FULL'. Without it the binlog carries no column names, "
            "no ENUM/SET values, no column character sets and no integer "
            "signedness, so change events would ship nulls for ENUM and SET "
            "columns, decode UNSIGNED integers as negative numbers, and crash on "
            "columns whose bytes are not UTF-8. MINIMAL is the MySQL default, so this "
            "is a configuration step rather than a version problem. Fix it with:\n"
            "  SET GLOBAL binlog_row_metadata = FULL;   -- takes effect immediately, "
            "no restart\n"
            "  FLUSH BINARY LOGS;                       -- leave the MINIMAL-era "
            "events behind\n"
            "and add 'binlog_row_metadata = FULL' to my.cnf so it survives a restart. "
            "If this table has no ENUM, SET, UNSIGNED or non-UTF-8 columns you can set "
            "allow_minimal_row_metadata=True instead."
        )

    def _warn_minimal_row_metadata(self, row_metadata: Optional[str]) -> None:
        """
        Say exactly what the opt-out costs, in values rather than adjectives.

        Logged on every start and on every reconnect, at WARNING, naming the parameter
        that enabled it. "Not recommended" would be useless here: the failures are
        specific, silent in three cases out of four, and the operator can only judge the
        risk against their own table.
        """
        logger.warning(
            "allow_minimal_row_metadata=True: streaming %s from %s with "
            "binlog_row_metadata=%s instead of FULL. The binlog carries no column "
            "metadata, so on THIS TABLE the following change events will be WRONG, "
            "silently, with no further warning: (1) every ENUM and every SET column "
            "arrives as null; (2) every UNSIGNED integer decodes as signed, so "
            "INT UNSIGNED 4294967295 arrives as -1 and BIGINT UNSIGNED above 2^63 "
            "arrives negative; (3) any column whose bytes are not valid UTF-8 - a "
            "BINARY/VARBINARY/BLOB, or a latin1 text column holding a byte above 0x7F - "
            "raises UnicodeDecodeError inside the decoder and stops the source, which "
            "then replays the same row and stops again on every restart. Snapshot rows "
            "are unaffected, so one topic carries two different answers for the same "
            "column. Set binlog_row_metadata=FULL on the server and remove this "
            "parameter unless the table is text-only and has no unsigned columns.",
            self._table_name,
            self._host,
            row_metadata,
        )

    def _require_distinct_server_id(self, cursor: Any, server_id: int) -> None:
        """
        Refuse to announce the server's own replication id.

        A client that registers with the server's `server_id` is always wrong, and it is
        the one collision that can be caught before streaming rather than as a 1236
        eviction minutes later. Other clients cannot be pre-checked: a
        `pymysqlreplication` stream never sends COM_REGISTER_SLAVE, so it never appears
        in `SHOW REPLICAS` and there is nothing to compare against.
        """
        cursor.execute("SELECT @@server_id")
        row = cursor.fetchone()
        own_id = int(row[0]) if row and row[0] is not None else None
        if own_id is not None and own_id == server_id:
            raise MySqlCdcError(
                f"This source would announce server_id={server_id}, which is "
                f"{self._host}'s own server-id. MySQL requires every replication client "
                "to use an id distinct from the server's and from every other client's. "
                "Set an explicit server_id on the source, or change the server's "
                "server-id."
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

        `use_column_name_cache=True` is a safety net for one window, not the mechanism
        that supplies column names. `validate_server_config()` requires
        `binlog_row_metadata=FULL`, under which every table-map event carries current
        names. But binlog files written *before* the setting was changed still parse as
        MINIMAL, and for those `pymysqlreplication` takes its INFORMATION_SCHEMA
        fallback (`row_event.py:1008-1019`) - this flag is what makes that fallback
        produce real names instead of `UNKNOWN_COL0..n`. It caches per `schema.table`
        for the lifetime of the process, which is harmless here because the events it
        covers are historical by definition.

        `ignore_decode_errors` is deliberately NOT passed: it turns the decoder's
        `decode()` into `errors="ignore"` (`row_event.py:403`), which silently drops
        undecodable bytes. `read_changes()` raises an actionable error instead.

        The connection settings dict is rebuilt on every call because
        `BinLogStreamReader` mutates the one it is given (`binlogstream.py:241`) and
        copies it into the control connection's settings (`:313-318`) - so this one dict
        is also what carries TLS and the UTC session zone to the control connection that
        reads INFORMATION_SCHEMA.
        """
        connection_settings: Dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "user": self._user,
            "password": self._password,
            "init_command": _SESSION_TIME_ZONE,
        }
        connection_settings.update(self._tls.connect_kwargs())
        return BinLogStreamReader(
            connection_settings=connection_settings,
            server_id=server_id,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            only_schemas=[self._database],
            only_tables=[self._table],
            resume_stream=True,
            blocking=False,
            log_file=log_file,
            log_pos=log_pos,
            use_column_name_cache=True,
        )

    def read_changes(
        self,
        stream: BinLogStreamReader,
        max_rows: int,
        max_seconds: float,
        should_continue: Callable[[], bool],
    ) -> Tuple[List[Dict[str, Any]], Optional[Tuple[str, int]]]:
        """
        Read row-changes until any of three bounds is reached, with their position.

        The three bounds are `max_rows` changes collected, `max_seconds` elapsed, and
        `should_continue()` going false. All three are needed. `max_rows` alone does not
        bound anything: events for *other* tables never increment the change count, so a
        busy neighbouring table can hold this loop for as long as it keeps committing.
        And a source that cannot notice `stop()` mid-catch-up blows its
        `shutdown_timeout`, gets SIGKILLed before any commit, and repeats the same
        catch-up on the next start - a failure shaped like data loss.

        Breaking mid-iteration is safe: the position is read once after the loop and
        `BinLogStreamReader` advances `log_file`/`log_pos` per decoded packet, including
        packets its `only_events`/`only_tables` filters discard, so committing it skips
        past uninteresting events rather than re-reading them. The same stream object
        continues on the next call.

        The returned position can be non-None alongside an empty change list - the
        normal case for a quiet table in a busy database - and committing it is safe
        precisely because those events were read and deliberately not emitted.

        :param stream: the open reader.
        :param max_rows: stop once this many changes have been collected.
        :param max_seconds: stop after this long, so one read cannot outlast the commit
            cadence.
        :param should_continue: polled once per decoded event; False means stop now.
        """
        changes: List[Dict[str, Any]] = []
        deadline = time.monotonic() + max_seconds
        try:
            for event in stream:
                # `only_schemas` and `only_tables` are matched independently by the
                # library, never as a pair, so the combination is re-checked here. Both
                # names come from the table-map event body, which every binlog carries
                # whatever `binlog_row_metadata` is set to - unlike the column names.
                if event.schema == self._database and event.table == self._table:
                    changes.extend(self._event_to_changes(event))
                if (
                    len(changes) >= max_rows
                    or time.monotonic() >= deadline
                    or not should_continue()
                ):
                    break
        except (UnicodeDecodeError, LookupError) as exc:
            raise MySqlCdcError(
                f"Could not decode a binlog event for {self._table_name} at "
                f"{stream.log_file}:{stream.log_pos}. The event carries no column "
                "character sets, so the decoder read every string column as UTF-8 and a "
                "column whose bytes are not UTF-8 - a BINARY/VARBINARY/BLOB, or a "
                "latin1 text column holding a byte above 0x7F - failed. "
                + (
                    "This source is running with allow_minimal_row_metadata=True, so "
                    "this is the documented consequence of that opt-out on a table that "
                    "turned out not to be text-only: set binlog_row_metadata=FULL on "
                    "the server (MySQL 8.0.1+) and remove the parameter."
                    if self._allow_minimal_row_metadata
                    else "The server passes this source's start-up check, so "
                    "binlog_row_metadata is FULL now and only events written before it "
                    "was changed are affected."
                )
                + " Two ways out, both of which leave the MINIMAL-era events behind: run "
                "SET GLOBAL binlog_row_metadata = FULL; followed by FLUSH BINARY LOGS on "
                "the server and let the source resume into the new file, or restart the "
                "source with initial_snapshot=True and force_snapshot=True to "
                "re-snapshot the table and re-anchor the position. The row is not "
                "skipped, and ignore_decode_errors is not used, because both would be "
                "silent data loss."
            ) from exc

        log_file, log_pos = stream.log_file, stream.log_pos
        position = (log_file, log_pos) if log_file and log_pos else None
        return changes, position

    def _event_to_changes(self, event: Any) -> List[Dict[str, Any]]:
        """Convert one row event into the connector's change dicts, one per row."""
        json_columns, float_columns = _typed_columns(event)

        def encode(values: Dict[str, Any], none_sources: Any) -> List[Any]:
            return serialize_binlog_values(
                values, none_sources, json_columns, float_columns
            )

        if isinstance(event, WriteRowsEvent):
            return [
                {
                    "kind": "insert",
                    "schema": event.schema,
                    "table": event.table,
                    "columnnames": list(row["values"].keys()),
                    "columnvalues": encode(row["values"], row.get("none_sources")),
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
                    "columnvalues": encode(
                        row["after_values"], row.get("after_none_sources")
                    ),
                    "oldkeys": {
                        "keynames": list(row["before_values"].keys()),
                        "keyvalues": encode(
                            row["before_values"], row.get("before_none_sources")
                        ),
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
                        "keyvalues": encode(row["values"], row.get("none_sources")),
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
                column_types = fetch_column_types(cursor, self._database, self._table)
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
                    column_types=column_types,
                    start_after=start_after,
                )
        finally:
            conn.close()
