"""Every MySQL connection the CDC source opens."""

import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from . import server_config
from .config import ConnectionTimeouts, MySqlCdcError, TlsConfig
from .drivers import (
    BinLogStreamReader,
    DeleteRowsEvent,
    DictCursor,
    MySQLError,
    ProgrammingError,
    UpdateRowsEvent,
    WriteRowsEvent,
    mysql_error_code,
    pymysql,
)
from .events import event_to_changes
from .server_config import ACCESS_DENIED_ERROR_CODE
from .snapshot import estimate_row_count, iter_snapshot_batches
from .values import fetch_column_types

__all__ = ("MySqlCdcError", "MySqlHelper")

logger = logging.getLogger(__name__)

# `SHOW MASTER STATUS` became `SHOW BINARY LOG STATUS` in 8.4; `SHOW SLAVE STATUS`
# became `SHOW REPLICA STATUS` in 8.0.22 and the old spelling survived until 8.4.
_BINLOG_STATUS_STATEMENTS = ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS")
_REPLICA_STATUS_STATEMENTS = ("SHOW REPLICA STATUS", "SHOW SLAVE STATUS")

# The binlog decoder renders TIMESTAMP with `datetime.utcfromtimestamp` and cannot be
# told otherwise, so the snapshot's sessions are moved to match it.
_SESSION_TIME_ZONE = "SET time_zone = '+00:00'"


def _first_present(row: Dict[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return None


class MySqlHelper:
    """
    Owns every MySQL connection the CDC source needs.

    Each method opens and closes its own short-lived connection, except
    `create_binlog_stream()`, whose reader owns its connections, and
    `perform_initial_snapshot()`, which holds one for the length of the table walk.
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
        timeouts: ConnectionTimeouts,
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
        self._timeouts = timeouts

    def connect_mysql(self, override_host: Optional[str] = None) -> Any:
        """
        Open a new connection, with the UTC session pin, TLS and the socket timeouts.

        :param override_host: connect here instead of `host`.
        """
        return pymysql.connect(
            host=override_host or self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
            charset="utf8mb4",
            init_command=_SESSION_TIME_ZONE,
            **self._timeouts.connect_kwargs(),
            **self._tls.connect_kwargs(),
        )

    def validate_server_config(self, require_primary_key: bool, server_id: int) -> None:
        """
        Check the server's preconditions and repair its row-image settings.

        :param require_primary_key: also require the table to have a PRIMARY KEY.
        :param server_id: the replication client id this source will announce.
        :raises MySqlCdcError: for any precondition the connector cannot fix itself.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                server_config.require_binlog_enabled(cursor, self._host)
                server_config.require_row_format(cursor, self._host)
                server_config.ensure_row_settings(cursor, self._host, self._user)
                server_config.require_distinct_server_id(cursor, self._host, server_id)
                server_config.require_table(
                    cursor, self._host, self._database, self._table
                )
                if require_primary_key:
                    server_config.require_primary_key(
                        cursor, self._database, self._table
                    )
        finally:
            conn.close()

        if self._snapshot_host != self._host:
            self.connect_mysql(override_host=self._snapshot_host).close()
            logger.info("Snapshot host %s is reachable", self._snapshot_host)

    def ensure_row_settings(self) -> None:
        """
        Re-apply the row-image settings on their own connection.

        :raises MySqlCdcError: if the account may not set them.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                server_config.ensure_row_settings(cursor, self._host, self._user)
        finally:
            conn.close()

    def fetch_start_position(self) -> Tuple[str, int]:
        """
        :return: the current binlog coordinates of `host` as `(log_file, log_pos)`.
        :raises MySqlCdcError: if no spelling of the status statement yields a row.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                for statement in _BINLOG_STATUS_STATEMENTS:
                    row = self._run_status_statement(cursor, statement)
                    if row:
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
        Return the primary coordinates a replica has already executed.

        With parallel appliers `Exec_*` is a low-water mark, so the result errs towards
        replaying changes rather than skipping them.

        :param host: the replica to ask.
        :raises MySqlCdcError: if it reports no executed coordinates.
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
                    # A zero Exec_* position means the applier has never run.
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
        :return: the coordinates on `host` to start the stream from, given where the
            snapshot rows will be read.
        """
        if self._snapshot_host == self._host:
            return self.fetch_start_position()
        return self.fetch_replica_executed_position(self._snapshot_host)

    @staticmethod
    def _run_status_statement(cursor: Any, statement: str) -> Any:
        """
        :return: the statement's first row, or None if this server or this user cannot
            run it. Every other MySQL error propagates.
        """
        try:
            cursor.execute(statement)
        except ProgrammingError as exc:
            logger.debug("%s is not available on this server: %s", statement, exc)
            return None
        except MySQLError as exc:
            if mysql_error_code(exc) != ACCESS_DENIED_ERROR_CODE:
                raise
            logger.debug("%s is not permitted for this user: %s", statement, exc)
            return None
        return cursor.fetchone()

    def create_binlog_stream(
        self, server_id: int, log_file: str, log_pos: int
    ) -> BinLogStreamReader:
        """
        Open a binlog stream positioned at `log_file`:`log_pos`.

        `resume_stream=True` continues with the event *after* `log_pos`.
        `use_column_name_cache=True` makes the INFORMATION_SCHEMA fallback
        (`row_event.py:1008-1019`) return real names for events written before
        `binlog_row_metadata` was FULL. `ignore_decode_errors` is not passed: it means
        `decode(errors="ignore")` (`row_event.py:403`).

        The settings dict is rebuilt per call because `BinLogStreamReader` mutates it
        (`binlogstream.py:240-241`) and copies it to the control connection
        (`binlogstream.py:313-318`).

        :param server_id: the replication client id to announce.
        """
        connection_settings: Dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "user": self._user,
            "password": self._password,
            "init_command": _SESSION_TIME_ZONE,
        }
        connection_settings.update(self._timeouts.connect_kwargs())
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

        All three bounds are evaluated per decoded event.

        :param stream: the open reader.
        :param max_rows: stop once this many changes have been collected.
        :param max_seconds: stop after this long.
        :param should_continue: polled once per decoded event; False means stop now.
        :return: `(changes, position)`. The position can be non-None with no changes,
            for events this source read and filtered out.
        :raises MySqlCdcError: if an event cannot be decoded.
        """
        changes: List[Dict[str, Any]] = []
        deadline = time.monotonic() + max_seconds
        try:
            for event in stream:
                # `only_schemas` and `only_tables` are matched independently by the
                # library, never as a pair.
                if event.schema == self._database and event.table == self._table:
                    changes.extend(event_to_changes(event))
                if (
                    len(changes) >= max_rows
                    or time.monotonic() >= deadline
                    or not should_continue()
                ):
                    break
        except (UnicodeDecodeError, LookupError) as exc:
            raise MySqlCdcError(
                f"Could not decode a binlog event for {self._table_name} at "
                f"{stream.log_file}:{stream.log_pos}: it was written before this source "
                "set binlog_row_metadata=FULL, so it carries no column character sets. "
                "Either let the source resume past those events, or restart it with "
                "initial_snapshot=True and force_snapshot=True to re-snapshot the table "
                "and re-anchor the position past them."
            ) from exc

        log_file, log_pos = stream.log_file, stream.log_pos
        position = (log_file, log_pos) if log_file and log_pos else None
        return changes, position

    def perform_initial_snapshot(
        self, batch_size: int, start_after: Optional[List[Any]] = None
    ) -> Iterator[Tuple[List[Dict[str, Any]], List[Any]]]:
        """
        Walk the table on `snapshot_host`, yielding one keyset page at a time.

        The generator owns its connection, so callers should wrap it in
        `contextlib.closing()`.

        :param batch_size: maximum rows per page.
        :param start_after: primary-key values to resume strictly after.
        :return: an iterator of `(changes, last_key_values)`.
        """
        conn = self.connect_mysql(override_host=self._snapshot_host)
        try:
            with conn.cursor() as cursor:
                pk_columns = server_config.require_primary_key(
                    cursor, self._database, self._table
                )
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
