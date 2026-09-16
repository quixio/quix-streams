"""Every MySQL connection the CDC source opens."""

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple

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
from .reader import BinlogReader
from .server_config import ACCESS_DENIED_ERROR_CODE
from .snapshot import SnapshotPlan, estimate_row_count, iter_snapshot_batches
from .values import fetch_column_types

__all__ = ("MySqlCdcError", "MySqlHelper")

logger = logging.getLogger(__name__)

# No single spelling answers on both 8.0.46 and 8.4.2 - each rejects one of the pair
# with a syntax error - so both are tried in turn.
_BINLOG_STATUS_STATEMENTS = ("SHOW MASTER STATUS", "SHOW BINARY LOG STATUS")
_REPLICA_STATUS_STATEMENTS = ("SHOW REPLICA STATUS", "SHOW SLAVE STATUS")

# `RowsEvent.__read_values` renders TIMESTAMP through `datetime.utcfromtimestamp` and
# takes no time zone, so the snapshot's sessions are moved to match it.
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

    @property
    def host(self) -> str:
        """The server the binlog stream runs against."""
        return self._host

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
                self._require_whole_table_select(cursor, self._host)
                if require_primary_key:
                    server_config.require_primary_key(
                        cursor, self._database, self._table
                    )
        finally:
            conn.close()

        if self._snapshot_host != self._host:
            conn = self.connect_mysql(override_host=self._snapshot_host)
            try:
                with conn.cursor() as cursor:
                    self._require_whole_table_select(cursor, self._snapshot_host)
            finally:
                conn.close()
            logger.info("Snapshot host %s is reachable", self._snapshot_host)

    def _require_whole_table_select(self, cursor: Any, host: str) -> None:
        server_config.require_whole_table_select(
            cursor, host, self._database, self._table, self._user
        )

    def binlog_file_present(self, log_file: str) -> Optional[bool]:
        """
        :param log_file: a binary log file name on `host`.
        :return: whether the server still holds it, or None if it may not be asked.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                return server_config.binlog_file_present(cursor, log_file)
        finally:
            conn.close()

    def binlog_retention_seconds(self) -> Optional[int]:
        """
        :return: how long `host` keeps a binary log file, or None if it keeps them
            until something purges them by hand.
        """
        conn = self.connect_mysql()
        try:
            with conn.cursor() as cursor:
                return server_config.binlog_retention_seconds(cursor)
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
    ) -> BinlogReader:
        """
        Open a binlog stream positioned on the event after `log_file`:`log_pos`.

        :param server_id: the replication client id to announce.
        """
        # A fresh dict per call: `BinLogStreamReader.__init__` keeps the dict it is
        # handed and setdefault()s "charset" into it.
        connection_settings: Dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "user": self._user,
            "password": self._password,
            "init_command": _SESSION_TIME_ZONE,
        }
        connection_settings.update(self._timeouts.connect_kwargs())
        connection_settings.update(self._tls.connect_kwargs())
        stream = BinLogStreamReader(
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
        return BinlogReader(stream, database=self._database, table=self._table)

    def plan_snapshot(self) -> SnapshotPlan:
        """
        Read the table's shape from `snapshot_host`, before any row is read.

        :raises MySqlCdcError: if the table has no PRIMARY KEY, or if a PRIMARY KEY
            column is missing from its column list.
        """
        conn = self.connect_mysql(override_host=self._snapshot_host)
        try:
            with conn.cursor() as cursor:
                pk_columns = server_config.require_primary_key(
                    cursor, self._database, self._table
                )
                column_types = fetch_column_types(cursor, self._database, self._table)
                estimated_rows = estimate_row_count(cursor, self._database, self._table)
        finally:
            conn.close()

        columns = list(column_types)
        known = {name.lower() for name in columns}
        unknown = [name for name in pk_columns if name.lower() not in known]
        if unknown:
            raise MySqlCdcError(
                f"The PRIMARY KEY of {self._table_name} names {', '.join(unknown)}, "
                f"which information_schema.COLUMNS on {self._snapshot_host} does not "
                "list for that table, so the snapshot has no key to page on."
            )
        return SnapshotPlan(
            pk_columns=pk_columns,
            columns=columns,
            column_types=column_types,
            estimated_rows=estimated_rows,
        )

    def perform_initial_snapshot(
        self,
        plan: SnapshotPlan,
        batch_size: int,
        start_after: Optional[List[Any]] = None,
    ) -> Iterator[Tuple[List[Dict[str, Any]], List[Any]]]:
        """
        Walk the table on `snapshot_host`, yielding one keyset page at a time.

        The generator owns its connection, so callers should wrap it in
        `contextlib.closing()`.

        :param plan: from `plan_snapshot()`.
        :param batch_size: maximum rows per page.
        :param start_after: primary-key values to resume strictly after.
        :return: an iterator of `(changes, last_key_values)`.
        """
        conn = self.connect_mysql(override_host=self._snapshot_host)
        try:
            with conn.cursor() as cursor:
                estimated = plan.estimated_rows
                logger.info(
                    "Starting initial snapshot of %s from %s: ~%s rows (estimate), "
                    "%s columns, paginating on %s",
                    self._table_name,
                    self._snapshot_host,
                    estimated if estimated is not None else "unknown",
                    len(plan.columns),
                    ", ".join(plan.pk_columns),
                )
                yield from iter_snapshot_batches(
                    cursor,
                    database=self._database,
                    table=self._table,
                    plan=plan,
                    batch_size=batch_size,
                    start_after=start_after,
                )
        finally:
            conn.close()
