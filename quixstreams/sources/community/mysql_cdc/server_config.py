"""Server preconditions the source checks, and the two settings it repairs itself."""

import logging
from typing import Any, List, Optional

from .config import MySqlCdcError
from .drivers import MySQLError, mysql_error_code
from .snapshot import discover_primary_key

__all__ = (
    "ACCESS_DENIED_ERROR_CODE",
    "ensure_row_settings",
    "require_binlog_enabled",
    "require_distinct_server_id",
    "require_primary_key",
    "require_row_format",
    "require_table",
    "show_global_variable",
)

logger = logging.getLogger(__name__)

# ER_SPECIFIC_ACCESS_DENIED_ERROR: MySQL's answer to a refused SET GLOBAL, a refused
# FLUSH BINARY LOGS and a refused SHOW ... STATUS alike.
ACCESS_DENIED_ERROR_CODE = 1227

# Both default lower than FULL on MySQL 8.0-9.x, both are global and dynamic, and
# neither loss can be recovered by the client once an event has been written.
_REQUIRED_GLOBALS = {
    "binlog_row_metadata": "SET GLOBAL binlog_row_metadata = FULL",
    "binlog_row_image": "SET GLOBAL binlog_row_image = FULL",
}

_NO_PRIMARY_KEY_ERROR = (
    "Table {table_name} has no PRIMARY KEY. The initial snapshot requires one for "
    "stable pagination. Add a primary key, or set initial_snapshot=False to stream "
    "binlog changes only."
)


def show_global_variable(cursor: Any, name: str) -> Optional[str]:
    """
    Read a variable at GLOBAL scope.

    `SHOW VARIABLES` answers from the session, which for `binlog_row_image` and
    `binlog_format` is a copy taken when the connection opened and does not follow a
    `SET GLOBAL`.

    :return: the value, or None if the server has no such variable.
    """
    cursor.execute("SHOW GLOBAL VARIABLES LIKE %s", (name,))
    row = cursor.fetchone()
    return row[1] if row else None


def require_binlog_enabled(cursor: Any, host: str) -> None:
    """
    :raises MySqlCdcError: if binary logging is off.
    """
    if show_global_variable(cursor, "log_bin") != "ON":
        raise MySqlCdcError(
            f"Binary logging is disabled on {host}. CDC requires it: set log_bin "
            "(e.g. log-bin=mysql-bin) in the MySQL configuration and restart the server."
        )


def require_row_format(cursor: Any, host: str) -> None:
    """
    :raises MySqlCdcError: if `binlog_format` is anything but ROW.
    """
    binlog_format = show_global_variable(cursor, "binlog_format")
    if binlog_format != "ROW":
        raise MySqlCdcError(
            f"binlog_format is {binlog_format!r} on {host}, but CDC requires 'ROW'. Any "
            "other format carries statements instead of row images, so this source "
            "would stream a binlog it cannot read. Set binlog_format=ROW in the MySQL "
            "configuration and restart the server."
        )


def ensure_row_settings(cursor: Any, host: str, user: str) -> None:
    """
    Set `binlog_row_metadata` and `binlog_row_image` to FULL if they are not already.

    Logs one INFO line naming what it changed, then rotates the binary log.

    :param host: named in the log line and in the error.
    :param user: named in the GRANT the error suggests.
    :raises MySqlCdcError: if the account may not set the variable.
    """
    changed = []
    for name, statement in _REQUIRED_GLOBALS.items():
        if show_global_variable(cursor, name) == "FULL":
            continue
        _set_global_full(cursor, name, statement, host, user)
        changed.append(name)

    if not changed:
        return

    logger.info(
        "Set %s = FULL on %s. Both are required: without them the binlog carries no "
        "column names, no ENUM/SET values, no character sets, no integer signedness, "
        "and no columns beyond the ones a statement touched. Add them to that server's "
        "my.cnf so they survive its next restart.",
        " and ".join(changed),
        host,
    )
    _flush_binary_logs(cursor, host)


def _set_global_full(
    cursor: Any, name: str, statement: str, host: str, user: str
) -> None:
    try:
        cursor.execute(statement)
    except MySQLError as exc:
        if mysql_error_code(exc) != ACCESS_DENIED_ERROR_CODE:
            raise
        raise MySqlCdcError(
            f"This source needs {name} = FULL on {host} and the account it connects "
            f"with may not set it, so grant it once with: GRANT SYSTEM_VARIABLES_ADMIN "
            f"ON *.* TO '{user}'@'%';"
        ) from exc


def _flush_binary_logs(cursor: Any, host: str) -> None:
    """Rotate the binary log, reporting rather than raising if RELOAD is not granted."""
    try:
        cursor.execute("FLUSH BINARY LOGS")
    except MySQLError as exc:
        if mysql_error_code(exc) != ACCESS_DENIED_ERROR_CODE:
            raise
        logger.info(
            "Did not rotate the binary logs on %s (the account has no RELOAD "
            "privilege), so events written before now stay in the current file. They "
            "are only read if this source resumes from a position inside them.",
            host,
        )


def require_distinct_server_id(cursor: Any, host: str, server_id: int) -> None:
    """
    :param server_id: the id this source will announce.
    :raises MySqlCdcError: if it is the server's own `server_id`.
    """
    cursor.execute("SELECT @@server_id")
    row = cursor.fetchone()
    own_id = int(row[0]) if row and row[0] is not None else None
    if own_id is not None and own_id == server_id:
        raise MySqlCdcError(
            f"This source would announce server_id={server_id}, which is {host}'s own "
            "server-id. MySQL requires every replication client to use an id distinct "
            "from the server's and from every other client's. Set an explicit server_id "
            "on the source, or change the server's server-id."
        )


def require_table(cursor: Any, host: str, database: str, table: str) -> None:
    """
    :raises MySqlCdcError: if the table does not exist or is invisible to this user.
    """
    cursor.execute(
        "SELECT 1 FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (database, table),
    )
    if not cursor.fetchone():
        raise MySqlCdcError(
            f"Table {database}.{table} does not exist on {host}, or the configured user "
            "cannot see it."
        )


def require_primary_key(cursor: Any, database: str, table: str) -> List[str]:
    """
    :return: the table's PK columns in index order.
    :raises MySqlCdcError: if the table has no PRIMARY KEY.
    """
    pk_columns = discover_primary_key(cursor, database, table)
    if not pk_columns:
        raise MySqlCdcError(
            _NO_PRIMARY_KEY_ERROR.format(table_name=f"{database}.{table}")
        )
    return pk_columns
