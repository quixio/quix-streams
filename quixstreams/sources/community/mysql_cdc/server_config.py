"""Server preconditions the source checks, and the two settings it repairs itself."""

import logging
from typing import Any, List, Optional

from .config import MySqlCdcError
from .drivers import MySQLError, mysql_error_code
from .snapshot import discover_primary_key, quote_identifier

__all__ = (
    "ACCESS_DENIED_ERROR_CODE",
    "binlog_file_present",
    "binlog_retention_seconds",
    "ensure_row_settings",
    "require_binlog_enabled",
    "require_distinct_server_id",
    "require_primary_key",
    "require_row_format",
    "require_table",
    "require_whole_table_select",
    "show_global_variable",
)

logger = logging.getLogger(__name__)

# ER_SPECIFIC_ACCESS_DENIED_ERROR: MySQL's answer to a refused SET GLOBAL, a refused
# FLUSH BINARY LOGS and a refused SHOW ... STATUS alike.
ACCESS_DENIED_ERROR_CODE = 1227

# ER_TABLEACCESS_DENIED_ERROR and ER_COLUMNACCESS_DENIED_ERROR: the two answers to a
# SELECT that reaches past the columns the account was granted.
_TABLE_ACCESS_DENIED_CODES = frozenset({1142, 1143})

# Both are global and dynamic. MySQL 8.0.46 ships binlog_row_metadata = MINIMAL, which
# is the one this normally raises; binlog_row_image already ships FULL and is raised
# only where it has been lowered deliberately.
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
    Read a variable at GLOBAL scope; `SHOW VARIABLES` answers from the session, whose
    copy was taken when it connected and does not follow a `SET GLOBAL`.

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
    :raises MySqlCdcError: if `binlog_format` exists and is anything but ROW.
    """
    binlog_format = show_global_variable(cursor, "binlog_format")
    if binlog_format is None:
        logger.info(
            "%s has no binlog_format variable, so it can only write row images", host
        )
        return
    if binlog_format != "ROW":
        raise MySqlCdcError(
            f"binlog_format is {binlog_format!r} on {host}, but CDC requires 'ROW': any "
            "other format carries statements instead of row images. Set "
            "binlog_format=ROW in the MySQL configuration and restart the server."
        )


def ensure_row_settings(cursor: Any, host: str, user: str) -> None:
    """
    Raise whichever of `binlog_row_metadata` and `binlog_row_image` is below FULL, then
    rotate the binary log.

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
        "Set %s = FULL on %s. Add them to that server's my.cnf so they survive its next "
        "restart, and reconnect any writer that is already connected.",
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
            "with may not set it. On a self-managed server, grant it once with: GRANT "
            f"SYSTEM_VARIABLES_ADMIN ON *.* TO '{user}'@'%'; On a managed one (RDS, "
            "Aurora, Cloud SQL) that GRANT is refused even to the master account - set "
            f"{name} to FULL in the instance's parameter group, database flags or "
            "equivalent console setting instead, and apply it."
        ) from exc


def require_whole_table_select(
    cursor: Any, host: str, database: str, table: str, user: str
) -> None:
    """
    Require SELECT on the whole table rather than on some of its columns.

    `information_schema.COLUMNS` and `SHOW COLUMNS` are both filtered to the columns the
    account holds a privilege on, so a column-level grant makes a truncated column list
    look like the table's real one; `SELECT *` is what MySQL refuses.

    :raises MySqlCdcError: if the account may read only some of the table's columns.
    """
    qualified = f"{quote_identifier(database)}.{quote_identifier(table)}"
    try:
        cursor.execute(f"SELECT * FROM {qualified} LIMIT 0")  # noqa: S608
        cursor.fetchall()
    except MySQLError as exc:
        if mysql_error_code(exc) not in _TABLE_ACCESS_DENIED_CODES:
            raise
        raise MySqlCdcError(
            "The account this source connects with may not read every column of "
            f"{database}.{table} on {host}: MySQL refused SELECT *, which means the "
            "account holds a column-level grant. The snapshot would then read only the "
            "granted columns while the binlog carries all of them, putting two "
            "different schemas on one topic. Grant the whole table with: GRANT SELECT "
            f"ON {database}.{table} TO '{user}'@'%';"
        ) from exc


def _flush_binary_logs(cursor: Any, host: str) -> None:
    """Rotate the binary log, reporting rather than raising if RELOAD is not granted."""
    try:
        cursor.execute("FLUSH BINARY LOGS")
    except MySQLError as exc:
        if mysql_error_code(exc) != ACCESS_DENIED_ERROR_CODE:
            raise
        logger.info(
            "Did not rotate the binary logs on %s: the account has no RELOAD privilege, "
            "so events written before now stay in the current file.",
            host,
        )


def binlog_file_present(cursor: Any, log_file: str) -> Optional[bool]:
    """
    :param log_file: a binary log file name as `SHOW BINARY LOGS` spells it.
    :return: whether the server still holds that file, or None if this account may not
        ask.
    """
    try:
        cursor.execute("SHOW BINARY LOGS")
    except MySQLError as exc:
        if mysql_error_code(exc) != ACCESS_DENIED_ERROR_CODE:
            raise
        logger.debug("SHOW BINARY LOGS is not permitted for this user: %s", exc)
        return None
    return any(str(row[0]) == log_file for row in cursor.fetchall())


def binlog_retention_seconds(cursor: Any) -> Optional[int]:
    """
    :return: how long the server keeps a binary log file, or None if it never expires
        one automatically or has no such variable.
    """
    value = show_global_variable(cursor, "binlog_expire_logs_seconds")
    if value is None or not str(value).isdigit():
        return None
    return int(value) or None


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
