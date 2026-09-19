"""The connector's single import of the optional `quixstreams[mysql]` drivers."""

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
    "FIELD_TYPE",
    "BinLogStreamReader",
    "DeleteRowsEvent",
    "DictCursor",
    "InterfaceError",
    "MySQLError",
    "OperationalError",
    "ProgrammingError",
    "UpdateRowsEvent",
    "WriteRowsEvent",
    "mysql_error_code",
    "pymysql",
)


def mysql_error_code(exc: BaseException) -> int:
    """
    Return the MySQL error number a pymysql exception carries.

    :return: the number, or 0 for an exception raised client-side, which carries a
        string or nothing in `args[0]`.
    """
    code = exc.args[0] if exc.args else None
    return code if isinstance(code, int) else 0
