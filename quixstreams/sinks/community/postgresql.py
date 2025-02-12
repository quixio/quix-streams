import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Optional

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError as exc:
    raise ImportError(
        f"Package `{exc.name}` is missing: "
        'run "pip install quixstreams[postgresql]" to fix it'
    ) from exc

from quixstreams.exceptions import QuixException
from quixstreams.models import HeadersTuples
from quixstreams.sinks import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBatch,
)

__all__ = ("PostgreSQLSink", "PostgreSQLSinkException")

logger = logging.getLogger(__name__)

# A column name for the records keys
_KEY_COLUMN_NAME = "__key"

# A column name for the records timestamps
_TIMESTAMP_COLUMN_NAME = "timestamp"

# A mapping of Python types to PostgreSQL column types for schema updates
_POSTGRES_TYPES_MAP: dict[type, str] = {
    int: "BIGINT",
    float: "DOUBLE PRECISION",
    Decimal: "NUMERIC",
    str: "TEXT",
    bytes: "BYTEA",
    datetime: "TIMESTAMP",
    list: "JSONB",
    dict: "JSONB",
    tuple: "JSONB",
    bool: "BOOLEAN",
}


class PostgreSQLSinkException(QuixException): ...


class PostgreSQLSink(BatchingSink):
    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        table_name: str,
        schema_auto_update: bool = True,
        connection_timeout_seconds: int = 30,
        statement_timeout_seconds: int = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        **kwargs,
    ):
        """
        A connector to sink topic data to PostgreSQL.

        :param host: PostgreSQL server address.
        :param port: PostgreSQL server port.
        :param dbname: PostgreSQL database name.
        :param user: Database user name.
        :param password: Database user password.
        :param table_name: PostgreSQL table name.
        :param schema_auto_update: Automatically update the schema when new columns are detected.
        :param connection_timeout_seconds: Timeout for connection.
        :param statement_timeout_seconds: Timeout for DDL operations such as table
            creation or schema updates.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        :param kwargs: Additional parameters for `psycopg2.connect`.
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self.table_name = table_name
        self.schema_auto_update = schema_auto_update
        options = kwargs.pop("options", "")
        if "statement_timeout" not in options:
            options = f"{options} -c statement_timeout={statement_timeout_seconds}s"
        self._client_settings = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "connect_timeout": connection_timeout_seconds,
            "options": options,
            **kwargs,
        }
        self._client = None

    def setup(self):
        self._client = psycopg2.connect(**self._client_settings)

        # Initialize table if schema_auto_update is enabled
        if self.schema_auto_update:
            self._init_table()

    def write(self, batch: SinkBatch):
        rows = []
        cols_types = {}

        for item in batch:
            row = {}
            if item.key is not None:
                key_type = type(item.key)
                cols_types.setdefault(_KEY_COLUMN_NAME, key_type)
                row[_KEY_COLUMN_NAME] = item.key

            for key, value in item.value.items():
                if value is not None:
                    cols_types.setdefault(key, type(value))
                    row[key] = value

            row[_TIMESTAMP_COLUMN_NAME] = datetime.fromtimestamp(item.timestamp / 1000)
            rows.append(row)

        try:
            with self._client:
                if self.schema_auto_update:
                    self._add_new_columns(cols_types)
                self._insert_rows(rows)
        except psycopg2.Error as e:
            self._client.rollback()
            raise PostgreSQLSinkException(f"Failed to write batch: {str(e)}") from e

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        if not isinstance(value, Mapping):
            raise TypeError(
                f'Sink "{self.__class__.__name__}" supports only dictionaries, '
                f"got {type(value)}"
            )
        return super().add(
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
            topic=topic,
            partition=partition,
            offset=offset,
        )

    def _init_table(self):
        query = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {table} (
                {timestamp_col} TIMESTAMP NOT NULL,
                {key_col} TEXT
            )
            """
        ).format(
            table=sql.Identifier(self.table_name),
            timestamp_col=sql.Identifier(_TIMESTAMP_COLUMN_NAME),
            key_col=sql.Identifier(_KEY_COLUMN_NAME),
        )

        with self._client.cursor() as cursor:
            cursor.execute(query)

    def _add_new_columns(self, columns: dict[str, type]) -> None:
        for col_name, py_type in columns.items():
            postgres_col_type = _POSTGRES_TYPES_MAP.get(py_type)
            if postgres_col_type is None:
                raise PostgreSQLSinkException(
                    f'Failed to add new column "{col_name}": '
                    f'cannot map Python type "{py_type}" to a PostgreSQL column type'
                )
            query = sql.SQL(
                """
                ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS {column} {col_type}
                """
            ).format(
                table=sql.Identifier(self.table_name),
                column=sql.Identifier(col_name),
                col_type=sql.SQL(postgres_col_type),
            )

            with self._client.cursor() as cursor:
                cursor.execute(query)

    def _insert_rows(self, rows: list[dict]) -> None:
        if not rows:
            return

        # Collect all column names from the first row
        columns = list(rows[0].keys())
        # Handle missing keys gracefully
        values = [[row.get(col, None) for col in columns] for row in rows]

        query = sql.SQL("INSERT INTO {table} ({columns}) VALUES %s").format(
            table=sql.Identifier(self.table_name),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        )

        with self._client.cursor() as cursor:
            execute_values(cursor, query, values)
