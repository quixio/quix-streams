import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Mapping, Optional, Union

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import register_adapter
    from psycopg2.extras import Json, execute_values
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
from quixstreams.sinks.base.item import SinkItem

__all__ = ("PostgreSQLSink", "PostgreSQLSinkException")

logger = logging.getLogger(__name__)

# A column name for the records keys
KEY_COLUMN_NAME = "__key"

# A column name for the records timestamps
TIMESTAMP_COLUMN_NAME = "timestamp"

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

TableName = Union[Callable[[SinkItem], str], str]
PrimaryKeySetter = Callable[[SinkItem], Union[str, list[str], tuple[str]]]
PrimaryKeyColumns = Union[str, list[str], tuple[str], PrimaryKeySetter]


class PostgreSQLSinkException(QuixException): ...


class PostgresSQLSinkMissingExistingPK(QuixException): ...


class PostgresSQLSinkInvalidPK(QuixException): ...


class PostgresSQLSinkPKNullValue(QuixException): ...


class PostgreSQLSink(BatchingSink):
    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        table_name: TableName,
        schema_name: str = "public",
        schema_auto_update: bool = True,
        connection_timeout_seconds: int = 30,
        statement_timeout_seconds: int = 30,
        primary_key_columns: PrimaryKeyColumns = (),
        upsert_on_primary_key: bool = False,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        **kwargs,
    ):
        """
        A connector to sink topic data to PostgreSQL.

        :param host: PostgreSQL server address.
        :param port: PostgreSQL server port.
        :param dbname: PostgreSQL database name.
        :param user: Database username.
        :param password: Database user password.
        :param table_name: PostgreSQL table name as either a string or a callable which
            receives a SinkItem and returns a string.
        :param schema_name: The schema name. Schemas are a way of organizing tables and
            not related to the table data, referenced as `<schema_name>.<table_name>`.
            PostrgeSQL uses "public" by default under the hood.
        :param schema_auto_update: Automatically update the schema when new columns are detected.
        :param connection_timeout_seconds: Timeout for connection.
        :param statement_timeout_seconds: Timeout for DDL operations such as table
            creation or schema updates.
        :param primary_key_columns: An optional single (string) or list of primary key
            column(s); len>1 is a composite key, a non-empty str or len==1 is a primary
            key, and len<1 or empty string means no primary key.
            Can instead provide a callable, which uses the message value as input and
            returns a string or list of strings.
            Often paired with `upsert_on_primary_key=True`.
            It must include all currently defined primary key columns on a given table.
        :param upsert_on_primary_key: Upsert based on the given `primary_key_columns`.
            If False, every message is treated as an independent entry, and any
            primary key collisions will consequently raise an exception.
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
        for t in (dict, list, tuple):
            register_adapter(t, Json)
        self._table_name = _table_name_setter(table_name)
        self._tables = set()
        self._schema_name = schema_name
        self._schema_auto_update = schema_auto_update
        options = kwargs.pop("options", "")
        if "statement_timeout" not in options:
            options = f"{options} -c statement_timeout={statement_timeout_seconds}s"

        if primary_key_columns and not upsert_on_primary_key:
            logger.warning(
                "NOTE: `primary_key_columns` was provided but `upsert_on_primary_key` "
                "is False; upserting is commonly desired when using a primary key."
            )
        self._primary_key_setter = _primary_key_setter(primary_key_columns)
        self._do_upsert = upsert_on_primary_key

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
        self._create_schema()

    def write(self, batch: SinkBatch):
        tables = {}
        for item in batch:
            table = tables.setdefault(
                self._table_name(item), {"rows": [], "cols_types": {}, "pks_types": {}}
            )
            primary_key_columns = self._primary_key_setter(item) or ()
            if isinstance(primary_key_columns, str):
                primary_key_columns = [primary_key_columns]

            row = {}
            if item.key is not None:
                key_type = type(item.key)
                table["cols_types"].setdefault(KEY_COLUMN_NAME, key_type)
                row[KEY_COLUMN_NAME] = item.key
            for key, value in item.value.items():
                if key in primary_key_columns:
                    if value is None:
                        raise PostgresSQLSinkPKNullValue(
                            f"Primary key column '{key}' value cannot be None (null)"
                        )
                    table["pks_types"].setdefault(key, type(value))
                    row[key] = value
                elif value is not None:
                    table["cols_types"].setdefault(key, type(value))
                    row[key] = value

            row[TIMESTAMP_COLUMN_NAME] = datetime.fromtimestamp(item.timestamp / 1000)
            table["rows"].append(row)

        table_counts = {}
        try:
            with self._client:
                for name, values in tables.items():
                    table_counts[name] = len(values["rows"])
                    if self._schema_auto_update:
                        self._create_table(name)
                        if self._primary_key_setter:
                            self._add_new_primary_keys(name, values["pks_types"])
                        self._add_new_columns(name, values["cols_types"])
                    self._insert_rows(name, values["rows"], values["pks_types"])
        except psycopg2.Error as e:
            self._client.rollback()
            if "duplicate key value violates unique constraint" in str(e):
                raise PostgreSQLSinkException(
                    f"Failed to write batch: attempted insert of new row using an "
                    f"existing primary key. Try `upsert_on_primary_key=True` for "
                    f"upsert functionality. PostgreSQL error info: {str(e)}"
                ) from e
            raise PostgreSQLSinkException(f"Failed to write batch: {str(e)}") from e
        schema_log = (
            " "
            if self._schema_name == "public"
            else f" for schema '{self._schema_name}' "
        )
        logger.info(
            f"Successfully wrote records{schema_log}to tables; "
            f"table row counts: {table_counts}"
        )

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

    def _create_schema(self):
        query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(self._schema_name)
        )

        with self._client.cursor() as cursor:
            cursor.execute(query)

    def _create_table(self, table_name: str):
        if table_name in self._tables:
            return
        query = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {table} (
                {timestamp_col} TIMESTAMP NOT NULL,
                {key_col} TEXT
            )
            """
        ).format(
            table=sql.Identifier(self._schema_name, table_name),
            timestamp_col=sql.Identifier(TIMESTAMP_COLUMN_NAME),
            key_col=sql.Identifier(KEY_COLUMN_NAME),
        )

        with self._client.cursor() as cursor:
            cursor.execute(query)

    def _add_new_columns(self, table_name: str, columns: dict[str, type]) -> None:
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
                table=sql.Identifier(self._schema_name, table_name),
                column=sql.Identifier(col_name),
                col_type=sql.SQL(postgres_col_type),
            )

            with self._client.cursor() as cursor:
                cursor.execute(query)

    def get_current_primary_key_columns(self, table: str) -> list[str]:
        if not self._client:
            self.setup()
        query = sql.SQL(
            """
            SELECT
                kcu.column_name
            FROM
                information_schema.table_constraints tc
            JOIN
                information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE
                tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_name = {table}
                AND tc.table_schema = {schema};
            """
        ).format(
            schema=sql.Literal(self._schema_name),
            table=sql.Literal(table),
        )
        with self._client.cursor() as cursor:
            cursor.execute(query)
            pks = sorted(row[0] for row in cursor.fetchall())
        return pks

    def _add_new_primary_keys(self, table_name: str, pks: dict[str, type]) -> None:
        current_pks = self.get_current_primary_key_columns(table_name)
        _pks = sorted(pks.keys())
        if current_pks == _pks:
            return

        _pks = set(_pks)
        if len(current_pks) != 0:
            current_pks = set(current_pks)
            if missing := current_pks - _pks:
                raise PostgresSQLSinkMissingExistingPK(
                    f"the PostgresSQLSink `primary_key` argument does not include the"
                    f"already existing Primary Key(s): {missing}"
                )
            else:
                # TODO: Provide an option to allow deletion of primary keys
                raise PostgresSQLSinkInvalidPK(
                    f"A different {'composite' if len(current_pks) > 1 else ''} "
                    f"primary key is already defined; to use the provided list as the "
                    f"new {'composite' if len(pks) > 1 else ''} primary key, remove "
                    f"the existing first. Current: {current_pks}. Provided: {pks}"
                )

        logger.info(f"Adding new primary key column(s): {_pks}")
        self._add_new_columns(table_name, pks)

        logger.info(f"Setting new primary key(s): {_pks}")
        query = sql.SQL(
            """
            ALTER TABLE {table}
            ADD PRIMARY KEY ({pks})
            """
        ).format(
            table=sql.Identifier(self._schema_name, table_name),
            pks=sql.SQL(", ").join(sql.Identifier(pk) for pk in pks),
        )

        with self._client.cursor() as cursor:
            cursor.execute(query)

    def _insert_rows(
        self, table_name: str, rows: list[dict], pks: Optional[dict] = None
    ) -> None:
        if not rows:
            return

        # Collect all column names from the first row
        columns = list(rows[0].keys())

        query = sql.SQL("INSERT INTO {table} ({columns}) VALUES %s").format(
            table=sql.Identifier(self._schema_name, table_name),
            columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        )
        if pks and self._do_upsert:
            # We must de-duplicate based on primary keys, else an exception is thrown.
            # We take the "latest" values of each based on original read order.
            rows = {tuple(row[pk] for pk in pks): row for row in rows}.values()
            upsert_stub = sql.SQL(
                "ON CONFLICT ({pks}) DO UPDATE SET {non_pk_cols}"
            ).format(
                pks=sql.SQL(", ").join(map(sql.Identifier, pks)),
                non_pk_cols=sql.SQL(", ").join(
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in columns
                    if col not in pks
                ),
            )
            query = sql.SQL(" ").join([query, upsert_stub])

        # Handle missing keys gracefully
        values = [[row.get(col, None) for col in columns] for row in rows]
        with self._client.cursor() as cursor:
            execute_values(cursor, query, values)


def _table_name_setter(
    table_name: Union[Callable[[SinkItem], str], str],
) -> Callable[[SinkItem], str]:
    if isinstance(table_name, str):
        return lambda sink_item: table_name
    return table_name


def _primary_key_setter(columns: PrimaryKeyColumns) -> PrimaryKeySetter:
    if not callable(columns):
        return lambda sink_item: columns
    return columns
