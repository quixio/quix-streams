import contextlib
import json
import logging
import re
from collections.abc import Generator
from datetime import datetime
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from quixstreams.exceptions import QuixException
from quixstreams.models import HeadersTuples

try:
    from firebolt.client.auth import ClientCredentials
    from firebolt.db import Connection, Cursor, connect
except ImportError as exc:
    raise ImportError(
        'Package "firebolt-sdk" is missing: '
        "run pip install quixstreams[firebolt] to fix it"
    ) from exc

from quixstreams.sinks.base import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBackpressureError,
    SinkBatch,
)
from quixstreams.sinks.base.item import SinkItem

__all__ = ("FireboltSink", "FireboltSinkException")

logger = logging.getLogger(__name__)

# A column name for the records keys
KEY_COLUMN_NAME = "__key"

# A column name for the records timestamps
TIMESTAMP_COLUMN_NAME = "__timestamp"

# A mapping of Python types to Firebolt column types for schema updates
_FIREBOLT_TYPES_MAP: dict[type, str] = {
    int: "BIGINT",
    float: "DOUBLE PRECISION",
    str: "TEXT",
    bytes: "TEXT",  # Firebolt doesn't have BYTES, use TEXT
    datetime: "TIMESTAMP",
    list: "TEXT",
    dict: "TEXT",
    tuple: "TEXT",
    bool: "BOOLEAN",
}

FireboltValueMap = dict[str, Union[str, int, float, bool]]

TableCallable = Callable[[FireboltValueMap], str]
DatabaseCallable = Callable[[FireboltValueMap], str]
ColumnsCallable = Callable[[FireboltValueMap], Iterable[str]]

TableName = Union[Callable[[SinkItem], str], str]


class FireboltSinkException(QuixException): ...


class FireboltSink(BatchingSink):
    def __init__(
        self,
        account_name: str,
        username: str,
        password: str,
        db_name: str,
        table_name: TableName,
        engine_name: Optional[str] = None,
        schema_auto_update: bool = True,
        batch_size: int = 1000,
        request_timeout_s: int = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        A connector to sink processed data to Firebolt.

        It batches the processed records in memory per topic partition, converts
        them to the appropriate format, and flushes them to Firebolt at the checkpoint.

        The Firebolt sink transparently handles backpressure if the destination instance
        cannot accept more data at the moment.

        >***NOTE***: FireboltSink can accept only dictionaries.
        > If the record values are not dicts, you need to convert them to dicts before
        > sinking.

        :param account_name: Firebolt account name for authentication
        :param username: Firebolt username for authentication
        :param password: Firebolt password for authentication
        :param db_name: Firebolt db_name name as a string.
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns a string.
        :param table_name: Firebolt table name as either a string or a callable which
            receives a SinkItem and returns a string.
        :param engine_name: Firebolt engine name. If not provided, uses default engine.
        :param schema_auto_update: Automatically update the schema when new columns are detected.
        :param batch_size: how many records to write to Firebolt in one request.
            Note that it only affects the size of one write request, and not the number
            of records flushed on each checkpoint.
            Default - `1000`.
        :param request_timeout_s: request timeout in seconds for Firebolt operations.
            Default - `30`.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """

        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._account_name = account_name
        self._username = username
        self._password = password
        self._engine_name = engine_name
        self._request_timeout_s = request_timeout_s
        self._batch_size = batch_size
        self._schema_auto_update = schema_auto_update

        self._db_name = db_name
        self._table_name = _table_name_setter(table_name)
        self._tables = set()

        self._client: Optional[Connection] = None

    # TODO: with a cleanup method for sinks, we could make a reusable connection
    #  and spawn cursors as-needed.
    @contextlib.contextmanager
    def _connection(self) -> Generator[Connection, None, None]:
        with connect(
            account_name=self._account_name,
            auth=ClientCredentials(self._username, self._password),
            database=self._db_name,
            engine_name=self._engine_name,
        ) as connection:
            yield connection

    def setup(self):
        """Initialize the Firebolt client and authenticate."""

        # Test connection
        try:
            with self._connection() as connection:
                cursor: Cursor = connection.cursor()
                if not cursor.is_db_available(self._db_name):
                    raise FireboltSinkException("database is not available")
            logger.debug("Successfully authenticated to Firebolt")
        except Exception as e:
            logger.error(f"Failed to authenticate to Firebolt: {e}")
            raise

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
                f'Sink "{self.__class__.__name__}" supports only dictionaries,'
                f" got {type(value)}"
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

    def write(self, batch: SinkBatch):
        """Write a batch of records to Firebolt."""
        tables = {}
        for item in batch:
            table = tables.setdefault(
                self._table_name(item), {"rows": [], "cols_types": {}}
            )

            row = {TIMESTAMP_COLUMN_NAME: datetime.fromtimestamp(item.timestamp / 1000)}
            if item.key is not None:
                key_type = type(item.key)
                table["cols_types"].setdefault(KEY_COLUMN_NAME, key_type)
                row[KEY_COLUMN_NAME] = item.key

            for key, value in item.value.items():
                if value is not None:
                    table["cols_types"].setdefault(key, type(value))
                    # Serialize incompatible types to "TEXT" (strings)
                    if isinstance(value, (list, dict, tuple)):
                        row[key] = json.dumps(value)
                    elif isinstance(value, bytes):
                        row[key] = value.decode("utf-8")
                    else:
                        row[key] = value

            table["rows"].append(row)

        table_counts = {}
        try:
            with self._connection() as connection:
                for name, values in tables.items():
                    table_counts[name] = len(values["rows"])
                    if self._schema_auto_update:
                        self._create_table(connection, name)
                        self._add_new_columns(connection, name, values["cols_types"])
                    self._insert_rows(connection, name, values["rows"])
        except Exception as e:
            logger.error(f"Failed to write batch: {str(e)}")
            # Handle potential backpressure or rate limiting
            if "rate limit" in str(e).lower() or "throttle" in str(e).lower():
                raise SinkBackpressureError(retry_after=15) from e
            raise FireboltSinkException(f"Failed to write batch: {str(e)}") from e

        logger.info(
            f"Successfully wrote records to tables; "
            f"table row counts: {table_counts}"
        )

    def _create_table(self, connection: Connection, table_name: str):
        """Create table if it doesn't exist."""
        if table_name in self._tables:
            return

        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
            raise ValueError(f"Unsafe table name: {table_name}")

        cursor = connection.cursor()
        # Create table with basic columns - more will be added dynamically
        query = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {TIMESTAMP_COLUMN_NAME} TIMESTAMP NOT NULL,
                {KEY_COLUMN_NAME} TEXT
            )
        """

        cursor.execute(query)
        self._tables.add(table_name)
        logger.debug(f"Created table {table_name}")

    def _add_new_columns(
        self, connection: Connection, table_name: str, columns: dict[str, type]
    ) -> None:
        """Add new columns to Firebolt table if they don't exist."""
        cursor = connection.cursor()

        for col_name, py_type in columns.items():
            firebolt_col_type = _FIREBOLT_TYPES_MAP.get(py_type)
            if firebolt_col_type is None:
                raise FireboltSinkException(
                    f'Failed to add new column "{col_name}": '
                    f'cannot map Python type "{py_type}" to a Firebolt column type'
                )

            # Firebolt uses ADD COLUMN IF NOT EXISTS syntax
            query = f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS {col_name} {firebolt_col_type}
            """

            cursor.execute(query)
            logger.debug(
                f"Added column {col_name} ({firebolt_col_type}) to table {table_name}"
            )

    def _insert_rows(
        self, connection: Connection, table_name: str, rows: list[dict]
    ) -> None:
        """Insert rows into Firebolt table."""
        if not rows:
            return

        cursor = connection.cursor()

        # Collect all column names from the first row
        columns = list(rows[0].keys())

        # Build single INSERT query for all rows
        query = f"""
        INSERT INTO "{table_name}" 
        ({', '.join(columns)}) 
        VALUES ({', '.join(['?']*len(columns))})
        """  # noqa: S608

        # Execute single batch insert
        cursor.executemany(
            query, [[row.get(col, None) for col in columns] for row in rows]
        )

        logger.debug(f"Inserted {len(rows)} rows into {table_name}")


def _table_name_setter(table_name: TableName) -> Callable[[SinkItem], str]:
    if isinstance(table_name, str):
        return lambda sink_item: table_name
    return table_name
