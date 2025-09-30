import logging
import time
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from quixstreams.models import HeadersTuples

try:
    from firebolt.client import Client
    from firebolt.client.auth import UsernamePassword
    from firebolt.common import Settings
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

logger = logging.getLogger(__name__)

FireboltValueMap = dict[str, Union[str, int, float, bool]]

TableCallable = Callable[[FireboltValueMap], str]
DatabaseCallable = Callable[[FireboltValueMap], str]
ColumnsCallable = Callable[[FireboltValueMap], Iterable[str]]

TableSetter = Union[str, TableCallable]
DatabaseSetter = Union[str, DatabaseCallable]
ColumnsSetter = Union[Iterable[str], ColumnsCallable]


class FireboltSink(BatchingSink):
    def __init__(
        self,
        username: str,
        password: str,
        database: DatabaseSetter,
        table: TableSetter,
        api_endpoint: Optional[str] = None,
        engine_name: Optional[str] = None,
        columns: ColumnsSetter = (),
        batch_size: int = 1000,
        request_timeout_s: int = 30,
        include_metadata_columns: bool = False,
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

        :param username: Firebolt username for authentication
        :param password: Firebolt password for authentication
        :param database: Firebolt database name as a string.
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns a string.
        :param table: Firebolt table name as a string.
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns a string.
        :param api_endpoint: Firebolt API endpoint URL. If not provided, uses default.
        :param engine_name: Firebolt engine name. If not provided, uses default engine.
        :param columns: an iterable (list) of strings used as column names for insertion.
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns an iterable of strings.
            - If empty, all keys from the record value will be used.
            Default - `()`.
        :param batch_size: how many records to write to Firebolt in one request.
            Note that it only affects the size of one write request, and not the number
            of records flushed on each checkpoint.
            Default - `1000`.
        :param request_timeout_s: request timeout in seconds for Firebolt operations.
            Default - `30`.
        :param include_metadata_columns: if True, includes record's key, topic,
            and partition as additional columns.
            Default - `False`.
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

        self._username = username
        self._password = password
        self._api_endpoint = api_endpoint
        self._engine_name = engine_name
        self._request_timeout_s = request_timeout_s
        self._include_metadata_columns = include_metadata_columns
        self._batch_size = batch_size

        self._database = _database_callable(database)
        self._table = _table_callable(table)
        self._columns = _columns_callable(columns)

        self._client: Optional[Client] = None

    def setup(self):
        """Initialize the Firebolt client and authenticate."""
        auth = UsernamePassword(self._username, self._password)
        settings = Settings(
            server=self._api_endpoint,
            default_region="us-east-1",  # Default region, can be made configurable
        )

        self._client = Client(auth=auth, settings=settings)

        # Test connection by attempting to get client info
        try:
            # This will validate the connection and authentication
            self._client.resource_manager.engines.get_many()
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
        database_name = self._database
        table_name = self._table
        columns_getter = self._columns

        for write_batch in batch.iter_chunks(n=self._batch_size):
            # Group records by database and table
            db_table_records = {}

            for item in write_batch:
                value = item.value.copy()

                # Evaluate database and table names for this record
                _database = database_name(value)
                _table = table_name(value)
                _columns = columns_getter(value)

                # Add metadata columns if requested
                if self._include_metadata_columns:
                    value["__key"] = item.key
                    value["__topic"] = batch.topic
                    value["__partition"] = batch.partition
                    value["__timestamp"] = item.timestamp
                    value["__offset"] = item.offset

                # Filter columns if specified
                if _columns:
                    filtered_value = {k: value[k] for k in _columns if k in value}
                else:
                    filtered_value = value

                # Group by database and table
                db_table_key = (_database, _table)
                if db_table_key not in db_table_records:
                    db_table_records[db_table_key] = {"rows": [], "columns": None}

                # Determine columns from first record or use all available
                if db_table_records[db_table_key]["columns"] is None:
                    db_table_records[db_table_key]["columns"] = list(
                        filtered_value.keys()
                    )

                # Create row as list of values in column order
                row = [
                    filtered_value.get(col)
                    for col in db_table_records[db_table_key]["columns"]
                ]
                db_table_records[db_table_key]["rows"].append(row)

            # Write to each database/table combination
            for (db_name, tbl_name), record_data in db_table_records.items():
                try:
                    _start = time.monotonic()

                    # Get database and table objects
                    database = self._client.get_database(db_name)
                    if self._engine_name:
                        database.bind_to_engine(self._engine_name)

                    table = database.get_table(tbl_name)

                    # Write rows to Firebolt
                    table.write_rows(record_data["rows"])

                    elapsed = round(time.monotonic() - _start, 2)
                    logger.info(
                        f"Sent data to Firebolt; "
                        f"database={db_name} table={tbl_name} "
                        f"total_records={len(record_data['rows'])} "
                        f"time_elapsed={elapsed}s"
                    )

                except Exception as exc:
                    # Handle potential backpressure or rate limiting
                    if (
                        "rate limit" in str(exc).lower()
                        or "throttle" in str(exc).lower()
                    ):
                        # Raise backpressure error to pause the partition
                        raise SinkBackpressureError(retry_after=15) from exc

                    logger.error(f"Failed to write to Firebolt: {exc}")
                    raise


def _database_callable(setter: DatabaseSetter) -> DatabaseCallable:
    """Convert database setter to callable."""
    if callable(setter):
        return setter
    return lambda value: setter


def _table_callable(setter: TableSetter) -> TableCallable:
    """Convert table setter to callable."""
    if callable(setter):
        return setter
    return lambda value: setter


def _columns_callable(setter: ColumnsSetter) -> ColumnsCallable:
    """Convert columns setter to callable."""
    if callable(setter):
        return setter
    return lambda value: setter
