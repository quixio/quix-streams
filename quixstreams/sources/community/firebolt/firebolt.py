import contextlib
import logging
import re
import time
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from functools import partial, wraps
from typing import Callable, Optional

try:
    from firebolt.client.auth import ClientCredentials
    from firebolt.db import Connection, Cursor, connect
except ImportError as exc:
    raise ImportError(
        'Package "firebolt-sdk" is missing: '
        "run pip install quixstreams[firebolt] to fix it"
    ) from exc

from quixstreams.models.topics import Topic
from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    Source,
)

__all__ = ("FireboltSource",)

logger = logging.getLogger(__name__)

TIME_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

# A column name for the records keys
KEY_COLUMN_NAME = "__key"

# A column name for the records timestamps
TIMESTAMP_COLUMN_NAME = "__timestamp"


def with_retry(func):
    """Retry decorator with exponential backoff."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        max_attempts = self._max_attempts
        attempts_remaining = self._max_attempts
        backoff = 1  # Initial backoff in seconds
        while attempts_remaining:
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logger.debug(f"{func.__name__} encountered an error: {e}")
                attempts_remaining -= 1
                if attempts_remaining:
                    logger.warning(
                        f"{func.__name__} failed and is retrying; "
                        f"backing off for {backoff}s (attempt "
                        f"{max_attempts-attempts_remaining}/{max_attempts})"
                    )
                    time.sleep(backoff)
                    backoff *= 2  # Exponential backoff
                else:
                    logger.error(f"Maximum retries reached for {func.__name__}: {e}")
                    raise

    return wrapper


def _interval_to_seconds(interval: str) -> int:
    """Convert interval string like '5m' to seconds."""
    return int(interval[:-1]) * TIME_UNITS[interval[-1]]


class FireboltSource(Source):
    """
    A source that reads data from Firebolt databases and streams it to Kafka.

    It processes data by gathering/producing a tumbling "time_delta"-sized window
    of data, starting from 'start_date' and eventually stopping at 'end_date'.

    If no 'end_date' is provided, it will run indefinitely.

    Example Usage:

    ```python
    from quixstreams import Application
    from quixstreams.sources.community.firebolt import FireboltSource
    from datetime import datetime, timezone

    source = FireboltSource(
        account_name="your_account",
        username="your_username",
        password="your_password",
        db_name="your_db_name",
        table_name="events",
        time_field="timestamp",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        time_delta="5m",  # 5-minute windows
    )

    app = Application(
        broker_address="localhost:9092",
        consumer_group="firebolt-consumer",
    )

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    if __name__ == "__main__":
        app.run()
    ```
    """

    def __init__(
        self,
        account_name: str,
        username: str,
        password: str,
        db_name: str,
        table_name: str,
        engine_name: Optional[str] = None,
        time_field: str = TIMESTAMP_COLUMN_NAME,
        start_date: datetime = datetime.now(tz=timezone.utc),
        end_date: Optional[datetime] = None,
        time_delta: str = "5m",
        sql_query: Optional[str] = None,
        key_setter: Optional[Callable[[dict], object]] = None,
        timestamp_setter: Optional[Callable[[dict], int]] = None,
        delay: float = 0,
        max_retries: int = 5,
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param account_name: Firebolt account name for authentication
        :param username: Firebolt username for authentication
        :param password: Firebolt password for authentication
        :param db_name: Firebolt db_name name to connect to
        :param table_name: Firebolt table name to query
        :param engine_name: Specific Firebolt engine name to use. If not provided, uses default.
        :param time_field: The timestamp column name used for time-windowed queries
        :param start_date: The start datetime for querying. Uses current time by default.
        :param end_date: The end datetime for querying. If None, runs indefinitely.
        :param time_delta: Time interval for batching queries, e.g., "5m" for 5 minutes.
        :param sql_query: Custom SQL query. Must include {start_time} and {end_time} placeholders.
            Default: "SELECT * FROM {table_name} WHERE {time_field} >= {start_time} AND {time_field} < {end_time}"
        :param key_setter: Function to set the Kafka message key from each record.
            By default, uses __key column if present.
        :param timestamp_setter: Function to set the Kafka message timestamp from each record.
            By default, uses __timestamp column if present.
        :param delay: Optional delay in seconds between producing batches.
        :param max_retries: Maximum number of retries for querying or producing.
        :param name: Unique name for the source, used for topic naming.
        :param shutdown_timeout: Time to wait for graceful shutdown.
        :param on_client_connect_success: Callback for successful connection.
        :param on_client_connect_failure: Callback for connection failures.
        """
        super().__init__(
            name=name or f"firebolt_{db_name}_{table_name}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._account_name = account_name
        self._username = username
        self._password = password
        self._db_name = db_name
        self._table_name = table_name
        self._engine_name = engine_name
        self._start_date = start_date
        self._end_date = end_date
        self._time_delta_seconds = _interval_to_seconds(time_delta)
        self._key_setter = key_setter or _default_key_setter
        self._timestamp_setter = timestamp_setter or _set_default_time_setter(
            time_field
        )
        self._delay = delay
        self._max_attempts = max_retries + 1

        for field in [time_field, table_name]:
            _validate_sql_field_name(field)

        self._sql_query = (
            sql_query
            or f"""
                SELECT * 
                FROM "{table_name}" 
                WHERE "{time_field}" >= ? 
                AND "{time_field}" < ?
            """  # noqa: S608
        )

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
        """Initialize the Firebolt client and validate connection."""
        # Test connection by attempting to execute a simple query
        try:
            with self._connection() as connection:
                cursor: Cursor = connection.cursor()
                if not cursor.is_db_available(self._db_name):
                    raise Exception("database is not available")
                cursor.execute("SELECT 1")
            logger.info(f"Successfully connected to Firebolt database: {self._db_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Firebolt: {e}")
            raise

    @with_retry
    def _query_data(self, start_time: datetime, end_time: datetime) -> list[dict]:
        """Execute time-windowed query and return results as list of dictionaries."""
        logger.info(
            f"Executing query for {self._table_name} FROM '{start_time}' TO '{end_time}'"
        )
        try:
            with self._connection() as connection:
                cursor: Cursor = connection.cursor()
                cursor.execute(
                    self._sql_query,
                    [start_time, end_time],
                )

                # Fetch column names
                column_names = (
                    [desc[0] for desc in cursor.description]
                    if cursor.description
                    else []
                )

                # Fetch all results
                results = []
                for row in cursor.fetchall():
                    record = dict(zip(column_names, row))
                    results.append(record)

                logger.info(f"Query returned {len(results)} records")
                return results

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    @with_retry
    def _produce_records(self, records: list[dict]):
        """Produce records to Kafka topic."""
        for record in records:
            msg = self.serialize(
                key=self._key_setter(record),
                value=record,
                timestamp_ms=self._timestamp_setter(record)
                if self._timestamp_setter
                else None,
            )
            self.produce(
                key=msg.key,
                value=msg.value,
                timestamp=msg.timestamp,
            )
        self.producer.flush()
        logger.info(f"Produced {len(records)} records to Kafka")

    def _should_continue(self, current_time: datetime) -> bool:
        """Check if processing should continue based on end_date and running status."""
        if not self.running:
            logger.info("Stopping table processing...")
            return False
        if not self._end_date or (current_time < self._end_date):
            return True
        logger.info(f"Reached defined end_date: {self._end_date}")
        return False

    def _process_table(self):
        """Process table data in time-windowed batches."""
        logger.info(f"Processing table: {self._table_name}")
        start_time = self._start_date

        while self._should_continue(start_time):
            end_time = start_time + timedelta(seconds=self._time_delta_seconds)

            # If no end_date, wait for time window to complete
            if self._end_date is None:
                if wait_time := max(
                    0.0, (end_time - datetime.now(timezone.utc)).total_seconds()
                ):
                    logger.info(
                        f"At current time; sleeping for {wait_time}s "
                        f"to allow a {self._time_delta_seconds}s query window"
                    )
                    time.sleep(wait_time)

            # Query and produce data for this time window
            records = self._query_data(start_time, end_time)
            if records:
                self._produce_records(records)

            # Move to next time window
            start_time = end_time

            # Optional delay between batches
            if self._delay > 0:
                logger.debug(f"Applying query delay of {self._delay}s")
                time.sleep(self._delay)

        logger.debug(f"Ended processing for {self._table_name}.")

    def run(self):
        """Main execution loop."""
        try:
            self._process_table()
        except Exception as e:
            logger.error(f"Error in FireboltSource run loop: {e}")
            raise
        finally:
            logger.info(f"Finished processing table {self._table_name}.")

    def default_topic(self) -> Topic:
        """Return default topic configuration."""
        return Topic(
            name=self.name,
            key_serializer="string",
            key_deserializer="string",
            value_deserializer="json",
            value_serializer="json",
        )


def _default_key_setter(record: dict) -> Optional[object]:
    """Default key setter returns __key column value if present."""
    return record.pop(KEY_COLUMN_NAME)


def _default_time_setter(time_col: str, record: dict) -> Optional[int]:
    if timestamp := record.pop(time_col):
        if isinstance(timestamp, datetime):
            timestamp = timestamp.timestamp() * 1e3
        return int(timestamp)


def _set_default_time_setter(time_col: str) -> Callable[[dict], Optional[int]]:
    """
    Returns a default timestamp setter which returns the specified time column
    value if present.
    """

    return partial(_default_time_setter, time_col)


def _validate_sql_field_name(field):
    """
    Ensure field name does not include any escapes.
    """
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", field):
        raise ValueError(f"Unsafe field name: {field}")
