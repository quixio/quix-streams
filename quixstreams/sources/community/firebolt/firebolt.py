import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

try:
    from firebolt.client import Client
    from firebolt.client.auth import UsernamePassword
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


class FireboltSource(Source):
    """
    A source that reads data from Firebolt databases and streams it to Kafka.

    This source connects to a Firebolt database, executes SQL queries to fetch data,
    and produces the results to a Kafka topic using Quix Streams transformations.

    The source can operate in two modes:
    1. **Batch mode**: Execute a query once and stream all results
    2. **Streaming mode**: Execute queries at regular intervals (e.g., for incremental data)

    Example Usage:

    ```python
    from quixstreams import Application
    from quixstreams.sources.community.firebolt import FireboltSource

    source = FireboltSource(
        username="your_username",
        password="your_password",
        database="your_database",
        query="SELECT * FROM events WHERE timestamp >= ?",
        poll_interval=300,  # Query every 5 minutes
        parameters=["2024-01-01T00:00:00"],
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
        database: str,
        query: str,
        engine_name: Optional[str] = None,
        parameters: Optional[list] = None,
        poll_interval: Optional[float] = None,
        key_extractor: Optional[Callable[[dict], Any]] = None,
        timestamp_extractor: Optional[Callable[[dict], int]] = None,
        batch_size: int = 1000,
        query_timeout: int = 300,
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param account_name: Firebolt account name for authentication
        :param username: Firebolt username for authentication
        :param password: Firebolt password for authentication
        :param database: Firebolt database name to connect to
        :param query: SQL query to execute. Use ? for parameterized queries.
        :param engine_name: Specific Firebolt engine name to use. If not provided, uses default.
        :param parameters: List of parameters for parameterized queries.
        :param poll_interval: If provided, the source will run continuously, executing
            the query every poll_interval seconds. If None, runs the query once.
        :param key_extractor: Function to extract the message key from each record.
            If not provided, uses row index as key.
        :param timestamp_extractor: Function to extract timestamp (in milliseconds)
            from each record. If not provided, uses current time.
        :param batch_size: Number of records to fetch and process in each batch.
        :param query_timeout: Query execution timeout in seconds.
        :param name: Unique name for the source, used for topic naming.
        :param shutdown_timeout: Time to wait for graceful shutdown.
        :param on_client_connect_success: Callback for successful connection.
        :param on_client_connect_failure: Callback for connection failures.
        """
        super().__init__(
            name=name or f"firebolt_{database}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._account_name = account_name
        self._auth = UsernamePassword(username, password)
        self._database = database
        self._query = query
        self._engine_name = engine_name
        self._parameters = parameters or []
        self._poll_interval = poll_interval
        self._key_extractor = key_extractor or self._default_key_extractor
        self._timestamp_extractor = timestamp_extractor
        self._batch_size = batch_size
        self._query_timeout = query_timeout

        self._client: Optional[Client] = None
        self._last_poll_time: Optional[datetime] = None

    def setup(self):
        """Initialize the Firebolt client and validate connection."""

        self._client = Client(account_name=self._account_name, auth=self._auth)

        # Test connection by attempting to get database info
        try:
            database = self._client.get_database(self._database)
            if self._engine_name:
                database.bind_to_engine(self._engine_name)

            # Validate that we can execute a simple query
            database.cursor().execute("SELECT 1")
            logger.info(
                f"Successfully connected to Firebolt database: {self._database}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Firebolt: {e}")
            raise

    def _default_key_extractor(self, record: dict) -> str:
        """Default key extractor that creates a key from record hash."""
        return str(hash(json.dumps(record, sort_keys=True)))

    def _execute_query(self) -> list[dict]:
        """Execute the configured query and return results as list of dictionaries."""
        try:
            database = self._client.get_database(self._database)
            if self._engine_name:
                database.bind_to_engine(self._engine_name)

            cursor = database.cursor()

            logger.info(f"Executing query: {self._query}")
            if self._parameters:
                cursor.execute(self._query, self._parameters)
            else:
                cursor.execute(self._query)

            # Fetch column names
            column_names = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )

            # Fetch results in batches
            results = []
            while True:
                batch = cursor.fetchmany(self._batch_size)
                if not batch:
                    break

                # Convert rows to dictionaries
                for row in batch:
                    record = dict(zip(column_names, row))
                    results.append(record)

            logger.info(f"Query returned {len(results)} records")
            return results

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def _produce_records(self, records: list[dict]):
        """Produce records to Kafka topic."""
        for record in records:
            # Extract key and timestamp
            key = self._key_extractor(record)
            timestamp_ms = None

            if self._timestamp_extractor:
                timestamp_ms = self._timestamp_extractor(record)

            # Serialize and produce message
            msg = self.serialize(
                key=key,
                value=record,
                timestamp_ms=timestamp_ms,
            )

            self.produce(
                key=msg.key,
                value=msg.value,
                timestamp=msg.timestamp,
            )

        # Flush the producer to ensure messages are sent
        self.flush()
        logger.info(f"Produced {len(records)} records to Kafka")

    def _should_poll(self) -> bool:
        """Check if it's time to poll based on poll_interval."""
        if self._poll_interval is None:
            # One-time execution mode
            return self._last_poll_time is None

        if self._last_poll_time is None:
            return True

        time_since_last_poll = (
            datetime.now(timezone.utc) - self._last_poll_time
        ).total_seconds()
        return time_since_last_poll >= self._poll_interval

    def run(self):
        """Main execution loop."""
        try:
            while self.running:
                if self._should_poll():
                    logger.info("Starting query execution cycle")
                    self._last_poll_time = datetime.now(timezone.utc)

                    # Execute query and produce results
                    records = self._execute_query()
                    if records:
                        self._produce_records(records)
                    else:
                        logger.info("No records returned from query")

                    # If this is one-time execution, stop after first run
                    if self._poll_interval is None:
                        logger.info("One-time execution completed, stopping source")
                        break

                # Sleep briefly to prevent busy waiting
                time.sleep(1.0)

        except Exception as e:
            logger.error(f"Error in FireboltSource run loop: {e}")
            raise
        finally:
            if self._client:
                try:
                    self._client.close()
                except Exception as e:
                    logger.warning(f"Error closing Firebolt client: {e}")

    def default_topic(self) -> Topic:
        """Return default topic configuration."""
        return Topic(
            name=self.name,
            key_serializer="str",
            key_deserializer="str",
            value_deserializer="json",
            value_serializer="json",
        )
