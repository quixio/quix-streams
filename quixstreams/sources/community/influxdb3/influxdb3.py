import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Optional

from influxdb_client_3 import InfluxDBClient3

from quixstreams.models.topics import Topic
from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    Source,
)

logger = logging.getLogger(__name__)

TIME_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}

__all__ = ("InfluxDB3Source",)


# retry decorator
def with_retry(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        max_attempts = self.max_attempts
        attempts_remaining = self.max_attempts
        backoff = 1  # Initial backoff in seconds
        while attempts_remaining:
            try:
                return func(*args, **kwargs)
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


class InfluxDB3Source(Source):
    def __init__(
        self,
        influxdb_host: str,
        influxdb_token: str,
        influxdb_org: str,
        influxdb_database: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        measurement: Optional[str] = None,
        sql_query: Optional[str] = None,
        time_delta: str = "5m",
        delay: float = 0,
        max_retries: int = 5,
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        """
        :param influxdb_host: Host URL of the InfluxDB instance.
        :param influxdb_token: Authentication token for InfluxDB.
        :param influxdb_org: Organization name in InfluxDB.
        :param influxdb_database: Database name in InfluxDB.
        :param start_date: The start datetime for querying InfluxDB.
        :param end_date: The end datetime for querying InfluxDB.
        :param measurement: The measurement to query. If None, all measurements will be processed.
        :param sql_query: Custom SQL query for retrieving data. If provided, it overrides the default logic.
        :param time_delta: Time interval for batching queries, e.g., "5m" for 5 minutes.
        :param delay: An optional delay between producing batches.
        :param name: A unique name for the Source, used as part of the topic name.
        :param shutdown_timeout: Time in seconds to wait for graceful shutdown.
        :param max_retries: Maximum number of retries for querying or producing.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            name=name or f"influxdb_{influxdb_database}_{measurement}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._client_kwargs = {
            "host": influxdb_host,
            "token": influxdb_token,
            "org": influxdb_org,
            "database": influxdb_database,
        }
        self.measurement = measurement
        self.sql_query = sql_query  # Custom SQL query
        self.start_date = start_date
        self.end_date = end_date
        self.time_delta = time_delta
        self.delay = delay
        self.max_retries = max_retries

        self._client: Optional[InfluxDBClient3] = None

    def setup(self):
        self._client = InfluxDBClient3(**self._client_kwargs)
        try:
            # We cannot safely parameterize the table (measurement) selection, so
            # the best we can do is confirm authentication was successful
            self._client.query("")
        except Exception as e:
            if "No SQL statements were provided in the query string" not in str(e):
                raise

    @with_retry
    def produce_records(self, records: list[dict], measurement_name: str):
        for record in records:
            msg = self.serialize(
                key=f"{measurement_name}_{random.randint(1, 1000)}",  # noqa: S311
                value=record,
            )
            self.produce(value=msg.value, key=msg.key)
        self.producer.flush()

    def run(self):
        measurements = (
            [self.measurement] if self.measurement else self.get_measurements()
        )

        for measurement_name in measurements:
            logger.info(f"Processing measurement: {measurement_name}")
            _start_date = self.start_date
            _end_date = self.end_date

            is_running = (
                True
                if _end_date is None
                else (self.running and _start_date < _end_date)
            )

            while is_running:
                end_time = _start_date + timedelta(
                    seconds=interval_to_seconds(self.time_delta)
                )
                wait_time = (end_time - datetime.now(timezone.utc)).total_seconds()
                if _end_date is None and wait_time > 0:
                    logger.info(f"Sleeping for {wait_time}s")
                    time.sleep(wait_time)

                data = self.query_data(measurement_name, _start_date, end_time)
                if data is not None and not data.empty:
                    if "iox::measurement" in data.columns:
                        data = data.drop(columns=["iox::measurement"])
                    self.produce_records(
                        json.loads(data.to_json(orient="records", date_format="iso")),
                        data["measurement_name"],
                    )

                _start_date = end_time
                if self.delay > 0:
                    time.sleep(self.delay)

    def get_measurements(self):
        try:
            query = "SHOW MEASUREMENTS"
            result = self._client.query(query=query, mode="pandas", language="influxql")
            return result["name"].tolist() if not result.empty else []
        except Exception as e:
            logger.error(f"Failed to retrieve measurements: {e}")
            return []

    @with_retry
    def query_data(self, measurement_name, start_time, end_time):
        try:
            if self.sql_query:
                query = self.sql_query.format(
                    measurement_name=measurement_name,
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                )
            else:
                query = (
                    f'SELECT * FROM "{measurement_name}" '  # noqa: S608
                    f"WHERE time >= '{start_time.isoformat()}' "
                    f"AND time < '{end_time.isoformat()}'"
                )

            logger.info(f"Executing query: {query}")
            return self._client.query(query=query, mode="pandas", language="influxql")
        except Exception as e:
            logger.error(f"Query failed for measurement {measurement_name}: {e}")
            raise

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_serializer="string",
            key_deserializer="string",
            value_deserializer="json",
            value_serializer="json",
        )


def interval_to_seconds(interval: str) -> int:
    return int(interval[:-1]) * TIME_UNITS[interval[-1]]
