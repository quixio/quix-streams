import json
import logging
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Callable, Optional, Union

try:
    from influxdb_client_3 import InfluxDBClient3
except ImportError as exc:
    raise ImportError(
        'Package "influxdb3-python" is missing: '
        "run pip install quixstreams[influxdb3] to fix it"
    ) from exc

from quixstreams.models.topics import Topic
from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    Source,
)

__all__ = ("InfluxDB3Source",)


logger = logging.getLogger(__name__)

TIME_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


class NoMeasurementsFound(Exception): ...


# retry decorator
def with_retry(func):
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
    return int(interval[:-1]) * TIME_UNITS[interval[-1]]


def _set_sql_query(sql_query: str) -> str:
    return (
        (
            sql_query.replace("{start_time}", "$start_time").replace(
                "{end_time}", "$end_time"
            )
        )
        or "SELECT * FROM {measurement_name} "  # noqa: S608
        "WHERE time >= $start_time "
        "AND time < $end_time"
    )


class InfluxDB3Source(Source):
    """
    InfluxDB3Source extracts data from a specified set of measurements in a
      database (or all available ones if none are specified).

    It processes measurements sequentially by gathering/producing a tumbling
      "time_delta"-sized window of data, starting from 'start_date' and eventually
      stopping at 'end_date', completing that measurement.

    It then starts the next measurement, continuing until all are complete.

    If no 'end_date' is provided, it will run indefinitely for a single
      measurement (which means no other measurements will be processed!).
    """

    def __init__(
        self,
        host: str,
        token: str,
        organization_id: str,
        database: str,
        key_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        start_date: datetime = datetime.now(tz=timezone.utc),
        end_date: Optional[datetime] = None,
        measurements: Optional[Union[str, list[str]]] = None,
        measurement_column_name: str = "_measurement_name",
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
        :param host: Host URL of the InfluxDB instance.
        :param token: Authentication token for InfluxDB.
        :param organization_id: Organization name in InfluxDB.
        :param database: Database name in InfluxDB.
        :param key_setter: sets the kafka message key for a measurement record.
            By default, will set the key to the measurement's name.
        :param timestamp_setter: sets the kafka message timestamp for a measurement record.
            By default, the timestamp will be the Kafka default (Kafka produce time).
        :param start_date: The start datetime for querying InfluxDB. Uses current time by default.
        :param end_date: The end datetime for querying InfluxDB.
            If none provided, runs indefinitely for a single measurement.
        :param measurements: The measurements to query. If None, all measurements will be processed.
        :param measurement_column_name: The column name used for appending the measurement name to the record.
        :param sql_query: Custom SQL query for retrieving data.
            Query expects a `{start_time}`, `{end_time}`, and `{measurement_name}` for later formatting.
            If provided, it overrides the default window-query logic.
        :param time_delta: Time interval for batching queries, e.g., "5m" for 5 minutes.
        :param delay: An optional delay between producing batches.
        :param name: A unique name for the Source, used as part of the topic name.
        :param shutdown_timeout: Time in seconds to wait for graceful shutdown.
        :param max_retries: Maximum number of retries for querying or producing.
            Note that consecutive retries have a multiplicative backoff.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        if isinstance(measurements, str):
            measurements = [measurements]
        measurements = measurements or []
        super().__init__(
            name=name
            or f"influxdb3_{database}_{'-'.join(measurements) or 'measurements'}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._client_kwargs = {
            "host": host,
            "token": token,
            "org": organization_id,
            "database": database,
        }
        self._measurement_column_name = measurement_column_name
        self._key_setter = key_setter or self._default_key_setter
        self._timestamp_setter = timestamp_setter
        self._measurements = measurements
        self._sql_query = _set_sql_query(sql_query or "")
        self._start_date = start_date
        self._end_date = end_date
        self._time_delta_seconds = _interval_to_seconds(time_delta)
        self._delay = delay
        self._max_attempts = max_retries + 1

        self._client: Optional[InfluxDBClient3] = None

    def setup(self):
        if not self._client:
            self._client = InfluxDBClient3(**self._client_kwargs)
            self._client.query(
                query="SHOW MEASUREMENTS", mode="pandas", language="influxql"
            )

    def _close_client(self):
        if self._client:
            self._client.close()
            self._client = None

    def _default_key_setter(self, record: dict):
        return record[self._measurement_column_name]

    @property
    def _measurement_names(self) -> list[str]:
        if not self._measurements:
            self._measurements = self._get_measurements()
        return self._measurements

    def _get_measurements(self) -> list[str]:
        try:
            result = self._client.query(
                query="SHOW MEASUREMENTS", mode="pandas", language="influxql"
            )
        except Exception as e:
            logger.error(f"Failed to retrieve measurements: {e}")
            raise
        else:
            if result.empty:
                raise NoMeasurementsFound(
                    "query 'SHOW MEASUREMENTS' returned an empty result set"
                )
            return result["name"].tolist()

    @with_retry
    def _produce_records(self, records: list[dict]):
        for record in records:
            msg = self.serialize(
                key=self._key_setter(record),
                value=record,
                timestamp_ms=self._timestamp_setter(record)
                if self._timestamp_setter
                else None,
            )
            self.produce(value=msg.value, key=msg.key, timestamp=msg.timestamp)
        self.producer.flush()

    @with_retry
    def _query_data(self, measurement_name, start_time, end_time):
        logger.info(
            f"Executing query for {measurement_name} FROM '{start_time}' TO '{end_time}'"
        )
        try:
            return self._client.query(
                query=self._sql_query.format(measurement_name=measurement_name),
                mode="pandas",
                language="influxql",
                query_parameters={
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                },
            )
        except Exception as e:
            logger.error(f"Query failed for measurement {measurement_name}: {e}")
            raise

    def _do_measurement_processing(self, current_time: datetime) -> bool:
        if not self.running:
            logger.info("Stopping all measurement processing...")
            return False
        if not self._end_date or (current_time < self._end_date):
            return True
        logger.info(f"Measurement is now at defined end_date: {self._end_date}")
        return False

    def _process_measurement(self, measurement_name):
        logger.info(f"Processing measurement: {measurement_name}")
        start_time = self._start_date

        while self._do_measurement_processing(start_time):
            end_time = start_time + timedelta(seconds=self._time_delta_seconds)

            # TODO: maybe allow querying more frequently once "caught up"?
            if self._end_date is None:
                if wait_time := max(
                    0.0, (end_time - datetime.now(timezone.utc)).total_seconds()
                ):
                    logger.info(
                        f"At current time; sleeping for {wait_time}s "
                        f"to allow a {self._time_delta_seconds}s query window"
                    )
                    time.sleep(wait_time)

            data = self._query_data(measurement_name, start_time, end_time)
            if data is not None and not data.empty:
                if "iox::measurement" in data.columns:
                    data = data.drop(columns=["iox::measurement"])
                data[self._measurement_column_name] = measurement_name
                self._produce_records(
                    json.loads(data.to_json(orient="records", date_format="iso")),
                )

            start_time = end_time
            if self._delay > 0:
                logger.debug(f"Applying query delay of {self._delay}s")
                time.sleep(self._delay)
        logger.debug(f"Ended processing for {measurement_name}.")

    def run(self):
        if len(self._measurement_names) > 1 and not self._end_date:
            logger.warning(
                "More than one measurement was found and no end_date "
                f"was specified; only measurement '{self._measurement_names[0]}' "
                f"will be processed!"
            )
        measurement_names = iter(self._measurement_names)
        try:
            while self.running:
                self._process_measurement(next(measurement_names))
        except StopIteration:
            logger.info("Finished processing all measurements.")
        finally:
            logger.info("Stopping InfluxDB3 client...")
            self._close_client()

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_serializer="string",
            key_deserializer="string",
            value_deserializer="json",
            value_serializer="json",
        )
