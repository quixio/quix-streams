import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Literal, Mapping, Optional, Union, get_args

from quixstreams.models import HeadersTuples

try:
    import influxdb_client_3
    from influxdb_client_3 import InfluxDBClient3, WriteOptions, WritePrecision
    from influxdb_client_3.write_client.client.write_api import WriteType
except ImportError as exc:
    raise ImportError(
        'Package "influxdb3-python" is missing: '
        "run pip install quixstreams[influxdb3] to fix it"
    ) from exc

from quixstreams.sinks.base import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBackpressureError,
    SinkBatch,
)

logger = logging.getLogger(__name__)


TimePrecision = Literal["ms", "ns", "us", "s"]
TIME_PRECISION_LEN = {
    "s": 10,
    "ms": 13,
    "ns": 16,
    "us": 19,
}

InfluxDBValueMap = dict[str, Union[str, int, float, bool]]

FieldsCallable = Callable[[InfluxDBValueMap], Iterable[str]]
MeasurementCallable = Callable[[InfluxDBValueMap], str]
TagsCallable = Callable[[InfluxDBValueMap], Iterable[str]]
TimeCallable = Callable[[InfluxDBValueMap], Optional[Union[str, int, datetime]]]

FieldsSetter = Union[Iterable[str], FieldsCallable]
MeasurementSetter = Union[str, MeasurementCallable]
TagsSetter = Union[Iterable[str], TagsCallable]
TimeSetter = Union[str, TimeCallable]


class InfluxDB3Sink(BatchingSink):
    _TIME_PRECISIONS = {
        "s": WritePrecision.S,
        "ms": WritePrecision.MS,
        "ns": WritePrecision.NS,
        "us": WritePrecision.US,
    }

    def __init__(
        self,
        token: str,
        host: str,
        organization_id: str,
        database: str,
        measurement: MeasurementSetter,
        fields_keys: FieldsSetter = (),
        tags_keys: TagsSetter = (),
        time_setter: Optional[TimeSetter] = None,
        time_precision: TimePrecision = "ms",
        allow_missing_fields: bool = False,
        include_metadata_tags: bool = False,
        convert_ints_to_floats: bool = False,
        batch_size: int = 1000,
        enable_gzip: bool = True,
        request_timeout_ms: int = 10_000,
        debug: bool = False,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        A connector to sink processed data to InfluxDB v3.

        It batches the processed records in memory per topic partition, converts
        them to the InfluxDB format, and flushes them to InfluxDB at the checkpoint.

        The InfluxDB sink transparently handles backpressure if the destination instance
        cannot accept more data at the moment
        (e.g., when InfluxDB returns an HTTP 429 error with the "retry_after" header set).
        When this happens, the sink will notify the Application to pause consuming
        from the backpressured topic partition until the "retry_after" timeout elapses.

        >***NOTE***: InfluxDB3Sink can accept only dictionaries.
        > If the record values are not dicts, you need to convert them to dicts before
        > sinking.

        :param token: InfluxDB access token
        :param host: InfluxDB host in format "https://<host>"
        :param organization_id: InfluxDB organization_id
        :param database: database name
        :param measurement: measurement name as a string.
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns a string.
        :param fields_keys: an iterable (list) of strings used as InfluxDB "fields".
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns an iterable of strings.
            - If present, it must not overlap with "tags_keys".
            - If empty, the whole record value will be used.
            >***NOTE*** The fields' values can only be strings, floats, integers, or booleans.
            Default - `()`.
        :param tags_keys: an iterable (list) of strings used as InfluxDB "tags".
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns an iterable of strings.
            - If present, it must not overlap with "fields_keys".
            - Given keys are popped from the value dictionary since the same key
            cannot be both a tag and field.
            - If empty, no tags will be sent.
            >***NOTE***: InfluxDB client always converts tag values to strings.
            Default - `()`.
        :param time_setter: an optional column name to use as "time" for InfluxDB.
            Also accepts a callable which receives the current message data and
            returns either the desired time or `None` (use default).
            The time can be an `int`, `string` (RFC3339 format), or `datetime`.
            The time must match the `time_precision` argument if not a `datetime` object, else raises.
            By default, a record's kafka timestamp with "ms" time precision is used.
        :param time_precision: a time precision to use when writing to InfluxDB.
            Possible values: "ms", "ns", "us", "s".
            Default - `"ms"`.
        :param allow_missing_fields: if `True`, skip the missing fields keys, else raise `KeyError`.
            Default - `False`
        :param include_metadata_tags: if True, includes record's key, topic,
            and partition as tags.
            Default - `False`.
        :param convert_ints_to_floats: if True, converts all integer values to floats.
            Default - `False`.
        :param batch_size: how many records to write to InfluxDB in one request.
            Note that it only affects the size of one write request, and not the number
            of records flushed on each checkpoint.
            Default - `1000`.
        :param enable_gzip: if True, enables gzip compression for writes.
            Default - `True`.
        :param request_timeout_ms: an HTTP request timeout in milliseconds.
            Default - `10000`.
        :param debug: if True, print debug logs from InfluxDB client.
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

        if time_precision not in (time_args := get_args(TimePrecision)):
            raise ValueError(
                f"Invalid 'time_precision' argument {time_precision}; "
                f"valid options: {time_args}"
            )
        if not callable(fields_keys) and not callable(tags_keys):
            fields_tags_keys_overlap = set(fields_keys) & set(tags_keys)
            if fields_tags_keys_overlap:
                overlap_str = ",".join(str(k) for k in fields_tags_keys_overlap)
                raise ValueError(
                    f'Keys {overlap_str} are present in both "fields_keys" and "tags_keys"'
                )

        self._client_args = {
            "token": token,
            "host": host,
            "org": organization_id,
            "database": database,
            "debug": debug,
            "enable_gzip": enable_gzip,
            "timeout": request_timeout_ms,
            "write_client_options": {
                "write_options": WriteOptions(
                    write_type=WriteType.synchronous,
                )
            },
        }
        self._client: Optional[InfluxDBClient3] = None
        self._measurement = _measurement_callable(measurement)
        self._fields_keys = _fields_callable(fields_keys)
        self._tags_keys = _tags_callable(tags_keys)
        self._time_setter = _time_callable(time_setter)
        self._include_metadata_tags = include_metadata_tags
        self._write_precision = self._TIME_PRECISIONS[time_precision]
        self._batch_size = batch_size
        self._allow_missing_fields = allow_missing_fields
        self._convert_ints_to_floats = convert_ints_to_floats

    def setup(self):
        self._client = InfluxDBClient3(**self._client_args)
        try:
            # We cannot safely parameterize the table (measurement) selection, so
            # the best we can do is confirm authentication was successful
            self._client.query("")
        except Exception as e:
            e_str = str(e)
            if not (
                "No SQL statements were provided in the query string" in e_str
                or "database not found" in e_str  # attempts making db when writing
            ):
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
        measurement = self._measurement
        fields_keys = self._fields_keys
        tags_keys = self._tags_keys
        time_setter = self._time_setter
        for write_batch in batch.iter_chunks(n=self._batch_size):
            records = []

            min_timestamp = None
            max_timestamp = None

            for item in write_batch:
                value = item.value
                # Evaluate these before we alter the value
                _measurement = measurement(value)
                _tags_keys = tags_keys(value)
                _fields_keys = fields_keys(value)
                ts = time_setter(value)

                tags = {}
                for tag_key in _tags_keys:
                    # TODO: InfluxDB client always converts tags values to strings
                    #  by doing str().
                    #  We may add some extra validation here in the future to prevent
                    #  unwanted conversion.
                    if tag_key in value:
                        tag = value.pop(tag_key)
                        tags[tag_key] = tag

                if ts is None:
                    ts = item.timestamp
                # Note: currently NOT validating the timestamp itself is valid
                elif not isinstance(ts, valid := (str, int, datetime)):
                    raise TypeError(
                        f'InfluxDB3 "time" field expects: {valid}, got {type(ts)}'
                    )

                if isinstance(ts, int):
                    time_len = len(str(ts))
                    expected = TIME_PRECISION_LEN[self._write_precision]
                    if time_len != expected:
                        raise ValueError(
                            f'`time_precision` of "{self._write_precision}" '
                            f"expects a {expected}-digit integer epoch, "
                            f"got {time_len} (timestamp: {ts})."
                        )

                if self._include_metadata_tags:
                    tags["__key"] = item.key
                    tags["__topic"] = batch.topic
                    tags["__partition"] = batch.partition

                if _fields_keys:
                    fields = {
                        f: value[f]
                        for f in _fields_keys
                        if f in value or not self._allow_missing_fields
                    }
                else:
                    fields = value

                if self._convert_ints_to_floats:
                    fields = {
                        k: float(v) if type(v) is int else v  # avoids bool matching
                        for k, v in fields.items()
                    }

                record = {
                    "measurement": _measurement,
                    "tags": tags,
                    "fields": fields,
                    "time": ts,
                }
                records.append(record)
                min_timestamp = min(ts, min_timestamp or _ts_min_default(ts))
                max_timestamp = max(ts, max_timestamp or _ts_max_default(ts))

            try:
                _start = time.monotonic()
                self._client.write(  # type: ignore[union-attr]
                    record=records, write_precision=self._write_precision
                )
                elapsed = round(time.monotonic() - _start, 2)
                logger.info(
                    f"Sent data to InfluxDB; "
                    f"total_records={len(records)} "
                    f"min_timestamp={min_timestamp} "
                    f"max_timestamp={max_timestamp} "
                    f"time_elapsed={elapsed}s"
                )
            except influxdb_client_3.InfluxDBError as exc:
                if exc.response and exc.response.status == 429 and exc.retry_after:
                    # The write limit is exceeded, raise a SinkBackpressureError
                    # to pause the partition for a certain period of time.
                    raise SinkBackpressureError(
                        retry_after=int(exc.retry_after)
                    ) from exc
                raise


def _ts_min_default(timestamp: Union[int, str, datetime]):
    if isinstance(timestamp, int):
        return sys.maxsize
    elif isinstance(timestamp, str):
        return "~"  # lexicographically largest ASCII char
    elif isinstance(timestamp, datetime):
        return datetime.max.replace(tzinfo=timezone.utc)


def _ts_max_default(timestamp: Union[int, str, datetime]):
    if isinstance(timestamp, int):
        return -1
    elif isinstance(timestamp, str):
        return ""
    elif isinstance(timestamp, datetime):
        return datetime.min.replace(tzinfo=timezone.utc)


def _measurement_callable(setter: MeasurementSetter) -> MeasurementCallable:
    if callable(setter):
        return setter
    return lambda value: setter


def _fields_callable(setter: FieldsSetter) -> FieldsCallable:
    if callable(setter):
        return setter
    return lambda value: setter


def _tags_callable(setter: TagsSetter) -> TagsCallable:
    if callable(setter):
        return setter
    return lambda value: setter


def _time_callable(setter: Optional[TimeSetter]) -> TimeCallable:
    if callable(setter):
        return setter
    if isinstance(setter, str):
        return lambda value: value[setter]  # type:  ignore
    return lambda value: None  # the kafka timestamp will be used
