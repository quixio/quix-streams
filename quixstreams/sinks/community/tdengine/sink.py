import base64
import json
import logging
import ssl
import sys
import time
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Literal, Mapping, Optional, Union, get_args
from urllib.parse import urlencode, urljoin

import urllib3

from quixstreams.models import HeadersTuples
from quixstreams.sinks.base import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBackpressureError,
    SinkBatch,
)

from .point import Point

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
SupertableCallable = Callable[[InfluxDBValueMap], str]
TagsCallable = Callable[[InfluxDBValueMap], Iterable[str]]
SubtableNameCallable = Callable[[InfluxDBValueMap], str]
TimeCallable = Callable[[InfluxDBValueMap], Optional[Union[str, int, datetime]]]

FieldsSetter = Union[Iterable[str], FieldsCallable]
SupertableSetter = Union[str, SupertableCallable]
TagsSetter = Union[Iterable[str], TagsCallable]
SubtableNameSetter = Union[str, SubtableNameCallable]
TimeSetter = Union[str, TimeCallable]


class TDengineSink(BatchingSink):
    def __init__(
        self,
        host: str,
        database: str,
        supertable: SupertableSetter,
        subtable: SubtableNameSetter,
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
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        verify_ssl: bool = True,
        username: str = "",
        password: str = "",
        token: str = "",
        max_retries: int = 5,
        retry_backoff_factor: float = 1.0,
    ):
        """
        A connector to sink processed data to TDengine.

        It batches the processed records in memory per topic partition, converts
        them to the InfluxDB line protocol, and flushes them to TDengine at the checkpoint.

        >***NOTE***: TDengineSink can accept only dictionaries.
        > If the record values are not dicts, you need to convert them to dicts before
        > sinking.

        :param token: TDengine cloud token
        :param host: TDengine host in format "http[s]://<host>[:<port>]".
        :param username: TDengine username
        :param password: TDengine password
        :param verify_ssl: if `True`, verifies the SSL certificate.
            Default - `True`.
        :param database: database name
        :param supertable: supertable name as a string.
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns a string.
        :param subtable: subtable name as a string.
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns a string.
            If the subtable name is empty string, a hash value will be generated from the data as the subtable name.
        :param fields_keys: an iterable (list) of strings used as InfluxDB line protocol "fields".
            Also accepts a single argument callable that receives the current message
            data as a dict and returns an iterable of strings.
            - If present, it must not overlap with "tags_keys".
            - If empty, the whole record value will be used.
            >***NOTE*** The fields' values can only be strings, floats, integers, or booleans.
            Default - `()`.
        :param tags_keys: an iterable (list) of strings used as InfluxDB line protocol "tags".
            Also accepts a single-argument callable that receives the current message
            data as a dict and returns an iterable of strings.
            - If present, it must not overlap with "fields_keys".
            - Given keys are popped from the value dictionary since the same key
            cannot be both a tag and field.
            - If empty, no tags will be sent.
            >***NOTE***: always converts tag values to strings.
            Default - `()`.
        :param time_setter: an optional column name to use as "time" when convert to InfluxDB line protocol.
            Also accepts a callable which receives the current message data and
            returns either the desired time or `None` (use default).
            The time can be an `int`, `string` (RFC3339 format), or `datetime`.
            The time must match the `time_precision` argument if not a `datetime` object, else raises.
            By default, a record's kafka timestamp with "ms" time precision is used.
        :param time_precision: a time precision to use when convert to InfluxDB line protocol.
            Possible values: "ms", "ns", "us", "s".
            Default - `"ms"`.
        :param allow_missing_fields: if `True`, skip the missing fields keys, else raise `KeyError`.
            Default - `False`
        :param include_metadata_tags: if True, includes record's key, topic,
            and partition as tags.
            Default - `False`.
        :param convert_ints_to_floats: if True, converts all integer values to floats.
            Default - `False`.
        :param batch_size: how many records to write to TDengine in one request.
            Note that it only affects the size of one write request, and not the number
            of records flushed on each checkpoint.
            Default - `1000`.
        :param enable_gzip: if True, enables gzip compression for writes.
            Default - `True`.
        :param request_timeout_ms: an HTTP request timeout in milliseconds.
            Default - `10000`.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        :param max_retries: maximum number of retries for failed requests.
            Default - `5`.
        :param retry_backoff_factor: a backoff factor applied between retry attempts starting from the second retry.
            The sleep duration between retries is calculated as `{backoff factor} * (2 ** ({number of previous retries}))` seconds.
            Default - `1.0`.
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
        url_path = "influxdb/v1/write"
        base_url = urljoin(host, url_path)
        sql_url = urljoin(host, "rest/sql")
        precision = time_precision
        if precision == "us":
            precision = "u"
        query_params = {
            "db": database,
            "precision": precision,
            "table_name_key": "__subtable",
        }
        header = {
            "Content-Type": "text/plain; charset=utf-8",
        }
        if enable_gzip:
            header["Accept-Encoding"] = "gzip"
        if token != "":
            query_params["token"] = token
            sql_url = urljoin(sql_url, f"?token={token}")
        elif username != "" and password != "":
            basic_auth = f"{username}:{password}"
            header["authorization"] = (
                f"Basic {base64.b64encode(basic_auth.encode('latin-1')).decode()}"
            )
        else:
            raise ValueError("Either token or username and password must be provided")

        query_string = urlencode(query_params)
        full_url = f"{base_url}?{query_string}"
        self._client_args = {
            "url": full_url,
            "sql_url": sql_url,
            "header": header,
            "timeout": request_timeout_ms,
            "verify_ssl": verify_ssl,
            "database": database,
            "max_retries": max_retries,
            "retry_backoff_factor": retry_backoff_factor,
        }
        self._client: Optional[urllib3.PoolManager] = None
        self._supertable_name = _supertable_callable(supertable)
        self._subtable_name = _subtable_name_callable(subtable)
        self._fields_keys = _fields_callable(fields_keys)
        self._tags_keys = _tags_callable(tags_keys)
        self._time_setter = _time_callable(time_setter)
        self._include_metadata_tags = include_metadata_tags
        self._write_precision = time_precision
        self._batch_size = batch_size
        self._allow_missing_fields = allow_missing_fields
        self._convert_ints_to_floats = convert_ints_to_floats

    def setup(self):
        if self._client_args["verify_ssl"]:
            cert_reqs = ssl.CERT_REQUIRED
        else:
            cert_reqs = ssl.CERT_NONE
        retry_strategy = urllib3.Retry(
            total=self._client_args["max_retries"],
            backoff_factor=self._client_args["retry_backoff_factor"],
            respect_retry_after_header=False,
            allowed_methods=["POST"],
        )
        self._client = urllib3.PoolManager(
            cert_reqs=cert_reqs,
            timeout=urllib3.Timeout(total=self._client_args["timeout"] / 1_000),
            retries=retry_strategy,
        )
        # check if the database is alive
        database = self._client_args["database"]
        check_db_sql = "SHOW DATABASES"
        logger.debug(f"Sending data to {self._client_args['sql_url']} : {check_db_sql}")
        resp = self._client.request(
            "POST",
            self._client_args["sql_url"],
            body=check_db_sql,
            headers=self._client_args["header"],
        )
        if resp.status != 200:
            raise urllib3.exceptions.HTTPError(
                f"Failed to get databases: {resp.status} {resp.data}"
            )
        resp_data = json.loads(resp.data.decode("utf-8"))
        resp_code = resp_data.get("code")
        if resp_code != 0:
            error_message = resp_data.get("desc", "Unknown error")
            raise urllib3.exceptions.HTTPError(
                f"Failed to get databases, [{resp_code}]:{error_message}"
            )
        data = resp_data.get("data")
        # TODO: create the database if it does not exist
        if not (
            isinstance(data, list)
            and any(database == sublist[0] for sublist in data if sublist)
        ):
            raise urllib3.exceptions.HTTPError(f"Database '{database}' does not exist")

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
        supertable = self._supertable_name
        subtable = self._subtable_name
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
                _measurement = supertable(value)
                # check if _measurement is empty
                if _measurement is None or not _measurement.strip():
                    raise ValueError(
                        f'Supertable name cannot be empty for record with key "{item.key}" '
                        f"and topic '{batch.topic}'"
                    )
                _tags_keys = tags_keys(value)
                _fields_keys = fields_keys(value)
                _subtable_name = subtable(item.value)
                ts = time_setter(value)

                tags = {}
                for tag_key in _tags_keys:
                    if tag_key in value:
                        tag = value.pop(tag_key)
                        tags[tag_key] = tag

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
                        k: float(v) if isinstance(v, int) else v
                        for k, v in fields.items()
                    }
                # check if fields is empty
                if not fields:
                    raise ValueError(
                        f'No fields found in the record for supertable "{_measurement}" '
                        f"and subtable name '{_subtable_name}'"
                    )

                tags["__subtable"] = _subtable_name

                # check if fields and tags overlap
                fields_tags_overlap = set(fields) & set(tags)
                if fields_tags_overlap:
                    overlap_str = ",".join(str(k) for k in fields_tags_overlap)
                    raise ValueError(
                        f'Keys {overlap_str} are present in both "fields" and "tags" '
                        f"for supertable '{_measurement}' and subtable '{_subtable_name}'"
                    )

                if ts is None:
                    ts = item.timestamp

                elif not isinstance(ts, valid := (str, int, datetime)):
                    raise TypeError(
                        f'TDengine "time" field expects: {valid}, got {type(ts)}'
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

                record = {
                    "measurement": _measurement,
                    "tags": tags,
                    "fields": fields,
                    "time": ts,
                }
                records.append(record)
                min_timestamp = min(ts, min_timestamp or _ts_min_default(ts))
                max_timestamp = max(ts, max_timestamp or _ts_max_default(ts))
            if not records:
                logger.warning(
                    f"No records to write for batch with key '{item.key}' "
                    f"and topic '{batch.topic}'"
                )
                continue
            _start = time.monotonic()
            l: list[bytes] = [b""] * len(records)
            for i, point in enumerate(records):
                p = Point.from_dict(point, self._write_precision)
                l[i] = p.to_line_protocol().encode("utf-8")
            body = b"\n".join([item for item in l if item])
            if body == b"":
                logger.warning(
                    f"No valid records to write for batch with key '{item.key}' "
                    f"and topic '{batch.topic}'"
                )
                continue
            logger.debug(f"Sending data to {self._client_args['url']} : {body}")
            resp = self._client.request(
                "POST",
                self._client_args["url"],
                body=body,
                headers=self._client_args["header"],
            )
            elapsed = round(time.monotonic() - _start, 2)
            logger.info(
                f"Sent data to TDengine; "
                f"total_records={len(records)} "
                f"min_timestamp={min_timestamp} "
                f"max_timestamp={max_timestamp} "
                f"time_elapsed={elapsed}s"
            )
            err = urllib3.exceptions.HTTPError(
                f"Failed to write data to TDengine: {resp.status} {resp.data}"
            )
            if resp.status == 503:
                retry_after = resp.getheader("Retry-After")
                raise SinkBackpressureError(retry_after=int(retry_after)) from err
            elif resp.status != 204:
                raise err


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


def _subtable_name_callable(setter: SubtableNameSetter) -> SubtableNameCallable:
    if callable(setter):
        return setter
    return lambda value: setter


def _supertable_callable(setter: SupertableSetter) -> SupertableCallable:
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
        # If setter not in value, it will raise KeyError
        return lambda value: value[setter]
    return lambda value: None  # the kafka timestamp will be used
