import logging
import sys
import time
from typing import Optional, Iterable, Any, List, Tuple, Mapping

from quixstreams.models import HeaderValue

try:
    import influxdb_client_3
    from influxdb_client_3 import InfluxDBClient3, WritePrecision, WriteOptions
    from influxdb_client_3.write_client.client.write_api import WriteType
except ImportError as exc:
    raise ImportError(
        'Package "influxdb3-python" is missing: '
        "run pip install quixstreams[influxdb3] to fix it"
    ) from exc

from .base import BatchingSink, SinkBatch
from .exceptions import SinkBackpressureError

logger = logging.getLogger(__name__)


class InfluxDB3Sink(BatchingSink):
    def __init__(
        self,
        token: str,
        host: str,
        organization_id: str,
        database: str,
        measurement: str,
        fields_keys: Iterable[str] = (),
        tags_keys: Iterable[str] = (),
        time_key: Optional[str] = None,
        time_precision: WritePrecision = WritePrecision.MS,
        include_metadata_tags: bool = False,
        batch_size: int = 1000,
        enable_gzip: bool = True,
        request_timeout_ms: int = 10_000,
        debug: bool = False,
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
        :measurement: measurement name
        :param fields_keys: a list of keys to be used as "fields" when writing to InfluxDB.
            If present, it must not overlap with "tags_keys".
            If empty, the whole record value will be used.
            >***NOTE*** The fields' values can only be strings, floats, integers, or booleans.
            Default - `()`.
        :param tags_keys: a list of keys to be used as "tags" when writing to InfluxDB.
            If present, it must not overlap with "fields_keys".
            These keys will be popped from the value dictionary
            automatically because InfluxDB doesn't allow the same keys be
            both in tags and fields.
            If empty, no tags will be sent.
            >***NOTE***: InfluxDB client always converts tag values to strings.
            Default - `()`.
        :param time_key: a key to be used as "time" when writing to InfluxDB.
            By default, the record timestamp will be used with "ms" time precision.
            When using a custom key, you may need to adjust the `time_precision` setting
            to match.
        :param time_precision: a time precision to use when writing to InfluxDB.
        :param include_metadata_tags: if True, includes record's key, topic,
            and partition as tags.
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
        """

        super().__init__()
        fields_tags_keys_overlap = set(fields_keys) & set(tags_keys)
        if fields_tags_keys_overlap:
            overlap_str = ",".join(str(k) for k in fields_tags_keys_overlap)
            raise ValueError(
                f'Keys {overlap_str} are present in both "fields_keys" and "tags_keys"'
            )

        self._client = InfluxDBClient3(
            token=token,
            host=host,
            org=organization_id,
            database=database,
            debug=debug,
            enable_gzip=enable_gzip,
            timeout=request_timeout_ms,
            write_client_options={
                "write_options": WriteOptions(
                    write_type=WriteType.synchronous,
                )
            },
        )
        self._measurement = measurement
        self._fields_keys = fields_keys
        self._tags_keys = tags_keys
        self._include_metadata_tags = include_metadata_tags
        self._time_key = time_key
        self._write_precision = time_precision
        self._batch_size = batch_size

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: List[Tuple[str, HeaderValue]],
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
        time_key = self._time_key
        for write_batch in batch.iter_chunks(n=self._batch_size):
            records = []

            min_timestamp = sys.maxsize
            max_timestamp = -1

            for item in write_batch:
                value = item.value
                tags = {}
                if tags_keys:
                    for tag_key in tags_keys:
                        # TODO: InfluxDB client always converts tags values to strings
                        #  by doing str().
                        #  We may add some extra validation here in the future to prevent
                        #  unwanted conversion.
                        tag = value.pop(tag_key)
                        tags[tag_key] = tag

                if self._include_metadata_tags:
                    tags["__key"] = item.key
                    tags["__topic"] = batch.topic
                    tags["__partition"] = batch.partition

                fields = (
                    {
                        field_key: value[field_key]
                        for field_key in fields_keys
                        if field_key not in tags_keys
                    }
                    if fields_keys
                    else value
                )
                ts = value[time_key] if time_key is not None else item.timestamp
                record = {
                    "measurement": measurement,
                    "tags": tags,
                    "fields": fields,
                    "time": ts,
                }
                records.append(record)
                min_timestamp = min(ts, min_timestamp)
                max_timestamp = max(ts, max_timestamp)

            try:
                _start = time.monotonic()
                self._client.write(
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
                        retry_after=int(exc.retry_after),
                        topic=batch.topic,
                        partition=batch.partition,
                    ) from exc
                raise
