from itertools import islice
from typing import Optional, Iterable, TypeVar

import influxdb_client_3
from influxdb_client_3 import InfluxDBClient3, WritePrecision, WriteOptions
from influxdb_client_3.write_client.client.write_api import WriteType

from .base import Sink, SinkBatch
from .exceptions import SinkBackpressureError

T = TypeVar("T")


class InfluxDBV3Sink(Sink):
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

        It batches the processed records in memory per topic partition
        and flushes them to InfluxDB at the checkpoint.

        The InfluxDB sink transparently handles backpressure if the DB instance
        cannot accept more data at the moment
        (e.g., when InfluxDB returns an HTTP 429 error with the "retry_after" header set).
        When this happens, the sink will notify the Application to pause consuming
        from the backpressured topic partition until the "retry_after" timeout elapses.

        :param token: InfluxDB access token
        :param host: InfluxDB host in format "https://<host>"
        :param organization_id: InfluxDB organization_id
        :param database: database name
        :measurement: measurement name
        :param fields_keys: a list of keys to be used as "fields" when writing to InfluxDB.
            If empty, the whole record value will be used.
            Default - empty.
        :param tags_keys: a list of keys to be used as "tags" when writing to InfluxDB.
            If empty, no tags will be sent.
            Default - empty.
        :param time_key: a key to be used as "time" when writing to InfluxDB.
            By default, the record timestamp will be used with "ms" time precision.
            When using a custom key, you may need to adjust the `time_precision` setting
            to match.
        :param include_metadata_tags: if True, includes record's key, topic,
            and partition as tags.
            Default - `False`.
        :param batch_size: how many records to write to InfluxDB at once.
            Note that it only affects the size of the writing batch, and not the number
            of records flushed on each checkpoint.
            Default - `1000`.
        :param enable_gzip: if True, enables gzip compression for writes.
            Default - `True`.
        :param request_timeout_ms: an HTTP request timeout in milliseconds.
            Default - `10000`.
        :param debug: if True, print debug logs from InfluxDB client.
            Default - `False`.
        """
        # TODO: Tests
        super().__init__()
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
        self._tags_keys = tags_keys or []
        self._include_metadata_tags = include_metadata_tags
        self._time_key = time_key
        self._write_precision = time_precision
        self._batch_size = batch_size

    def _iter_batches(self, it: Iterable[T], n: int) -> Iterable[Iterable[T]]:
        """
        Batch data into tuples of length n. The last batch may be shorter.
        """
        if n < 1:
            raise ValueError("n must be at least one")
        it_ = iter(it)
        while batch := tuple(islice(it_, n)):
            yield batch

    def write(self, batch: SinkBatch):
        measurement = self._measurement
        fields_keys = self._fields_keys
        tags_keys = self._tags_keys
        time_key = self._time_key
        for write_batch in self._iter_batches(batch, n=self._batch_size):
            records = []
            for item in write_batch:
                value = item.value
                tags = {tag_key: value[tag_key] for tag_key in tags_keys}
                if self._include_metadata_tags:
                    tags["__key"] = item.key
                    tags["__topic"] = batch.topic
                    tags["__partition"] = batch.partition
                fields = (
                    {field_key: value[field_key] for field_key in fields_keys}
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
            try:
                self._client.write(records, write_precision=self._write_precision)
            except influxdb_client_3.InfluxDBError as exc:
                if exc.response.status == 429 and exc.retry_after:
                    # The write limit is exceeded, raise a SinkBackpressureError
                    # to pause the partition for a certain period of time.
                    raise SinkBackpressureError(
                        retry_after=int(exc.retry_after),
                        topic=batch.topic,
                        partition=batch.partition,
                    ) from exc
                raise
