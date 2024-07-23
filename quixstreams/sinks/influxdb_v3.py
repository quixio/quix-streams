from itertools import islice
from typing import Optional, Iterable, TypeVar

import influxdb_client_3
from influxdb_client_3 import InfluxDBClient3, WritePrecision

from .base import Sink, SinkBatch
from .exceptions import SinkBackpressureError

T = TypeVar("T")


class InfluxDBV3Sink(Sink):
    def __init__(
        self,
        token: str,
        host: str,
        org: str,
        database: str,
        measurement: str,
        fields_keys: Iterable[str] = (),
        tags_keys: Iterable[str] = (),
        time_key: Optional[str] = None,
        batch_size: int = 1000,
        debug: bool = False,
    ):
        super().__init__()
        self._client = InfluxDBClient3(
            token=token,
            host=host,
            org=org,
            database=database,
            debug=debug,
        )
        self._measurement = measurement
        self._fields_keys = fields_keys
        self._tags_keys = tags_keys or []
        self._time_key = time_key
        self._write_precision = WritePrecision.MS
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
                tags["__key"] = item.key
                tags["__offset"] = item.offset
                tags["__partition"] = batch.partition
                tags["__topic"] = batch.topic
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
                # TODO: Maybe use backoff and retries here
            except influxdb_client_3.InfluxDBError as exc:
                if exc.retry_after:
                    # The write limit is exceeded, raise a SinkBackpressureError
                    # to pause the partition for a certain period of time.
                    raise SinkBackpressureError(
                        retry_after=int(exc.retry_after),
                        topic=batch.topic,
                        partition=batch.partition,
                    )
                raise
