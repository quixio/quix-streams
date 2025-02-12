import csv
import json
import os
from typing import Any, Callable

from ..base import BatchingSink, SinkBatch


class CSVSink(BatchingSink):
    def __init__(
        self,
        path: str,
        dialect: str = "excel",
        key_serializer: Callable[[Any], str] = str,
        value_serializer: Callable[[Any], str] = json.dumps,
    ):
        """
        A base CSV sink that writes data from all assigned partitions to a single file.
        It's best to be used for local debugging.

        Column format:
            (key, value, timestamp, topic, partition, offset)

        :param path: a path to CSV file
        :param dialect: a CSV dialect to use. It affects quoting and delimiters.
            See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
            Default - `"excel"`.
        :param key_serializer: a callable to convert keys to strings.
            Default - `str`.
        :param value_serializer: a callable to convert values to strings.
            Default - `json.dumps`.
        """
        super().__init__()
        self.path = path
        self.dialect = dialect
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

    def setup(self):
        return

    def write(self, batch: SinkBatch):
        is_new = not os.path.exists(self.path)
        fieldnames = (
            "key",
            "value",
            "timestamp",
            "topic",
            "partition",
            "offset",
        )
        with open(self.path, "a") as f:
            writer = csv.DictWriter(f, dialect=self.dialect, fieldnames=fieldnames)
            if is_new:
                writer.writeheader()

            _key_serializer = self._key_serializer
            _value_serializer = self._value_serializer

            for item in batch:
                writer.writerow(
                    {
                        "key": _key_serializer(item.key),
                        "value": _value_serializer(item.value),
                        "timestamp": item.timestamp,
                        "topic": batch.topic,
                        "partition": batch.partition,
                        "offset": item.offset,
                    }
                )
