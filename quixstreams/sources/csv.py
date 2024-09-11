import csv
import json

from typing import Optional, Callable, Any

from quixstreams.models.topics import Topic

from .base import Source


class CSVSource(Source):
    def __init__(
        self,
        path: str,
        dialect: str = "excel",
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
        key_deserializer: Callable[[Any], str] = str,
        value_deserializer: Callable[[Any], str] = json.loads,
    ) -> None:
        """
        A base CSV source that reads data from a single CSV file.
        Best used with `quixstreams.sinks.csv.CSVSink`.

        Required columns: key, value
        Optional columns: timestamp

        :param path: path to the CSV file
        :param dialect: a CSV dialect to use. It affects quoting and delimiters.
            See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
            Default - `"excel"`.
        :param key_deseralizer: a callable to convert strings to key.
            Default - `str`
        :param value_deserializer: a callable to convert strings to value.
            Default - `json.loads`
        """
        super().__init__(name or path, shutdown_timeout)
        self.path = path
        self.dialect = dialect

        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer

    def run(self):
        key_deserializer = self._key_deserializer
        value_deserializer = self._value_deserializer

        with open(self.path, "r") as f:
            reader = csv.DictReader(f, dialect=self.dialect)

            while self.running:
                try:
                    item = next(reader)
                except StopIteration:
                    return

                # if a timestamp column exist with no value timestamp is ""
                timestamp = item.get("timestamp") or None
                if timestamp is not None:
                    timestamp = int(timestamp)

                msg = self.serialize(
                    key=key_deserializer(item["key"]),
                    value=value_deserializer(item["value"]),
                    timestamp_ms=timestamp,
                )

                self.produce(
                    key=msg.key,
                    value=msg.value,
                    timestamp=msg.timestamp,
                    headers=msg.headers,
                )

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_serializer="string",
            key_deserializer="string",
            value_deserializer="json",
            value_serializer="json",
        )
