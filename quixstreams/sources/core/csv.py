import csv
import logging
import time
from pathlib import Path
from typing import Callable, Optional, Union

from quixstreams.models.topics import Topic
from quixstreams.sources.base import Source

logger = logging.getLogger(__name__)


class CSVSource(Source):
    def __init__(
        self,
        path: Union[str, Path],
        name: str,
        key_extractor: Optional[Callable[[dict], Union[str, bytes]]] = None,
        timestamp_extractor: Optional[Callable[[dict], int]] = None,
        delay: float = 0,
        shutdown_timeout: float = 10,
        dialect: str = "excel",
    ) -> None:
        """
        A base CSV source that reads data from a CSV file and produces rows
        to the Kafka topic in JSON format.

        :param path: a path to the CSV file.
        :param name: a unique name for the Source.
            It is used as a part of the default topic name.
        :param key_extractor: an optional callable to extract the message key from the row.
            It must return either `str` or `bytes`.
            If empty, the Kafka messages will be produced without keys.
            Default - `None`.
        :param timestamp_extractor: an optional callable to extract the message timestamp from the row.
            It must return time in milliseconds as `int`.
            If empty, the current epoch will be used.
            Default - `None`
        :param delay: an optional delay after producing each row for stream simulation.
            Default - `0`.
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shut down.
        :param dialect: a CSV dialect to use. It affects quoting and delimiters.
            See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
            Default - `"excel"`.
        """
        self.path = path
        self.delay = delay
        self.dialect = dialect

        self.key_extractor = key_extractor
        self.timestamp_extractor = timestamp_extractor

        super().__init__(name=name, shutdown_timeout=shutdown_timeout)

    def setup(self):
        return

    def run(self):
        # Start reading the file
        with open(self.path, "r") as f:
            logger.info(f'Producing data from the file "{self.path}"')
            reader = csv.DictReader(f, dialect=self.dialect)

            while self.running:
                try:
                    row = next(reader)
                except StopIteration:
                    return

                # Extract message key from the row
                message_key = self.key_extractor(row) if self.key_extractor else None
                # Extract timestamp from the row
                timestamp = (
                    self.timestamp_extractor(row) if self.timestamp_extractor else None
                )
                # Serialize data before sending to Kafka
                msg = self.serialize(key=message_key, value=row, timestamp_ms=timestamp)

                # Publish the data to the topic
                self.produce(timestamp=msg.timestamp, key=msg.key, value=msg.value)

                # If the delay is specified, sleep before producing the next row
                if self.delay > 0:
                    time.sleep(self.delay)

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_serializer="string",
            key_deserializer="string",
            value_deserializer="json",
            value_serializer="json",
        )
