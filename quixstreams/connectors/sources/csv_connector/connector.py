import csv
import logging
import json
from typing import Optional, Union, Callable, Iterator

from quixstreams.connectors.sources.templates import (
    SourceProducer,
    SourceConnector,
)
from quixstreams.models.serializers import SerializerType
from quixstreams.models.types import MessageKey, MessageValue


logger = logging.getLogger(__name__)

__all__ = ("CsvSourceConnector",)


class CsvSourceConnector(SourceConnector):
    """
    A thin wrapper around csv.DictReader for lazy row-based reading of a CSV file
    with a header.
    """

    def __init__(
        self,
        csv_path: str,
        producer_topic_name: str,
        producer_broker_address: Optional[str] = None,
        producer_key_serializer: SerializerType = "string",
        producer_value_serializer: SerializerType = "json",
        key_extractor: Optional[Union[str, Callable[[dict], MessageKey]]] = None,
        value_extractor: Callable[[dict], MessageValue] = json.dumps,
        **csv_reader_kwargs,
    ):
        """
        :param csv_path: file path to a .csv file
        :param producer_topic_name: name of topic
        :param producer_broker_address: broker address (if Quix environment not defined)
        :param producer_key_serializer: kafka key serializer; Default "string"
        :param producer_value_serializer: kafka value serializer; Default "json"
        :param key_extractor: how a key is generated for a given data row. Options:
            - None; no key will be generated (default)
            - A string, representing a column name; uses that row's column value.
            - A function that handles the row as a dict.
        :param value_extractor: A function for generating resulting message value for the
            row. Default: json.dumps
        :param csv_reader_kwargs: kwargs passthrough to csv.DictReader
        """
        self._path = csv_path
        self._key_extract = key_extractor
        self._value_extract = value_extractor
        self._producer_actual = SourceProducer(
            topic_name=producer_topic_name,
            broker_address=producer_broker_address,
            key_serializer=producer_key_serializer,
            value_serializer=producer_value_serializer,
        )

        self._file = None
        self._reader = None
        self._reader_kwargs = csv_reader_kwargs

    @property
    def _producer(self) -> SourceProducer:
        return self._producer_actual

    def _lazy_reader(self) -> Iterator[dict]:
        """Lazy row reading of the CSV to avoid loading it all in memory"""
        for row in self._reader:
            yield row

    def _read_row(self) -> Optional[dict]:
        try:
            return next(self._lazy_reader())
        except StopIteration:
            logger.info("CSV Consumer has reached the end of the file")
            return

    def _start(self):
        if not self._file:
            self._file = open(self._path, "r")
            self._reader = csv.DictReader(self._file, **self._reader_kwargs)
            logger.info(f"CSV Consumer beginning reading file: {self._path}")

    def _stop(self):
        if self._file:
            logger.info(f"CSV Consumer has stopped reading file: {self._path}")
            self._file.close()
            self._file = None
        self._reader = None

    def _key_extractor(self, data: dict) -> Optional[MessageKey]:
        """
        :param data: CSV row data as a dict

        :return: a MessageKey type or None
        """
        if self._key_extract:
            if isinstance(self._key_extract, str):
                return data[self._key_extract]
            return self._key_extract(data)

    def _value_extractor(self, data: dict) -> Optional[MessageValue]:
        """
        :param data: CSV row data as a dict

        :return: a MessageValue type or None
        """
        return self._value_extract(data)

    def run(self):
        """
        Run the connector until CSV is fully read.

        Should be executed within a "with" context block.
        """
        with self._producer as producer:
            while row := self._read_row():
                producer.produce(
                    key=self._key_extractor(row), value=self._value_extractor(row)
                )
