import pandas as pd
import logging
import time
from pathlib import Path
from typing import Callable, Optional, Union

from quixstreams.models.topics import Topic
from quixstreams.sources.base import Source

logger = logging.getLogger(__name__)


class PandasDataFrameSource(Source):
    def __init__(
        self,
        df: pd.DataFrame,
        key_column: str = None,
        timestamp_column: str = None,
        name: str = "PandasDataFrame",
        delay: float = 0,
        shutdown_timeout: float = 10,
        keep_meta_as_values: bool = True,
    ) -> None:
        """
        A base pandas.DataFrame source that reads data from a pandas.DataFrame object and produces rows
        to the Kafka topic in JSON format.

        :df: the pandas.DataFrame object to read data from.
        :param key_column: an optional argument to specify dataframe column that contains the messages keys.
            The values in dataframe[key_column] must be `str`.
            If empty, the Kafka messages will be produced without keys.
            Default - `None`.
        :param timestamp_column: an optional argument to specify dataframe column that contains the messages timestamps.
            he values in dataframe[timestamp_column] must be time in milliseconds as `int`.
            If empty, the current epoch will be used.
            Default - `None`
        :param name: a unique name for the Source.
            It is used as a part of the default topic name.
        :param delay: an optional delay after producing each row for stream simulation.
            Default - `0`.
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shut down.
        :param keep_meta_as_values: Whether to keep metadata (timestamp_column and key_column) as-values data too.
            If True, timestamp and key columns are passed both as metadata and values in the message.
            If False, timestamp and key columns are passed only as the message's metadata.
            Default - `True`.
        """
        self.df = df
        self.delay = delay
        self.keep_meta_as_values = keep_meta_as_values
        self.key_col = self.get_key_column(key_column)
        self.timestamp_col = self.get_timestamp_column(timestamp_column)

        super().__init__(name=name, shutdown_timeout=shutdown_timeout)

    def get_key_column(self, key_column:str):
        # Check that column exists
        if key_column not in self.df.columns:
            logger.warning(f'Key column provided "{key_column}" does not exist in df dataframe.')
            return None
        # Check if df column dtype is str
        is_str_dtype = pd.api.types.is_string_dtype(self.df[key_column])
        if not is_str_dtype:
            logger.warning(f'Key column provided "{key_column}" dtype is not a str. It will not be used as Key.')
            return None
        return key_column

    def get_timestamp_column(self, timestamp_column):
        # Check that column exists
        if timestamp_column not in self.df.columns:
            logger.warning(f'Timestamp column provided "{timestamp_column}" does not exist in df dataframe.')
            return None
        # Check if df column dtype is integer
        is_integer_dtype = pd.api.types.is_integer_dtype(self.df[timestamp_column])
        if not is_integer_dtype:
            logger.warning(f'Timestamp column provided "{timestamp_column}" dtype is not an integer. It will not be used as timestamp.')
            return None
        # Check if column integers are milliseconds
        is_milliseconds = (self.df[timestamp_column].min() >= 10**12) and (self.df[timestamp_column].max() < 10 ** 13)
        if not is_milliseconds:
            logger.warning(f'Timestamp column provided "{timestamp_column}" is not in milliseconds. It will not be used as timestamp.')
            return None
        return timestamp_column

    def run(self):
        logger.info(f"Producing the data from the pandas.DataFrame ({len(self.df)} rows).")

        # Iterate row by row over the dataframe
        for i, row in self.df.iterrows():

            if self.running:
                row_dict = row.to_dict()

                # Extract message key from the row
                message_key = row_dict[self.key_col] if self.key_col else None

                # Extract timestamp from the row
                message_timestamp = row_dict[self.timestamp_col] if self.timestamp_col else None

                # Message values
                if not self.keep_meta_as_values:
                    if self.key_col:
                        row_dict.pop(self.key_col, None)
                    if self.timestamp_col:
                        row_dict.pop(self.timestamp_col, None)

                # Serialize data before sending to Kafka
                msg = self.serialize(key=message_key, value=row_dict, timestamp_ms=message_timestamp)

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
