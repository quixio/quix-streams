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
        A source that reads data from a pandas.DataFrame and produces rows to a Kafka topic in JSON format.

        :df: the pandas.DataFrame object to read data from.
        :param key_column: an optional argument to specify dataframe column that contains the messages keys.
            The values in dataframe[key_column] must be `str`.
            If empty, the Kafka messages will be produced without keys.
            Default - `None`.
        :param timestamp_column: an optional argument to specify dataframe column that contains the messages timestamps.
            he values in dataframe[timestamp_column] must be time in milliseconds as `int`.
            If empty, the current epoch will be used.
            Default - `None`
        :param name: a unique name for the Source, used as a part of the default topic name.
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

    def get_key_column(self, key_column:str) -> Optional[str]:
        """
        Validates the key column.

        :param key_column: The column to use as the message key.
        :return: The column name if valid; otherwise, None.
        """
        if key_column not in self.df.columns:
            logger.warning(f'Key column "{key_column}" does not exist in the DataFrame.')
            return None

        if not pd.api.types.is_string_dtype(self.df[key_column]):
            logger.warning(f'Key column "{key_column}" is not of string dtype. It will not be used as a key.')
            return None

        return key_column


    def get_timestamp_column(self, timestamp_column):
        """
        Validates the timestamp column.

        :param timestamp_column: The column to use as the message timestamp.
        :return: The column name if valid; otherwise, None.
        """
        if timestamp_column not in self.df.columns:
            logger.warning(f'Timestamp column "{timestamp_column}" does not exist in the DataFrame.')
            return None

        if not pd.api.types.is_integer_dtype(self.df[timestamp_column]):
            logger.warning(f'Timestamp column "{timestamp_column}" is not of integer dtype. It will not be used as a timestamp.')
            return None

        is_milliseconds = (self.df[timestamp_column].min() >= 10 ** 12) and (self.df[timestamp_column].max() < 10 ** 13)
        if not is_milliseconds:
            logger.warning(f'Timestamp column "{timestamp_column}" values are not in milliseconds. It will not be used as a timestamp.')
            return None

        return timestamp_column

    def run(self):
        """
        Produces data from the DataFrame row by row.
        """
        logger.info(f"Producing data from DataFrame ({len(self.df)} rows).")

        # Iterate row by row over the dataframe
        for i, row in self.df.iterrows():
            if self.running:
                row_dict = row.to_dict()

                # Extract message key
                message_key = row_dict[self.key_col] if self.key_col else None

                # Extract timestamp
                message_timestamp = row_dict[self.timestamp_col] if self.timestamp_col else None

                # Remove metadata from values if necessary
                if not self.keep_meta_as_values:
                    row_dict.pop(self.key_col, None)
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
