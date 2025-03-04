import logging
import time

from quixstreams.models.topics import Topic
from quixstreams.sources.base import Source

try:
    import pandas as pd
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[pandas]" to use PandasDataFrameSource'
    ) from exc

__all__ = ("PandasDataFrameSource",)

logger = logging.getLogger(__name__)


class PandasDataFrameSource(Source):
    def __init__(
        self,
        df: pd.DataFrame,
        key_column: str,
        timestamp_column: str = None,
        delay: float = 0,
        shutdown_timeout: float = 10,
        keep_meta_as_values: bool = True,
        name: str = "pandas-dataframe-source",
    ) -> None:
        """
        A source that reads data from a pandas.DataFrame and produces rows to a Kafka topic in JSON format.

        :param df: the pandas.DataFrame object to read data from.
        :param key_column: a column name that contains the messages keys.
            The values in dataframe[key_column] must be either strings or `None`.
        :param timestamp_column: an optional argument to specify a dataframe column that contains the messages timestamps.
            The values in dataframe[timestamp_column] must be time in milliseconds as `int`.
            If empty, the current epoch will be used.
            Default - `None`
        :param name: a unique name for the Source, used as a part of the default topic name.
            Default - `"pandas-dataframe-source"`.
        :param delay: an optional delay after producing each row for stream simulation.
            Default - `0`.
        :param shutdown_timeout: Time in seconds the application waits for the source to gracefully shut down.
        :param keep_meta_as_values: Whether to keep metadata (timestamp_column and key_column) as-values data too.
            If True, timestamp and key columns are passed both as metadata and values in the message.
            If False, timestamp and key columns are passed only as the message's metadata.
            Default - `True`.
        """
        self._df = df
        self._delay = delay
        self._keep_meta_as_values = keep_meta_as_values
        self._key_col = self._get_key_column(key_column)
        self._timestamp_col = (
            self._get_timestamp_column(col_name=timestamp_column)
            if timestamp_column
            else None
        )

        super().__init__(name=name, shutdown_timeout=shutdown_timeout)

    def setup(self):
        # nothing client related to set up
        return

    def _get_key_column(self, col_name: str) -> str:
        """
        Validates the key column.

        :param col_name: The column to be used as the message key.
        :return: The column name if valid; otherwise, `None`.
        """
        key_column = self._df.get(col_name)
        # Verify that the key column exists
        if key_column is None:
            raise ValueError(f'Key column "{col_name}" does not exist in the DataFrame')

        # Verify that the key column is of string type or all its values are nulls
        if not (key_column.isnull().all() or pd.api.types.is_string_dtype(key_column)):
            raise ValueError(f'Key column "{col_name}" must be of a "string" dtype')
        return col_name

    def _get_timestamp_column(self, col_name: str) -> str:
        """
        Validates the timestamp column.

        :param col_name: The column to be used as the message timestamp.
        :return: The column name if valid; otherwise, `None`.
        """

        timestamp_column = self._df.get(col_name)
        # Verify that the timestamp column exists if it's passed
        if timestamp_column is None:
            raise ValueError(
                f'Timestamp column "{col_name}" does not exist in the DataFrame'
            )

        # Verify that the timestamp column is of integer type
        if not pd.api.types.is_integer_dtype(self._df[col_name]):
            raise ValueError(
                f'Timestamp column "{col_name}" must be of "integer" dtype'
            )

        return col_name

    def run(self):
        """
        Produces data from the DataFrame row by row.
        """
        logger.info(f"Producing data from DataFrame ({len(self._df)} rows).")

        # Iterate row by row over the dataframe
        for i, row in self._df.iterrows():
            if self.running:
                row_dict = row.to_dict()

                # Extract message key
                message_key = row_dict[self._key_col]

                # Extract timestamp
                message_timestamp = (
                    row_dict[self._timestamp_col] if self._timestamp_col else None
                )

                # Remove metadata from values if necessary
                if not self._keep_meta_as_values:
                    row_dict.pop(self._key_col, None)
                    row_dict.pop(self._timestamp_col, None)

                # Serialize data before sending to Kafka
                msg = self.serialize(
                    key=message_key, value=row_dict, timestamp_ms=message_timestamp
                )

                # Publish the data to the topic
                self.produce(timestamp=msg.timestamp, key=msg.key, value=msg.value)

                # If the delay is specified, sleep before producing the next row
                if self._delay > 0:
                    time.sleep(self._delay)

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_serializer="string",
            key_deserializer="string",
            value_deserializer="json",
            value_serializer="json",
        )
