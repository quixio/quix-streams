import logging
from abc import abstractmethod
from pathlib import Path
from time import sleep
from typing import BinaryIO, Callable, Iterable, Optional, Union

from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    Source,
)

from .components import FileFetcher, raw_filestream_deserializer
from .compressions import CompressionName
from .formats import Format, FormatName

__all__ = ("FileSource",)

logger = logging.getLogger(__name__)


class FileSource(Source):
    """
    An interface for interacting with a file-based client.

    Requires defining methods for navigating folders and retrieving/opening raw files
    that will be handed to other methods.

    When these abstract methods are defined, a FileSource will be able to:
    1. Prepare a list of files to download, and retrieve them sequentially
    2. Retrieve file contents asynchronously by downloading the upcoming one in the
        background
    3. Decompress and deserialize the current file to loop through its records
    4. Apply a replay delay for each contained record based on previous record
    5. Serialize and produce respective messages to Kafka based on provided `setters`
    """

    def __init__(
        self,
        directory: Union[str, Path],
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        self._directory = Path(directory)
        super().__init__(
            name=name or self._directory.name,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._key_setter = key_setter or _default_key_setter
        self._value_setter = value_setter or _default_value_setter
        self._timestamp_setter = timestamp_setter or _default_timestamp_setter
        self._replay_speed = max(replay_speed, 0)
        self._raw_filestream_deserializer = raw_filestream_deserializer(
            file_format, compression
        )

        self._previous_timestamp = None
        self._previous_partition = None

    @abstractmethod
    def get_file_list(self, filepath: Path) -> Iterable[Path]:
        """
        Find all files/"blobs" starting from a root folder.

        Each item in the iterable should be a resolvable filepath.

        :param filepath: a starting filepath
        :return: an iterable will all desired files in their desired processing order
        """

    @abstractmethod
    def read_file(self, filepath: Path) -> BinaryIO:
        """
        Returns a filepath as an unaltered, open filestream.

        Result should be ready for deserialization (and/or decompression).
        """

    def process_record(self, record: object):
        """
        Applies replay delay, serializes the record, and produces it to Kafka.
        """
        timestamp = self._timestamp_setter(record)
        self._handle_replay_delay(timestamp)
        serialized = self._producer_topic.serialize(
            key=self._key_setter(record),
            value=self._value_setter(record),
            timestamp_ms=timestamp,
        )
        self.produce(
            key=serialized.key, value=serialized.value, timestamp=serialized.timestamp
        )

    def run(self):
        files = FileFetcher(self.read_file, self.get_file_list(self._directory))
        for filepath, raw_filestream in files:
            logger.info(f"Processing file {filepath}...")
            for record in self._raw_filestream_deserializer(raw_filestream):
                self.process_record(record)
            self._producer.flush()

    def _handle_replay_delay(self, current_timestamp: int):
        """
        Apply the replay speed by calculating the delay between messages
        based on their timestamps.
        """
        if self._previous_timestamp is not None:
            time_diff_seconds = (current_timestamp - self._previous_timestamp) / 1000
            replay_diff_seconds = time_diff_seconds * self._replay_speed
            if replay_diff_seconds > 0.01:  # only sleep when diff is "big enough"
                logger.debug(f"Sleeping for {replay_diff_seconds} seconds...")
                sleep(replay_diff_seconds)
        self._previous_timestamp = current_timestamp


def _default_key_setter(row: dict) -> object:
    return row["_key"]


def _default_value_setter(row: dict) -> object:
    return row["_value"]


def _default_timestamp_setter(row: dict) -> int:
    return row["_timestamp"]
