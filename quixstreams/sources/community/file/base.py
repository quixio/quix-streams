import logging
from abc import abstractmethod
from pathlib import Path
from time import sleep
from typing import BinaryIO, Callable, Iterable, Optional, Union

from quixstreams.models import Topic, TopicConfig
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
    An interface for extracting records using a file-based client.

    It recursively iterates from a provided path (file or folder) and
    processes all found files by parsing and producing the given records contained
    in each file as individual messages to a kafka topic.

    Requires defining methods for navigating folders and retrieving/opening raw
    files for the respective client.

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
        filepath: Union[str, Path],
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        has_partition_folders: bool = False,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param filepath: folder to recursively iterate from (a file will be used directly).
        :param key_setter: sets the kafka message key for a record in the file.
        :param value_setter: sets the kafka message value for a record in the file.
        :param timestamp_setter: sets the kafka message timestamp for a record in the file.
        :param file_format: what format the files are stored as (ex: "json").
        :param compression: what compression was used on the files, if any (ex. "gzip").
        :param has_partition_folders: whether files are nested within partition folders.
            If True, FileSource will match the output topic partition count with it.
            Set this flag to True if Quix Streams FileSink was used to dump data.
            Note: messages will only align with these partitions if original key is used.
            Example structure - a 2 partition topic (0, 1):
            [/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
        :param replay_speed: Produce messages with this speed multiplier, which
            roughly reflects the time "delay" between the original message producing.
            Use any float >= 0, where 0 is no delay, and 1 is the original speed.
            NOTE: Time delay will only be accurate per partition, NOT overall.
        :param name: The name of the Source application (Default: last folder name).
        :param shutdown_timeout: Time in seconds the application waits for the source
            to gracefully shut down.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        filepath = Path(filepath)
        super().__init__(
            name=name or f"file_{filepath.name}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._filepath = filepath
        self._key_setter = key_setter or _default_key_setter
        self._value_setter = value_setter or _default_value_setter
        self._timestamp_setter = timestamp_setter or _default_timestamp_setter
        self._has_partition_folders = has_partition_folders
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
        file_fetcher = FileFetcher(self.read_file, self.get_file_list(self._filepath))
        try:
            while self._running:
                filepath, raw_filestream = next(file_fetcher)
                logger.info(f"Processing file {filepath}...")
                for record in self._raw_filestream_deserializer(raw_filestream):
                    self.process_record(record)
                self._producer.flush()
        except StopIteration:
            logger.info("Finished processing all files.")
        finally:
            file_fetcher.stop()

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

    def file_partition_counter(self) -> int:
        """
        Can optionally define a way of counting folders to intelligently
        set the "default_topic" partition count to match partition folder count.

        If defined, class flag "has_partition_folders" can then be set to employ it.

        It is not required since this operation may not be easy to implement, and the
        file structure may not be used outside Quix Streams FileSink.

        Example structure with 2 partitions (0,1):
        ```
        topic_name/
        ├── 0/               # partition 0
        │   ├── file_a.ext
        │   └── file_b.ext
        └── 1/               # partition 1
            ├── file_x.ext
            └── file_y.ext
        ```
        """
        raise NotImplementedError(
            f'{self.__class__.__name__} must implement "file_partition_counter" method'
            f'to enable the option "has_partition_folders=True"'
        )

    def default_topic(self) -> Topic:
        """
        Optionally allows the file structure to define the partition count for the
        internal topic with file_partition_counter (instead of the default of 1).
        :return: the default topic with optionally altered partition count
        """
        topic = super().default_topic()
        if self._has_partition_folders:
            topic.create_config = TopicConfig(
                num_partitions=self.file_partition_counter(),
                replication_factor=1,
            )
        return topic


def _default_key_setter(row: dict) -> object:
    return row["_key"]


def _default_value_setter(row: dict) -> object:
    return row["_value"]


def _default_timestamp_setter(row: dict) -> int:
    return row["_timestamp"]
