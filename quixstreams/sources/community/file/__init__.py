import logging
from pathlib import Path
from time import sleep
from typing import Generator, Optional, Union

from quixstreams.models import Topic, TopicConfig
from quixstreams.sources import Source

from .compressions import CompressionName
from .formats import FORMAT_MAPPER, Format, FormatName

__all__ = ("FileSource",)

logger = logging.getLogger(__name__)


class FileSource(Source):
    def __init__(
        self,
        filepath: Union[str, Path],
        file_format: FormatName,
        file_compression: Optional[CompressionName] = None,
        as_replay: bool = True,
        name: Optional[str] = None,
        shutdown_timeout: float = 10,
    ):
        self._filepath = Path(filepath)
        self._formatter = _get_formatter(file_format, file_compression)
        self._as_replay = as_replay
        self._previous_timestamp = None
        self._previous_folder_name = None
        super().__init__(
            name=name or self._filepath.name, shutdown_timeout=shutdown_timeout
        )

    def _replay_delay(self, current_timestamp: int):
        """
        Apply the replay speed by calculating the delay between messages
        based on their timestamps.
        """
        if self._previous_timestamp is not None:
            time_diff = (
                current_timestamp - self._previous_timestamp
            ) / 1000.0  # Convert ms to seconds
            if time_diff > 0:
                logger.debug(f"Sleeping for {time_diff} seconds...")
                sleep(time_diff)

    def _get_partition_count(self) -> int:
        return len([f for f in self._filepath.iterdir()])

    def default_topic(self) -> Topic:
        topic = super().default_topic()
        topic.config = TopicConfig(
            num_partitions=self._get_partition_count(), replication_factor=1
        )
        return topic

    def _check_folder_name(self, file: Path):
        """
        Checks whether the next file is the start of a new key or partition so the
        timestamp tracker can be reset.
        """
        name = file.parent.name
        if self._previous_folder_name != name:
            self._previous_timestamp = None
            self._previous_folder_name = name
            logger.debug(f"Beginning reading {name}")

    def _produce(self, record: dict):
        kafka_msg = self._producer_topic.serialize(
            key=record["_key"],
            value=record["_value"],
            timestamp_ms=record["_timestamp"],
        )
        self.produce(
            key=kafka_msg.key, value=kafka_msg.value, timestamp=kafka_msg.timestamp
        )

    def run(self):
        while self._running:
            for file in _recursive_file_grabber(self._filepath):
                logger.info(f"Reading files from topic {self._filepath.name}")
                self._check_folder_name(file)
                for record in self._formatter.deserialize(file):
                    if self._as_replay:
                        self._replay_delay(record["_timestamp"])
                    self._produce(record)
                self.flush()
            return


def _get_formatter(name: FormatName, compression: Optional[CompressionName]) -> Format:
    return FORMAT_MAPPER[name](compression)


def _recursive_file_grabber(filepath: Path) -> Generator[Path, None, None]:
    if filepath.is_dir():
        for i in filepath.iterdir():
            yield from _recursive_file_grabber(i)
    else:
        yield filepath
