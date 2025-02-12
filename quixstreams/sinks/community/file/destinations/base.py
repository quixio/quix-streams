import logging
import re
from abc import ABC, abstractmethod
from pathlib import Path

from quixstreams.sinks.base import SinkBatch
from quixstreams.sinks.community.file.formats import Format

__all__ = ("Destination",)

logger = logging.getLogger(__name__)

_UNSAFE_CHARACTERS_REGEX = re.compile(r"[^a-zA-Z0-9 ._/]")


class Destination(ABC):
    """Abstract base class for defining where and how data should be stored.

    Destinations handle the storage of serialized data, whether that's to local
    disk, cloud storage, or other locations. They manage the physical writing of
    data while maintaining a consistent directory/path structure based on topics
    and partitions.
    """

    _base_directory: str = ""
    _extension: str = ""

    @abstractmethod
    def setup(self):
        """Authenticate and validate connection here"""
        ...

    @abstractmethod
    def write(self, data: bytes, batch: SinkBatch) -> None:
        """Write the serialized data to storage.

        :param data: The serialized data to write.
        :param batch: The batch information containing topic, partition and offset
            details.
        """
        ...

    def set_directory(
        self,
        directory: str,
    ) -> None:
        """Configure the base directory for storing files.

        :param directory: The base directory path where files will be stored.
        :raises ValueError: If the directory path contains invalid characters.
            Only alphanumeric characters (a-zA-Z0-9), spaces, dots, slashes, and
            underscores are allowed.
        """
        # TODO: logic around "/" for sinks that require special handling of them
        if _UNSAFE_CHARACTERS_REGEX.search(directory):
            raise ValueError(
                f"Invalid characters in directory path: {directory}. "
                f"Only alphanumeric characters (a-zA-Z0-9), spaces ( ), "
                "dots (.), and underscores (_) are allowed."
            )
        self._base_directory = directory
        logger.info("Directory set to '%s'", directory)

    def set_extension(self, format: Format) -> None:
        """Set the file extension based on the format.

        :param format: The Format instance that defines the file extension.
        """
        self._extension = format.file_extension
        logger.info("File extension set to '%s'", self._extension)

    def _path(self, batch: SinkBatch) -> Path:
        """Generate the full path where the batch data should be stored.

        :param batch: The batch information containing topic, partition and offset
            details.
        :return: A Path object representing the full file path.
        """
        return self._directory(batch) / self._filename(batch)

    def _directory(self, batch: SinkBatch) -> Path:
        """Generate the full directory path for a batch.

        Creates a directory structure using the base directory, sanitized topic
        name, and partition number.

        :param batch: The batch information containing topic and partition details.
        :return: A Path object representing the directory where files should be
            stored.
        """
        topic = _UNSAFE_CHARACTERS_REGEX.sub("_", batch.topic)
        return Path(self._base_directory) / topic / str(batch.partition)

    def _filename(self, batch: SinkBatch) -> str:
        """Generate the filename for a batch.

        Creates a filename using the batch's starting offset as a zero-padded
        number to ensure correct ordering. The offset is padded to cover the max
        length of a signed 64-bit integer (19 digits), e.g., '0000000000000123456'.

        :param batch: The batch information containing start_offset details.
        :return: A string representing the filename with zero-padded offset and
            extension.
        """
        return f"{batch.start_offset:019d}{self._extension}"
