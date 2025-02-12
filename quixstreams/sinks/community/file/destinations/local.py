import logging
from pathlib import Path
from typing import Optional

from quixstreams.sinks.base import SinkBatch
from quixstreams.sinks.community.file.formats import Format

from .base import Destination

__all__ = ("LocalDestination",)

logger = logging.getLogger(__name__)


class LocalDestination(Destination):
    """A destination that writes data to the local filesystem.

    Handles writing data to local files with support for both creating new files
    and appending to existing ones.
    """

    def __init__(self, append: bool = False) -> None:
        """Initialize the local destination.

        :param append: If True, append to existing files instead of creating new
            ones. Defaults to False.
        """
        self._append = append
        self._mode = "ab" if append else "wb"
        logger.debug("LocalDestination initialized with append=%s", append)

    def setup(self):
        return

    def set_extension(self, format: Format) -> None:
        """Set the file extension and validate append mode compatibility.

        :param format: The Format instance that defines the file extension.
        :raises ValueError: If append mode is enabled but the format doesn't
            support appending.
        """
        if self._append and not format.supports_append:
            raise ValueError(f"`{format}` format does not support appending.")
        super().set_extension(format)

    def write(self, data: bytes, batch: SinkBatch) -> None:
        """Write data to a local file.

        :param data: The serialized data to write.
        :param batch: The batch information containing topic and partition details.
        """
        path = self._path(batch)
        logger.debug("Writing %d bytes to file: %s", len(data), path)
        with open(path, self._mode) as f:
            f.write(data)

    def _path(self, batch: SinkBatch) -> Path:
        """Get the path for writing, creating directories if needed.

        :param batch: The batch information containing topic and partition details.
        :return: Path where the data should be written.
        """
        directory = self._directory(batch)
        directory.mkdir(parents=True, exist_ok=True)
        return self._existing_file(directory) or directory / self._filename(batch)

    def _existing_file(self, directory: Path) -> Optional[Path]:
        """Find the most recent file in the directory for append mode.

        :param directory: Directory to search for existing files.
        :return: Path to the most recent file if in append mode and files exist,
            None otherwise.
        """
        if self._append and (files := sorted(directory.iterdir())):
            file = files[-1]
            logger.debug("Found existing file for append: %s", file)
            return file
        return None
