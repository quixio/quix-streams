import logging
from pathlib import Path
from typing import Optional, Union

from quixstreams.sinks import SinkBatch

from .base import FileSink
from .formats import Format, FormatName

__all__ = ("LocalFileSink",)

logger = logging.getLogger(__name__)


class AppendNotSupported(Exception):
    """Raised when append=True but specified format does not support it"""


class LocalFileSink(FileSink):
    """A destination that writes data to the local filesystem.

    Handles writing data to local files with support for both creating new files
    and appending to existing ones.
    """

    def __init__(
        self,
        append: bool = False,
        directory: str = "",
        format: Union[FormatName, Format] = "json",
    ) -> None:
        """Initialize the local destination.

        :param append: If True, append to existing files instead of creating new
            ones by selecting the lexicographical last file in the given directory
            (or creates one).
            Defaults to False.
        :param directory: Base directory path for storing files. Defaults to
            current directory.
        :param format: Data serialization format, either as a string
            ("json", "parquet") or a Format instance.

        :raises AppendNotSupported: If append=True but given format does not support it.
        """
        super().__init__(directory=directory, format=format)
        if append and not self._format.supports_append:
            raise AppendNotSupported(
                f"File format {self._format.file_extension} does not support appending."
            )

        logger.debug("LocalFileSink initialized with append=%s", append)
        self._mode = "ab" if append else "wb"
        self._append = append

    def setup(self):
        return

    def _write(self, data: bytes, batch: SinkBatch) -> None:
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
