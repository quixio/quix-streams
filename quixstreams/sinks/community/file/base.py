import logging
import re
from abc import abstractmethod
from pathlib import Path
from typing import Optional, Union

from quixstreams.sinks import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBatch,
)

from .formats import Format, FormatName, resolve_format

__all__ = ("FileSink",)

logger = logging.getLogger(__name__)


_UNSAFE_CHARACTERS_REGEX = re.compile(r"[^a-zA-Z0-9 ._/]")


class FileSink(BatchingSink):
    """A sink that writes data batches to files using configurable formats and
    destinations.

    The sink groups messages by their topic and partition, ensuring data from the
    same source is stored together. Each batch is serialized using the specified
    format (e.g., JSON, Parquet) before being written to the configured
    destination.

    The destination determines the storage location and write behavior. By default,
    it uses LocalDestination for writing to the local filesystem, but can be
    configured to use other storage backends (e.g., cloud storage).
    """

    def __init__(
        self,
        directory: str = "",
        format: Union[FormatName, Format] = "json",
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        """Initialize the FileSink with the specified configuration.

        :param directory: Base directory path for storing files. Defaults to
            current directory.
        :param format: Data serialization format, either as a string
            ("json", "parquet") or a Format instance.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        if _UNSAFE_CHARACTERS_REGEX.search(directory):
            raise ValueError(
                f"Invalid characters in directory path: {directory}. "
                f"Only alphanumeric characters (a-zA-Z0-9), spaces ( ), "
                "dots (.), and underscores (_) are allowed."
            )
        self._base_directory = directory
        logger.info("Directory set to '%s'", directory)

        self._format = resolve_format(format)
        logger.info("File extension set to '%s'", self._format.file_extension)

    @abstractmethod
    def setup(self):
        """Authenticate and validate connection here"""
        ...

    @abstractmethod
    def _write(self, data: bytes, batch: SinkBatch) -> None:
        """
        Write the serialized data to storage.

        :param data: The serialized data to write.
        :param batch: The batch object containing topic, partition and offset details.
        """
        ...

    def write(self, batch: SinkBatch) -> None:
        """
        Write a batch of data using the configured format.

        :param batch: The batch of data to write.
        """
        self._write(self._format.serialize(batch), batch)

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
        return f"{batch.start_offset:019d}{self._format.file_extension}"
