import logging
import re
from pathlib import Path
from typing import Literal, Union

from quixstreams.sinks import BatchingSink, SinkBatch

from .formats import BytesFormat, Format, JSONFormat, ParquetFormat

__all__ = [
    "BatchFormat",
    "BytesFormat",
    "FileSink",
    "InvalidFormatError",
    "JSONFormat",
    "ParquetFormat",
]

logger = logging.getLogger(__name__)

FormatName = Literal["bytes", "json", "parquet"]

_FORMATS: dict[FormatName, Format] = {
    "bytes": BytesFormat(),
    "json": JSONFormat(),
    "parquet": ParquetFormat(),
}

_UNSAFE_CHARACTERS_REGEX = re.compile(r"[^a-zA-Z0-9 ._]")


class InvalidFormatError(Exception):
    """
    Raised when the format is specified incorrectly.
    """


class FileSink(BatchingSink):
    """
    Writes batches of data to files on disk using specified formats.

    Messages are grouped by their topic and partition. Data from messages with
    the same topic and partition are saved in the same directory. Each batch of
    messages is serialized and saved to a new file within that directory. Files
    are named using the batch's starting offset to ensure uniqueness and order.
    """

    def __init__(self, output_dir: str, format: Union[FormatName, Format]) -> None:
        """
        Initializes the FileSink.

        :param output_dir: The directory where files will be written.
        :param format: The data serialization format to use. This can be either a
            format name ("bytes", "json", "parquet") or an instance of a `Format`
            subclass.
        """
        super().__init__()
        self._format = self._resolve_format(format)
        self._output_dir = output_dir  # TODO: validate
        logger.info(f"Files will be written to '{self._output_dir}'.")

    def write(self, batch: SinkBatch) -> None:
        """
        Writes a batch of data to files on disk, grouping data by topic and partition.

        :param batch: The batch of data to write.
        """

        # Generate directory based on topic and partition
        directory = Path(self._output_dir)
        directory /= _UNSAFE_CHARACTERS_REGEX.sub("_", batch.topic)
        directory /= _UNSAFE_CHARACTERS_REGEX.sub("_", str(batch.partition))
        directory.mkdir(parents=True, exist_ok=True)

        # Generate filename based on the batch's starting offset
        # Padded to cover max length of a signed 64-bit integer (19 digits)
        # e.g., 0000000000000123456
        padded_offset = str(batch.start_offset).zfill(19)
        file_path = directory / (padded_offset + self._format.file_extension)

        # Serialize messages using the specified format
        data = self._format.serialize(batch)

        # Write data to a new file
        with open(file_path, "wb") as f:
            f.write(data)

        logger.info(f"Wrote {batch.size} records to file '{file_path}'.")

    def _resolve_format(self, format: Union[FormatName, Format]) -> Format:
        """
        Resolves the format into a `Format` instance.

        :param format: The format to resolve, either a format name ("bytes", "json",
            "parquet") or a `Format` instance.
        :return: An instance of `Format` corresponding to the specified format.
        :raises InvalidFormatError: If the format name is invalid.
        """
        if isinstance(format, Format):
            return format
        elif format_obj := _FORMATS.get(format):
            return format_obj

        allowed_formats = ", ".join(FormatName.__args__)
        raise InvalidFormatError(
            f'Invalid format name "{format}". '
            f"Allowed values: {allowed_formats}, "
            f"or an instance of a subclass of `Format`."
        )
