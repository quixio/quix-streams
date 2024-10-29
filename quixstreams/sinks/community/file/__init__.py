import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Hashable, Literal, Union

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
    Raised when format is specified incorrectly
    """


class FileSink(BatchingSink):
    """
    FileSink writes batches of data to files on disk using specified formats.
    Files are named using message keys, and data from multiple messages with the
    same key are appended to the same file where possible.
    """

    def __init__(self, output_dir: str, format: Union[FormatName, Format]) -> None:
        """
        Initializes the FileSink with the specified configuration.

        Parameters:
            output_dir (str): The directory where files will be written.
            format (S3SinkBatchFormat): The data serialization format to use.
        """
        super().__init__()
        self._format = self._resolve_format(format)
        self._output_dir = Path(output_dir)
        logger.info(f"Files will be written to '{self._output_dir}'.")

    def write(self, batch: SinkBatch) -> None:
        """
        Writes a batch of data to files on disk, grouping data by message key.

        Parameters:
            batch (SinkBatch): The batch of data to write.
        """

        # Group messages by key
        messages_by_key: dict[Hashable, list[Any]] = defaultdict(list)
        for message in batch:
            messages_by_key[message.key].append(message)

        _to_str = bytes.decode if isinstance(message.key, bytes) else str

        for key, messages in messages_by_key.items():
            # Serialize messages for this key using the specified format
            data = self._format.serialize(messages)

            # Generate filename based on the key
            safe_key = _UNSAFE_CHARACTERS_REGEX.sub("_", _to_str(key))

            directory = self._output_dir / safe_key
            directory.mkdir(parents=True, exist_ok=True)

            padded_offset = str(messages[0].offset).zfill(15)
            file_path = directory / (padded_offset + self._format.file_extension)

            # Write data to a new file
            with open(file_path, "wb") as f:
                f.write(data)

            logger.info(f"Wrote {len(messages)} records to file '{file_path}'.")

    def _resolve_format(self, format: Union[FormatName, Format]) -> Format:
        if isinstance(format, Format):
            return format
        elif format_obj := _FORMATS.get(format):
            return format_obj

        allowed_formats = ", ".join(Format.__args__)
        raise InvalidFormatError(
            f'Invalid format name "{format}". '
            f"Allowed values: {allowed_formats}, "
            f"or an instance of {Format.__class__.__name__}."
        )
