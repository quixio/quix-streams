import logging
import re
from pathlib import Path
from typing import Literal, Union

from quixstreams.sinks import BatchingSink, SinkBatch

from .formats import Format, JSONFormat, ParquetFormat

__all__ = ["FileSink", "InvalidFormatError"]

logger = logging.getLogger(__name__)

FormatName = Literal["json", "parquet"]

_FORMATS: dict[FormatName, Format] = {
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
    messages is serialized and saved to a file within that directory. Files are
    named using the batch's starting offset to ensure uniqueness and order.

    If `append` is set to `True`, the sink will attempt to append data to an
    existing file rather than creating a new one. This is only supported for
    formats that allow appending.
    """

    def __init__(
        self, output_dir: str, format: Union[FormatName, Format], append: bool = False
    ) -> None:
        """
        Initializes the FileSink.

        :param output_dir: The directory where files will be written.
        :param format: The data serialization format to use. This can be either a
            format name ("json", "parquet") or an instance of a `Format`
            subclass.
        :param append: If `True`, data will be appended to existing files when possible.
            Note that not all formats support appending. Defaults to `False`.
        :raises ValueError: If `append` is `True` but the specified format does not
            support appending.
        """
        super().__init__()
        self._format = self._resolve_format(format)
        self._output_dir = output_dir  # TODO: validate
        if append and not self._format.supports_append:
            raise ValueError(f"`{format}` format does not support appending.")
        self._append = append
        self._file_mode = "ab" if append else "wb"
        logger.info(f"Files will be written to '{self._output_dir}'.")

    def write(self, batch: SinkBatch) -> None:
        """
        Writes a batch of data to files on disk, grouping data by topic and partition.

        If `append` is `True` and an existing file is found, data will be appended to
        the last file. Otherwise, a new file is created based on the batch's starting
        offset.

        :param batch: The batch of data to write.
        """
        file_path = self._get_file_path(batch)

        # Serialize messages using the specified format
        data = self._format.serialize(batch)

        # Write data to the file
        with open(file_path, self._file_mode) as f:
            f.write(data)

        logger.info(f"Wrote {batch.size} records to file '{file_path}'.")

    def _get_file_path(self, batch: SinkBatch) -> Path:
        """
        Generate and return the file path for storing data related to the given batch.

        The file path is constructed based on the output directory, with sanitized
        topic and partition names to ensure valid file paths. If appending is enabled
        and existing files are found in the directory, the latest existing file is
        returned for appending data. Otherwise, a new file path is generated using
        the batch's starting offset, padded to 19 digits, and appended with the
        appropriate file extension.

        :param batch: The batch object containing attributes:
                    - topic (str): The name of the topic associated with the batch.
                    - partition (int): The partition number of the batch.
                    - start_offset (int): The starting offset of the batch.
        :return: The file path where the batch data should be stored.

        Notes:
            - Unsafe characters in `topic` and `partition` are replaced with underscores
              `_` to ensure valid directory names.
            - The directory structure is organized as: `output_dir/topic/partition/`.
            - File names are based on the starting offset of the batch, padded to 19
              digits (e.g., `0000000000000123456`), to accommodate the maximum length
              of a signed 64-bit integer.
            - If appending is enabled (`self._append` is `True`) and existing files are
              present, data will be appended to the latest existing file.
        """
        directory = Path(self._output_dir)
        directory /= _UNSAFE_CHARACTERS_REGEX.sub("_", batch.topic)
        directory /= _UNSAFE_CHARACTERS_REGEX.sub("_", str(batch.partition))
        directory.mkdir(parents=True, exist_ok=True)

        if self._append and (existing_files := self._get_existing_files(directory)):
            return existing_files[-1]
        else:
            # Generate filename based on the batch's starting offset
            # Padded to cover max length of a signed 64-bit integer (19 digits)
            # e.g., 0000000000000123456
            padded_offset = str(batch.start_offset).zfill(19)
            return directory / (padded_offset + self._format.file_extension)

    def _get_existing_files(self, directory: Path) -> list[Path]:
        """
        Retrieves a sorted list of existing files in the given directory that match
        the current format's file extension.

        :param directory: The directory to search for existing files.
        :return: A list of Path objects to existing files, sorted by name.
        """
        return sorted(
            path
            for path in directory.iterdir()
            if path.suffix == self._format.file_extension
        )

    def _resolve_format(self, format: Union[FormatName, Format]) -> Format:
        """
        Resolves the format into a `Format` instance.

        :param format: The format to resolve, either a format name ("json",
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
