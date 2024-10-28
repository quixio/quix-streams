import logging
import os
from typing import Any, Dict, List, Literal, Union

from file_formats import BatchFormat
from formats.bytes_format import BytesFormat
from formats.json_format import JSONFormat
from formats.parquet_format import ParquetFormat

from quixstreams.sinks import BatchingSink, SinkBatch

LogLevel = Literal[
    "CRITICAL",
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
    "NOTSET",
]
FormatSpec = Literal["bytes", "json", "parquet"]

_SINK_FORMATTERS: Dict[FormatSpec, BatchFormat] = {
    "json": JSONFormat(),
    "bytes": BytesFormat(),
    "parquet": ParquetFormat(),
}


class InvalidS3FormatterError(Exception):
    """
    Raised when S3 formatter is specified incorrectly
    """


class FileSink(BatchingSink):
    """
    FileSink writes batches of data to files on disk using specified formats.
    Files are named using message keys, and data from multiple messages with the
    same key are appended to the same file where possible.
    """

    def __init__(
        self, output_dir: str, format: FormatSpec, loglevel: LogLevel = "INFO"
    ):
        """
        Initializes the FileSink with the specified configuration.

        Parameters:
            output_dir (str): The directory where files will be written.
            format (S3SinkBatchFormat): The data serialization format to use.
            loglevel (LogLevel): The logging level for the logger (default is 'INFO').
        """
        super().__init__()

        # Configure logging.
        self._logger = logging.getLogger("FileSink")
        log_format = (
            "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(name)s] : %(message)s"
        )
        logging.basicConfig(format=log_format, datefmt="%Y-%m-%d %H:%M:%S")
        self._logger.setLevel(loglevel)

        self._format = self._resolve_format(format)
        self._output_dir = output_dir

        # Ensure the output directory exists.
        os.makedirs(self._output_dir, exist_ok=True)
        self._logger.info(f"Files will be written to '{self._output_dir}'.")

    def write(self, batch: SinkBatch):
        """
        Writes a batch of data to files on disk, grouping data by message key.

        Parameters:
            batch (SinkBatch): The batch of data to write.
        """
        try:
            # Group messages by key
            messages_by_key: Dict[str, List[Any]] = {}
            for message in batch:
                key = (
                    message.key.decode()
                    if isinstance(message.key, bytes)
                    else str(message.key)
                )
                if key not in messages_by_key:
                    messages_by_key[key] = []
                messages_by_key[key].append(message)

            for key, messages in messages_by_key.items():
                # Serialize messages for this key using the specified format
                data = self._format.serialize_batch_values(messages)

                # Generate filename based on the key
                safe_key = "".join(
                    [c if c.isalnum() or c in (" ", ".", "_") else "_" for c in key]
                )

                padded_offset = str(messages[0].offset).zfill(15)

                filename = f"{safe_key}/{padded_offset}{self._format.file_extension}"

                file_path = os.path.join(self._output_dir, filename)

                # Get the folder path
                folder_path = os.path.dirname(file_path)

                # Create the folder if it doesn't exist
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)

                # Write data to a new file
                with open(file_path, "wb") as f:
                    f.write(data)

                self._logger.info(
                    f"Wrote {len(messages)} records to file '{file_path}'."
                )

        except Exception as e:
            self._logger.error(f"Error writing data to file: {e}")
            raise

    def _resolve_format(
        self,
        formatter_spec: Union[
            Literal["bytes"], Literal["json"], Literal["parquet"], BatchFormat
        ],
    ) -> BatchFormat:
        if isinstance(formatter_spec, BatchFormat):
            return formatter_spec

        formatter_obj = _SINK_FORMATTERS.get(formatter_spec)
        if formatter_obj is None:
            raise InvalidS3FormatterError(
                f'Invalid format name "{formatter_obj}". '
                f'Allowed values: "json", "bytes", "parquet", '
                f"or an instance of {BatchFormat.__class__.__name__} "
            )
        return formatter_obj
