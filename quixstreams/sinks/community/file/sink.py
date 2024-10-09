import logging
import os
from quixstreams.sinks import SinkBatch, BatchingSink
from quixstreams.sinks.base.item import SinkItem
from typing import Dict, List
from quixstreams.sinks.community.file.formats.base import FileFormatter

__all__ = ("FileSink",)

logger = logging.getLogger(__name__)


class FileSink(BatchingSink):
    """
    FileSink writes batches of data to files on disk using specified formats.
    Files are named using message keys, and data from multiple messages with the
    same key are appended to the same file where possible.
    """

    def __init__(
        self,
        output_dir: str,
        formatter: FileFormatter,
    ):
        """
        Initializes the FileSink with the specified configuration.

        Parameters:
            output_dir (str): The directory where files will be written.
            format (str): The data serialization format to use.
        """
        super().__init__()

        self._formatter = formatter
        self._output_dir = output_dir

        # Ensure the output directory exists.
        os.makedirs(self._output_dir, exist_ok=True)
        logger.info(f"Files will be written to '{self._output_dir}'.")

    def write(self, batch: SinkBatch):
        """
        Writes a batch of data to files on disk, grouping data by message key.

        Parameters:
            batch (SinkBatch): The batch of data to write.
        """
        try:
            # Group messages by key
            messages_by_key: Dict[str, List[SinkItem]] = {}
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

                # Generate filename based on the key
                safe_key = "".join(
                    [c if c.isalnum() or c in (" ", ".", "_") else "_" for c in key]
                )

                padded_offset = str(messages[0].offset).zfill(15)

                filename = f"{safe_key}/{padded_offset}{self._formatter.file_extension}"

                file_path = os.path.join(self._output_dir, filename)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)

                # Write data to a new file
                self._formatter.write_batch_values(file_path, messages)

                logger.info(f"Wrote {len(messages)} records to file '{file_path}'.")

        except Exception as e:
            logger.error(f"Error writing data to file: {e}")
            raise
