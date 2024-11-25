import logging
from pathlib import Path
from typing import Optional, Union

from quixstreams.sources.community.file import FileSource
from quixstreams.sources.community.file.compressions import CompressionName
from quixstreams.sources.community.file.formats import Format, FormatName

from .clients.base import BlobClient

logger = logging.getLogger(__name__)


__all__ = ("BlobFileSource",)


class BlobFileSource(FileSource):
    def __init__(
        self,
        blob_client: BlobClient,
        blob_format: Union[FormatName, Format],
        blob_compression: Optional[CompressionName] = None,
        blob_folder: Optional[str] = None,
        blob_file: Optional[str] = None,
        as_replay: bool = True,
        name: Optional[str] = None,
        shutdown_timeout: float = 10.0,
    ):
        self._client = blob_client
        self._blob_file = blob_file
        self._blob_folder = blob_folder

        super().__init__(
            filepath=self._client.location,
            file_format=blob_format,
            file_compression=blob_compression,
            as_replay=as_replay,
            name=name or self._client.location,
            shutdown_timeout=shutdown_timeout,
        )

    def _get_partition_count(self) -> int:
        return 1

    def _check_file_partition_number(self, file: Path) -> int:
        return 0

    def run(self):
        blobs = (
            [self._blob_file]
            if self._blob_file
            else self._client.blob_finder(self._blob_folder)
        )
        while self._running:
            for file in blobs:
                self._check_file_partition_number(Path(file))
                filestream = self._client.get_raw_blob_stream(file)
                for record in self._formatter.file_read(filestream):
                    if self._as_replay and (timestamp := record.get("_timestamp")):
                        self._replay_delay(timestamp)
                    self._produce(record)
                self.flush()
            return
