import logging
from pathlib import Path
from typing import Optional, Union

from quixstreams.sources.community.file import FileSource

from .clients.base import BlobClient
from .compressions import CompressionName
from .formats import Format, FormatName

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
        self._blob_client = blob_client
        if blob_file:
            self._blobs = lambda: [blob_file]
        else:
            self._blobs = lambda: self._blob_client.blob_finder(blob_folder)

        super().__init__(
            filepath=self._blob_client.location,
            file_format=blob_format,
            file_compression=blob_compression,
            as_replay=as_replay,
            name=name or self._blob_client.location,
            shutdown_timeout=shutdown_timeout,
        )

    def _get_partition_count(self) -> int:
        return 1

    def run(self):
        while self._running:
            for file in self._blobs():
                self._check_file_partition_number(Path(file))
                filestream = self._blob_client.get_raw_blob(file)
                for record in self._formatter.file_read(filestream):
                    if self._as_replay and (timestamp := record.get("_timestamp")):
                        self._replay_delay(timestamp)
                    self._produce(record)
                self.flush()
            return
