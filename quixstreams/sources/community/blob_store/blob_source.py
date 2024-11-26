import logging
from pathlib import Path
from typing import Generator, Optional, Union

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
        blob_folder: Optional[Union[str, Path]] = None,
        blob_file: Optional[Union[str, Path]] = None,
        as_replay: bool = True,
        name: Optional[str] = None,
        shutdown_timeout: float = 10.0,
    ):
        self._client = blob_client
        self._blob_file = Path(blob_file) if blob_file else None
        self._blob_folder = Path(blob_folder) if blob_folder else None

        super().__init__(
            filepath=self._client.location,
            file_format=blob_format,
            file_compression=blob_compression,
            as_replay=as_replay,
            name=name or self._client.location,
            shutdown_timeout=shutdown_timeout,
        )

    def _get_partition_count(self) -> int:
        return self._client.get_root_folder_count(self._blob_folder)

    def _file_read(self, file: Path) -> Generator[dict, None, None]:
        yield from super()._file_read(self._client.get_raw_blob_stream(file))

    def _file_list(self) -> Generator[Path, None, None]:
        if self._blob_file:
            yield self._blob_file
        else:
            yield from self._client.blob_collector(self._blob_folder)
