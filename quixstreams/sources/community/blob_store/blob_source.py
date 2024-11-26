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
        filepath: Union[str, Path],
        blob_compression: Optional[CompressionName] = None,
        as_replay: bool = True,
        name: Optional[str] = None,
        shutdown_timeout: float = 10.0,
    ):
        self._client = blob_client

        super().__init__(
            filepath=filepath,
            file_format=blob_format,
            file_compression=blob_compression,
            as_replay=as_replay,
            name=name or self._client.root_location,
            shutdown_timeout=shutdown_timeout,
        )

    def _get_partition_count(self) -> int:
        return self._client.get_root_folder_count(self._filepath)

    def _file_read(self, file: Path) -> Generator[dict, None, None]:
        yield from super()._file_read(self._client.get_raw_blob_stream(file))

    def _file_list(self) -> Generator[Path, None, None]:
        yield from self._client.blob_collector(self._filepath)
