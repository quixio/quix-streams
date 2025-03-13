import logging
from io import BytesIO
from pathlib import Path
from typing import Callable, Iterable, Optional, Union

from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
)

from .base import FileSource
from .compressions import CompressionName
from .formats import Format, FormatName

try:
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._container_client import ContainerClient
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[azure-file]" to use AzureFileOrigin'
    ) from exc

__all__ = ("AzureFileSource",)


logger = logging.getLogger(__name__)


class AzureFileSource(FileSource):
    def __init__(
        self,
        connection_string: str,
        container: str,
        directory: Union[str, Path],
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param connection_string: Azure client authentication string.
        :param container: Azure container name.
        """
        super().__init__(
            directory=directory,
            key_setter=key_setter,
            value_setter=value_setter,
            timestamp_setter=timestamp_setter,
            file_format=file_format,
            compression=compression,
            replay_speed=replay_speed,
            name=name,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._container = container
        self._auth = connection_string
        self._client: Optional[ContainerClient] = None

    def _get_client(self) -> ContainerClient:
        """
        Get an Azure file container client and validate the container exists.
        :return: An Azure ContainerClient
        """
        storage_client = BlobServiceClient.from_connection_string(self._auth)
        container_client = storage_client.get_container_client(self._container)
        return container_client

    def get_file_list(self, filepath: Path) -> Iterable[Path]:
        data = self._client.list_blob_names(name_starts_with=str(filepath))
        for page in data.by_page():
            for item in page:
                yield Path(item)

    def read_file(self, filepath: Path) -> BytesIO:
        blob_client = self._client.get_blob_client(str(filepath))
        data = blob_client.download_blob().readall()
        return BytesIO(data)
