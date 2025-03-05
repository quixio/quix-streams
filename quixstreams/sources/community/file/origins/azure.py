import logging
import os
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

from typing_extensions import Self

from .base import Origin

try:
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._container_client import ContainerClient
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[azure-file]" to use AzureFileOrigin'
    ) from exc

__all__ = ("AzureFileOrigin",)


logger = logging.getLogger(__name__)


class AzureFileOrigin(Origin):
    def __init__(
        self,
        connection_string: str,
        container: str,
    ):
        """
        :param connection_string: Azure client authentication string.
        :param container: Azure container name.
        """
        self._container = container
        self._auth = connection_string
        self._client: Optional[ContainerClient] = None

    def _get_client(self) -> ContainerClient:
        """
        Get an Azure file container client and validate the container exists.

        :param auth: Azure client authentication string.
        :return: An Azure ContainerClient
        """
        storage_client = BlobServiceClient.from_connection_string(self._auth)
        container_client = storage_client.get_container_client(self._container)
        return container_client

    def file_collector(self, filepath: Path) -> Generator[Path, None, None]:
        data = self._client.list_blob_names(name_starts_with=str(filepath))
        for page in data.by_page():
            for item in page:
                yield Path(item)

    def get_folder_count(self, directory: Path) -> int:
        """
        This is a simplified version of the recommended way to retrieve folder
        names based on the azure SDK docs examples.
        """
        path = f"{directory}/"
        folders = set()
        for blob in self._client.list_blobs(name_starts_with=path):
            relative_dir = os.path.dirname(os.path.relpath(blob.name, path))
            if relative_dir and ("/" not in relative_dir):
                folders.add(relative_dir)
        return len(folders)

    def get_raw_file_stream(self, filepath: Path) -> BytesIO:
        blob_client = self._client.get_blob_client(str(filepath))
        data = blob_client.download_blob().readall()
        return BytesIO(data)

    def __enter__(self) -> Self:
        if not self._client:
            self._client = self._get_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.info(f"Source client session exited non-gracefully: {e}")
                pass
        self._client = None
