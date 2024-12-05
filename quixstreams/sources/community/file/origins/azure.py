import os
from io import BytesIO
from pathlib import Path
from typing import Generator

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
        self.root_location = container
        self._client = self._get_client(connection_string)

    def _get_client(self, auth: str) -> ContainerClient:
        """
        Get an Azure file container client and validate the container exists.

        :param auth: Azure client authentication string.
        :return: An Azure ContainerClient
        """
        storage_client = BlobServiceClient.from_connection_string(auth)
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
