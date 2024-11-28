import os
from io import BytesIO
from pathlib import Path
from typing import Generator

from .base import ExternalOrigin

try:
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._container_client import ContainerClient
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[azure]" to use AzureOrigin'
    ) from exc

__all__ = ("AzureOrigin",)


class AzureOrigin(ExternalOrigin):
    def get_folder_count(self, path: Path) -> int:
        """
        This is a simplified version of the recommended way to retrieve folder
        names based on the azure SDK docs examples.
        """
        path = f"{path}/"
        folders = set()
        for blob in self._client.list_blobs(name_starts_with=path):
            relative_dir = os.path.dirname(os.path.relpath(blob.name, path))
            if relative_dir and ("/" not in relative_dir):
                folders.add(relative_dir)
        return len(folders)

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
        blob_client = BlobServiceClient.from_connection_string(auth)
        return blob_client.get_container_client(self.root_location)

    def file_collector(self, folder: Path) -> Generator[Path, None, None]:
        data = self._client.list_blob_names(name_starts_with=str(folder))
        for page in data.by_page():
            for item in page:
                yield Path(item)

    def get_raw_file_stream(self, blob_name: Path) -> BytesIO:
        blob_client = self._client.get_blob_client(str(blob_name))
        data = blob_client.download_blob().readall()
        return BytesIO(data)
