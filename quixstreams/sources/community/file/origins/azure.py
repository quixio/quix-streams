from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

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
    def get_folder_count(self, filepath: Path) -> int:
        data = self._client.list_blobs(name_starts_with=str(filepath))
        folders = [f for page in data.by_page() for f in page.get("blob_prefixes", [])]
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
        self._client: Optional[ContainerClient] = self._get_client(connection_string)
        self.root_location = container

    def _get_client(self, auth: str):
        if not self._client:
            blob_client = BlobServiceClient.from_connection_string(auth)
            container_client = blob_client.get_container_client(self.root_location)
            self._client: ContainerClient = container_client
        return self._client

    def file_collector(self, folder: Path) -> Generator[Path, None, None]:
        data = self._client.list_blob_names(name_starts_with=str(folder))
        for page in data.by_page():
            for item in page:
                yield Path(item)

    def get_raw_file_stream(self, blob_name: Path) -> BytesIO:
        blob_client = self._client.get_blob_client(str(blob_name))
        data = blob_client.download_blob().readall()
        return BytesIO(data)
