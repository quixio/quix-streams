from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

from .base import FileOrigin

try:
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._container_client import ContainerClient
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[azure]" to use AzureFileOrigin'
    ) from exc

__all__ = ("AzureFileOrigin",)


class AzureFileOrigin(FileOrigin):
    def get_folder_count(self, filepath: Path) -> int:
        data = self.client.list_blobs(name_starts_with=str(filepath))
        folders = [f for page in data.by_page() for f in page.get("blob_prefixes", [])]
        return len(folders)

    def __init__(
        self,
        connection_string: str,
        container: str,
    ):
        self._client: Optional[ContainerClient] = None
        self.root_location = container
        self._credentials = connection_string

    @property
    def client(self):
        if not self._client:
            blob_client = BlobServiceClient.from_connection_string(self._credentials)
            container_client = blob_client.get_container_client(self.root_location)
            self._client: ContainerClient = container_client
        return self._client

    def file_collector(self, folder: Path) -> Generator[str, None, None]:
        data = self.client.list_blob_names(name_starts_with=str(folder))
        for page in data.by_page():
            for item in page:
                yield item

    def get_raw_file_stream(self, blob_name: Path) -> BytesIO:
        blob_client = self.client.get_blob_client(str(blob_name))
        data = blob_client.download_blob().readall()
        return BytesIO(data)
