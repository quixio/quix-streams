from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

from .base import BlobClient

try:
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._container_client import ContainerClient
except Exception:
    raise

__all__ = ("AzureBlobClient",)


class AzureBlobClient(BlobClient):
    def get_root_folder_count(self, filepath: Path) -> int:
        # TODO: implement
        ...

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

    def blob_collector(self, folder: Path) -> Generator[str, None, None]:
        # TODO: Recursively navigate folders.
        for item in self.client.list_blob_names(name_starts_with=str(folder)):
            yield item

    def get_raw_blob_stream(self, blob_name: Path) -> BytesIO:
        blob_client = self.client.get_blob_client(str(blob_name))
        data = blob_client.download_blob().readall()
        return BytesIO(data)
