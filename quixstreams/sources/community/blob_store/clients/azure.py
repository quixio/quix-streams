from io import BytesIO
from typing import Generator, Optional

from .base import BlobClient

try:
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._container_client import ContainerClient
except Exception:
    raise

__all__ = ("AzureBlobClient",)


class AzureBlobClient(BlobClient):
    def __init__(
        self,
        connection_string: str,
        container: str,
    ):
        self._client: Optional[ContainerClient] = None
        self.location = container
        self._credentials = connection_string

    @property
    def client(self):
        if not self._client:
            blob_client = BlobServiceClient.from_connection_string(self._credentials)
            container_client = blob_client.get_container_client(self.location)
            self._client: ContainerClient = container_client
        return self._client

    def blob_finder(self, folder: str) -> Generator[str, None, None]:
        for item in self.client.list_blob_names(name_starts_with=folder):
            yield item

    def get_raw_blob_stream(self, blob_name) -> BytesIO:
        blob_client = self.client.get_blob_client(blob_name)
        data = blob_client.download_blob().readall()
        return BytesIO(data)
