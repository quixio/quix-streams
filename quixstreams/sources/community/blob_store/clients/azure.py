from io import BytesIO
from typing import Generator

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
        blob_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_client.get_container_client(container)

        self.client: ContainerClient = container_client
        self.location = container

    def blob_finder(self, folder: str) -> Generator[str, None, None]:
        for item in self.client.list_blob_names(name_starts_with=folder):
            yield item

    def get_raw_blob(self, blob_name) -> BytesIO:
        blob_client = self.client.get_blob_client(blob_name)
        data: bytes = blob_client.download_blob().readall()
        return BytesIO(data)
