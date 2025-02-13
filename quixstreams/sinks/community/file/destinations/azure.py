import logging
from typing import Optional

from quixstreams.sinks import SinkBatch
from quixstreams.sinks.community.file.destinations.base import Destination

try:
    from azure.core.exceptions import HttpResponseError
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob._container_client import ContainerClient
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[azure-file]" to use AzureFileDestination'
    ) from exc


__all__ = ("AzureFileDestination",)


logger = logging.getLogger(__name__)


class AzureContainerNotFoundError(Exception):
    """Raised when the specified Azure File container does not exist."""


class AzureContainerAccessDeniedError(Exception):
    """Raised when the specified Azure File container access is denied."""


class AzureFileDestination(Destination):
    """
    A destination that writes data to Microsoft Azure File.

    Handles writing data to Azure containers using the Azure Blob SDK. Credentials can
    be provided directly or via environment variables.
    """

    def __init__(
        self,
        connection_string: str,
        container: str,
    ) -> None:
        """
        Initialize the Azure File destination.

        :param connection_string: Azure client authentication string.
        :param container: Azure container name.

        :raises AzureContainerNotFoundError: If the specified container doesn't exist.
        :raises AzureContainerAccessDeniedError: If access to the container is denied.
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

    def _validate_container(self) -> None:
        """
        Validate that the container exists and is accessible.

        :raises AzureContainerNotFoundError: If the specified container doesn't exist.
        :raises AzureContainerAccessDeniedError: If access to the container is denied.
        """
        try:
            if self._client.exists(timeout=10):
                return
        except HttpResponseError as e:
            if e.status_code == 403:
                raise AzureContainerAccessDeniedError(
                    f"Container access denied: {self._container}"
                )
        except Exception as e:
            logger.error("An unexpected Azure client error occurred", exc_info=e)
            raise
        raise AzureContainerNotFoundError(f"Container not found: {self._container}")

    def setup(self):
        if not self._client:
            self._client = self._get_client()
            self._validate_container()

    def write(self, data: bytes, batch: SinkBatch) -> None:
        """
        Write data to Azure.

        :param data: The serialized data to write.
        :param batch: The batch information containing topic and partition details.
        """
        file_name = str(self._path(batch))
        logger.debug(
            "Writing %d bytes to Azure container=%s, path=%s",
            len(data),
            self._container,
            file_name,
        )
        self._client.get_blob_client(file_name).upload_blob(data)
