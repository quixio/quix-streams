import logging
import os
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
    from azure.storage.blob import ContainerClient
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[azure-file]" to use AzureFileOrigin'
    ) from exc

__all__ = (
    "AzureFileSource",
    "MissingAzureContainer",
)


logger = logging.getLogger(__name__)


class MissingAzureContainer(Exception): ...


class AzureFileSource(FileSource):
    """
    A source for extracting records stored within files in an Azure Filestore container.

    It recursively iterates from the provided path (file or folder) and
    processes all found files by parsing and producing the given records contained
    in each file as individual messages to a kafka topic (same topic for all).
    """

    def __init__(
        self,
        connection_string: str,
        container: str,
        filepath: Union[str, Path],
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        has_partition_folders: bool = False,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param connection_string: Azure client authentication string.
        :param container: Azure container name.
        :param filepath: folder to recursively iterate from (a file will be used directly).
        :param key_setter: sets the kafka message key for a record in the file.
        :param value_setter: sets the kafka message value for a record in the file.
        :param timestamp_setter: sets the kafka message timestamp for a record in the file.
        :param file_format: what format the files are stored as (ex: "json").
        :param compression: what compression was used on the files, if any (ex. "gzip").
        :param has_partition_folders: whether files are nested within partition folders.
            If True, FileSource will match the output topic partition count with it.
            Set this flag to True if Quix Streams FileSink was used to dump data.
            Note: messages will only align with these partitions if original key is used.
            Example structure - a 2 partition topic (0, 1):
            [/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
        :param replay_speed: Produce messages with this speed multiplier, which
            roughly reflects the time "delay" between the original message producing.
            Use any float >= 0, where 0 is no delay, and 1 is the original speed.
            NOTE: Time delay will only be accurate per partition, NOT overall.
        :param name: The name of the Source application (Default: last folder name).
        :param shutdown_timeout: Time in seconds the application waits for the source
            to gracefully shut down.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.

        """
        super().__init__(
            filepath=filepath,
            key_setter=key_setter,
            value_setter=value_setter,
            timestamp_setter=timestamp_setter,
            file_format=file_format,
            compression=compression,
            has_partition_folders=has_partition_folders,
            replay_speed=replay_speed,
            name=name or f"azurefilestore_{container}_{Path(filepath).name}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._container = container
        self._auth = connection_string
        self._client: Optional[ContainerClient] = None

    def setup(self):
        if self._client is None:
            self._client = ContainerClient.from_connection_string(
                conn_str=self._auth,
                container_name=self._container,
            )
            # Test the connection, plus raise if container is missing
            if not self._client.exists(
                timeout=10, connection_timeout=10, retry_connect=1
            ):
                raise MissingAzureContainer(
                    f'Azure container "{self._container}" does not exist.'
                )

    def get_file_list(self, filepath: Path) -> Iterable[Path]:
        data = self._client.list_blob_names(name_starts_with=str(filepath))
        for page in data.by_page():
            for item in page:
                yield Path(item)

    def read_file(self, filepath: Path) -> BytesIO:
        blob_client = self._client.get_blob_client(str(filepath))
        data = blob_client.download_blob().readall()
        return BytesIO(data)

    def file_partition_counter(self) -> int:
        """
        This is a simplified version of the recommended way to retrieve folder
        names based on the azure SDK docs examples.
        """
        self._init_client()  # allows connection callbacks to trigger off this
        path = f"{self._filepath}/"
        folders = set()
        for blob in self._client.list_blobs(name_starts_with=path):
            relative_dir = os.path.dirname(os.path.relpath(blob.name, path))
            if relative_dir and ("/" not in relative_dir):
                folders.add(relative_dir)
        return len(folders)

    def stop(self):
        self._close_client()
        super().stop()

    def _close_client(self):
        logger.debug("Closing Azure client session...")
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.error(f"Azure client session exited non-gracefully: {e}")
            self._client = None
