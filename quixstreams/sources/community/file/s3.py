import logging
from io import BytesIO
from os import getenv
from pathlib import Path
from typing import Callable, Iterable, Optional, Union

import botocore.exceptions

from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
)

from .base import FileSource
from .compressions import CompressionName
from .formats import Format, FormatName

try:
    from boto3 import client as boto_client
    from mypy_boto3_s3 import S3Client
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[s3]" to use S3Origin'
    ) from exc

logger = logging.getLogger(__name__)

__all__ = (
    "S3FileSource",
    "MissingS3Bucket",
)


class MissingS3Bucket(Exception): ...


class S3FileSource(FileSource):
    """
    A source for extracting records stored within files in an S3 bucket location.

    It recursively iterates from the provided path (file or folder) and
    processes all found files by parsing and producing the given records contained
    in each file as individual messages to a kafka topic (same topic for all).
    """

    def __init__(
        self,
        filepath: Union[str, Path],
        bucket: str,
        region_name: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        has_partition_folders: bool = False,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param filepath: folder to recursively iterate from (a file will be used directly).
        :param bucket: The S3 bucket name only (ex: 'your-bucket').
        :param region_name: The AWS region.
            NOTE: can alternatively set the AWS_REGION environment variable
        :param aws_access_key_id: the AWS access key ID.
            NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
        :param aws_secret_access_key: the AWS secret access key.
            NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
        :param endpoint_url: the endpoint URL to use; only required for connecting
        to a locally hosted S3.
            NOTE: can alternatively set the AWS_ENDPOINT_URL_S3 environment variable
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
            name=name or f"s3_{bucket}_{Path(filepath).name}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._bucket = bucket
        self._credentials = {
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "endpoint_url": endpoint_url,
        }
        self._client: Optional[S3Client] = None

    def setup(self):
        if self._client is None:
            self._client = boto_client("s3", **self._credentials)
            try:
                self._client.head_bucket(Bucket=self._bucket)
            except botocore.exceptions.ClientError as e:
                if "404" in str(e):
                    raise MissingS3Bucket(f'S3 bucket "{self._bucket}" does not exist.')
                raise

    def get_file_list(self, filepath: Union[str, Path]) -> Iterable[Path]:
        resp = self._client.list_objects(
            Bucket=self._bucket,
            Prefix=str(filepath),
            Delimiter="/",
        )
        for folder in resp.get("CommonPrefixes", []):
            yield from self.get_file_list(folder["Prefix"])

        for file in resp.get("Contents", []):
            yield Path(file["Key"])

    def read_file(self, filepath: Path) -> BytesIO:
        data = self._client.get_object(Bucket=self._bucket, Key=str(filepath))[
            "Body"
        ].read()
        return BytesIO(data)

    def file_partition_counter(self) -> int:
        self._init_client()  # allows connection callbacks to trigger off this
        resp = self._client.list_objects(
            Bucket=self._bucket, Prefix=f"{self._filepath}/", Delimiter="/"
        )
        self._close_client()  # close due to multiprocessing pickling limitations
        return len(resp["CommonPrefixes"])

    def stop(self):
        self._close_client()
        super().stop()

    def _close_client(self):
        logger.debug("Closing S3 client session...")
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.error(f"S3 client session exited non-gracefully: {e}")
            self._client = None
