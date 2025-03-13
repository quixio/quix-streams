import logging
from io import BytesIO
from os import getenv
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
    from boto3 import client as boto_client
    from mypy_boto3_s3 import S3Client
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[s3]" to use S3Origin'
    ) from exc

logger = logging.getLogger(__name__)

__all__ = ("S3FileSource",)


class S3FileSource(FileSource):
    def __init__(
        self,
        directory: Union[str, Path],
        bucket: str,
        region_name: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        super().__init__(
            directory=directory,
            key_setter=key_setter,
            value_setter=value_setter,
            timestamp_setter=timestamp_setter,
            file_format=file_format,
            compression=compression,
            replay_speed=replay_speed,
            name=name,
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
        self._client = boto_client("s3", **self._credentials)

    def get_file_list(self, filepath: Union[str, Path]) -> Iterable[Path]:
        resp = self._client.list_objects(
            Bucket=self._bucket,
            Prefix=str(filepath),
            Delimiter="/",
        )
        for _folder in resp.get("CommonPrefixes", []):
            yield from self.get_file_list(_folder["Prefix"])

        for file in resp.get("Contents", []):
            yield Path(file["Key"])

    def read_file(self, filepath: Path) -> BytesIO:
        data = self._client.get_object(Bucket=self._bucket, Key=str(filepath))[
            "Body"
        ].read()
        return BytesIO(data)
