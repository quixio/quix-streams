import logging
from io import BytesIO
from os import getenv
from pathlib import Path
from typing import Generator, Optional, Union

from .base import ExternalOrigin

try:
    from boto3 import client as boto_client
    from mypy_boto3_s3 import S3Client
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[s3]" to use S3Origin'
    ) from exc

logger = logging.getLogger(__name__)

__all__ = ("S3Origin",)


class S3Origin(ExternalOrigin):
    def __init__(
        self,
        aws_s3_bucket: str,
        aws_region: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        aws_endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
    ):
        """
        Configure IcebergSink to work with AWS Glue.

        :param aws_s3_bucket: The S3 bucket name only (ex: 'your-bucket').
        :param aws_region: The AWS region.
            NOTE: can alternatively set the AWS_REGION environment variable
        :param aws_access_key_id: the AWS access key ID.
            NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
        :param aws_secret_access_key: the AWS secret access key.
            NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
        :param aws_endpoint_url: the endpoint URL to use; only required for connecting
        to a locally hosted Kinesis.
            NOTE: can alternatively set the AWS_ENDPOINT_URL_S3 environment variable
        """
        self.root_location = aws_s3_bucket
        self._credentials = {
            "region_name": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "endpoint_url": aws_endpoint_url,
        }
        # S3 client runs into pickling errors with multiprocessing. We can't set it
        # until multiprocessing starts it.
        # We can work around it by setting it during file collection
        self._client: Optional[S3Client] = None

    def _get_client(self) -> S3Client:
        return boto_client("s3", **self._credentials)

    def get_raw_file_stream(self, filepath: Path) -> BytesIO:
        data = self._client.get_object(Bucket=self.root_location, Key=str(filepath))[
            "Body"
        ].read()
        return BytesIO(data)

    def get_folder_count(self, folder: Path) -> int:
        resp = self._get_client().list_objects(
            Bucket=self.root_location, Prefix=str(folder), Delimiter="/"
        )
        return len(resp["CommonPrefixes"])

    def file_collector(self, folder: Union[str, Path]) -> Generator[Path, None, None]:
        self._client = self._get_client()
        resp = self._client.list_objects(
            Bucket=self.root_location,
            Prefix=str(folder),
            Delimiter="/",
        )
        for folder in resp.get("CommonPrefixes", []):
            yield from self.file_collector(folder["Prefix"])

        for file in resp.get("Contents", []):
            yield Path(file["Key"])
