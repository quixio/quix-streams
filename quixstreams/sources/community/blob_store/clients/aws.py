from io import BytesIO
from os import getenv
from typing import Generator, Optional

from .base import BlobClient

try:
    from boto3 import client as boto_client
    from mypy_boto3_s3 import S3Client
except Exception:
    raise


__all__ = ("AwsS3BlobClient",)


class AwsS3BlobClient(BlobClient):
    def __init__(
        self,
        aws_s3_bucket: str,
        aws_region: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        aws_endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
        aws_session_token: Optional[str] = None,
    ):
        """
        Configure IcebergSink to work with AWS Glue.

        :param aws_s3_bucket: The S3 URI where the table data will be stored
            (e.g., 's3://your-bucket/warehouse/').
        :param aws_region: The AWS region.
            NOTE: can alternatively set the AWS_REGION environment variable
        :param aws_access_key_id: the AWS access key ID.
            NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
        :param aws_secret_access_key: the AWS secret access key.
            NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
        :param aws_endpoint_url: the endpoint URL to use; only required for connecting
        to a locally hosted Kinesis.
            NOTE: can alternatively set the AWS_ENDPOINT_URL_KINESIS environment variable
        """
        self.location = aws_s3_bucket
        self._client: Optional[S3Client] = None
        self._credentials = {
            "region_name": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
            "endpoint_url": aws_endpoint_url,
        }

    @property
    def client(self):
        if not self._client:
            self._client: S3Client = boto_client("s3", **self._credentials)
        return self._client

    def get_raw_blob_stream(self, blob_path: str) -> BytesIO:
        data = self.client.get_object(Bucket=self.location, Key=blob_path)[
            "Body"
        ].read()
        return BytesIO(data)

    def blob_finder(self, folder: str) -> Generator[str, None, None]:
        # TODO: Recursively navigate through folders.
        resp = self.client.list_objects(
            Bucket=self.location, Prefix=folder, Delimiter="/"
        )
        for item in resp["Contents"]:
            yield item["Key"]
