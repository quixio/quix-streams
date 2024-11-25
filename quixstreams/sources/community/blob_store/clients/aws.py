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
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        """
        Configure IcebergSink to work with AWS Glue.

        :param aws_s3_bucket: The S3 URI where the table data will be stored
            (e.g., 's3://your-bucket/warehouse/').
        :param region_name: The AWS region for the S3 bucket
            NOTE: can alternatively set AWS_REGION environment variable
        :param aws_access_key_id: the AWS access key ID.
            NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
        :param aws_secret_access_key: the AWS secret access key.
            NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
        :param aws_session_token: a session token (or will be generated for you).
            NOTE: can alternatively set the AWS_SESSION_TOKEN environment variable.
        """
        self.location = aws_s3_bucket
        self.client: S3Client = boto_client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def get_raw_blob(self, blob_path: str) -> bytes:
        return self.client.get_object(Bucket=self.location, Key=blob_path)[
            "Body"
        ].read()

    def blob_finder(self, folder: str) -> Generator[str, None, None]:
        # TODO: Recursively navigate through folders.
        resp = self.client.list_objects(
            Bucket=self.location, Prefix=folder, Delimiter="/"
        )
        for item in resp["Contents"]:
            yield item["Key"]
