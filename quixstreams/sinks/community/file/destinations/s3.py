import logging
from os import getenv
from typing import Optional

import boto3
from mypy_boto3_s3 import S3Client

from quixstreams.sinks import SinkBatch
from quixstreams.sinks.community.file.destinations.base import Destination

logger = logging.getLogger(__name__)


class S3BucketNotFoundError(Exception):
    """Raised when the specified S3 bucket does not exist."""


class S3BucketAccessDeniedError(Exception):
    """Raised when the specified S3 bucket access is denied."""


class S3Destination(Destination):
    """A destination that writes data to Amazon S3.

    Handles writing data to S3 buckets using the AWS SDK. Credentials can be
    provided directly or via environment variables.
    """

    def __init__(
        self,
        bucket: str,
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        region_name: Optional[str] = getenv("AWS_REGION", getenv("AWS_DEFAULT_REGION")),
        endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
        **kwargs,
    ) -> None:
        """Initialize the S3 destination.

        :param bucket: Name of the S3 bucket to write to.
        :param aws_access_key_id: AWS access key ID. Defaults to AWS_ACCESS_KEY_ID
            environment variable.
        :param aws_secret_access_key: AWS secret access key. Defaults to
            AWS_SECRET_ACCESS_KEY environment variable.
        :param region_name: AWS region name. Defaults to AWS_REGION or
            AWS_DEFAULT_REGION environment variable.
        :param endpoint_url: the endpoint URL to use; only required for connecting
        to a locally hosted S3.
            NOTE: can alternatively set the AWS_ENDPOINT_URL_S3 environment variable
        :param kwargs: Additional keyword arguments passed to boto3.client.
        :raises S3BucketNotFoundError: If the specified bucket doesn't exist.
        :raises S3BucketAccessDeniedError: If access to the bucket is denied.
        """
        self._bucket = bucket
        self._credentials = {
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "endpoint_url": endpoint_url,
            **kwargs,
        }
        self._client: Optional[S3Client] = None

    def setup(self):
        if not self._client:
            # See init comment as to why we cannot set the client here.
            # We then attempt a likely-to-succeed query on the bucket to confirm auth.
            self._client = boto3.client("s3", **self._credentials)
            self._validate_bucket()

    def _validate_bucket(self) -> None:
        """Validate that the bucket exists and is accessible.

        :raises S3BucketNotFoundError: If the specified bucket doesn't exist.
        :raises S3BucketAccessDeniedError: If access to the bucket is denied.
        """
        bucket = self._bucket
        logger.debug("Validating access to bucket: %s", bucket)
        try:
            self._client.head_bucket(Bucket=bucket)
        except self._client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "403":
                raise S3BucketAccessDeniedError(f"S3 bucket access denied: {bucket}")
            elif error_code == "404":
                raise S3BucketNotFoundError(f"S3 bucket not found: {bucket}")
            raise

    def write(self, data: bytes, batch: SinkBatch) -> None:
        """Write data to S3.

        :param data: The serialized data to write.
        :param batch: The batch information containing topic and partition details.
        """
        key = str(self._path(batch))
        logger.debug(
            "Writing %d bytes to S3 bucket=%s, path=%s", len(data), self._bucket, key
        )
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data)
