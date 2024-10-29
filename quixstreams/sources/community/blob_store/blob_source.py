import time
import logging
from abc import abstractmethod
from dataclasses import dataclass
from quixstreams.sources.base import Source
from typing import Optional, Any, Iterable
from quixstreams.models.topics import Topic
from . import BlobFormatReader


logger = logging.getLogger(__name__)


@dataclass
class BlobClient:
    client: Any
    location: str

    @abstractmethod
    def list_blobs(self, folder: str) -> Iterable: ...

    @abstractmethod
    def get_raw_blob(self, blob_name: str) -> Any: ...


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
        try:
            from boto3 import client as boto_client
        except Exception:
            raise

        self.location = aws_s3_bucket
        self.client = boto_client(
            "s3",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
        )

    def get_raw_blob(self, blob_path: str) -> Any:
        return self.client.get_object(Bucket=self.location, Key=blob_path)[
            "Body"
        ].read()

    def list_blobs(self, folder: str) -> Iterable:
        resp = self.client.list_objects(
            Bucket=self.location, Prefix=folder, Delimiter="/"
        )
        return [item["Key"] for item in resp["Contents"]]


class AzureBlobClient(BlobClient):
    def __init__(
        self,
        connection_string: str,
        container: str,
    ):
        try:
            from azure.storage.blob import BlobServiceClient
        except Exception:
            raise

        blob_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_client.get_container_client(container)

        self.client = container_client
        self.location = container

    def list_blobs(self, folder):
        return self.client.list_blobs(name_starts_with=folder)

    def get_raw_blob(self, blob_name):
        blob_client = self.client.get_blob_client(blob_name)
        return blob_client.download_blob().readall()


class BlobSource(Source):
    def __init__(
        self,
        client: BlobClient,
        format_reader: BlobFormatReader,
        shutdown_timeout: float = 10,
        as_replay: bool = True,
        name: Optional[str] = None,
        folder: Optional[str] = None,
        blob_name: Optional[str] = None,
    ) -> None:
        if bool(folder) ^ bool(blob_name):
            raise ValueError("Cannot provide both a blob and a folder.")

        self._client = client
        self._reader = format_reader
        self._folder = folder
        self._blob_name = blob_name
        self._as_replay = as_replay
        self._previous_timestamp = None
        super().__init__(name or client.location, shutdown_timeout)

    def _process_blob(self, blob_name: str):
        """
        Process an individual blob (file) by reading its content and producing messages.
        """
        try:
            blob_data = self._client.get_raw_blob(blob_name)
            blob_data = self._reader.deserialize_blob(blob_data)

            for item in blob_data:
                if self._as_replay:
                    self._replay_delay(item["_timestamp"])

                # Produce the message with the deserialized key and value
                self.produce(
                    key=item["_key"],
                    value=item["_value"],
                    timestamp=item["_timestamp"],
                    headers=item["_headers"],
                )

            # Flush the produced messages
            self.flush()
            logger.info(f"Processed blob: {blob_name}")

        except Exception as e:
            logger.error(f"Error processing blob '{blob_name}': {e}")

    def _process_folder(self):
        """
        Traverse through the blobs in the specified folder/prefix and process each blob.
        """
        try:
            blob_list = self._client.list_blobs(self._folder)
            for blob in blob_list:
                self._process_blob(blob.name)

        except Exception as e:
            logger.error(f"Error listing or processing blobs: {e}")

    def _replay_delay(self, current_timestamp: int):
        """
        Apply the replay speed by calculating the delay between messages
        based on their timestamps.

        The delay is scaled by the replay speed.
        """
        if self._previous_timestamp is not None:
            time_diff = (
                current_timestamp - self._previous_timestamp
            ) / 1000.0  # Convert ms to seconds
            if time_diff > 0:
                logger.debug(f"Sleeping for {time_diff} seconds")
                time.sleep(time_diff)  # Delay execution to simulate real-time

        self._previous_timestamp = current_timestamp

    def default_topic(self) -> Topic:
        """
        Defines the default topic to which messages will be sent.
        """
        return Topic(
            name=self.name,
            key_serializer="bytes",
            key_deserializer="bytes",
            value_deserializer="json",
            value_serializer="json",
        )

    def run(self):
        if self._folder:
            self._process_folder()
        else:
            self._process_blob(self._blob_name)
