from os import getenv
from typing import Optional

from quixstreams.models.topics import Topic
from quixstreams.sources.base import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    StatefulSource,
)

from .consumer import (
    AutoOffsetResetType,
    AWSCredentials,
    KinesisCheckpointer,
    KinesisConsumer,
    KinesisRecord,
)

__all__ = ("KinesisSource",)


class KinesisSource(StatefulSource):
    """
    NOTE: Requires `pip install quixstreams[kinesis]` to work.

    This source reads data from an Amazon Kinesis stream, dumping it to a
    kafka topic using desired `StreamingDataFrame`-based transformations.

    Provides "at-least-once" guarantees.

    The incoming message value will be in bytes, so transform in your SDF accordingly.

    Example Usage:

    ```python
    from quixstreams import Application
    from quixstreams.sources.community.kinesis import KinesisSource


    kinesis = KinesisSource(
        stream_name="<YOUR STREAM>",
        aws_access_key_id="<YOUR KEY ID>",
        aws_secret_access_key="<YOUR SECRET KEY>",
        aws_region="<YOUR REGION>",
        auto_offset_reset="earliest",  # start from the beginning of the stream (vs end)
    )

    app = Application(
        broker_address="<YOUR BROKER INFO>",
        consumer_group="<YOUR GROUP>",
    )

    sdf = app.dataframe(source=kinesis).print(metadata=True)
    # YOUR LOGIC HERE!

    if __name__ == "__main__":
        app.run()
    ```
    """

    def __init__(
        self,
        stream_name: str,
        aws_region: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        aws_endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_KINESIS"),
        shutdown_timeout: float = 10,
        auto_offset_reset: AutoOffsetResetType = "latest",
        max_records_per_shard: int = 1000,
        commit_interval: float = 5.0,
        retry_backoff_secs: float = 5.0,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param stream_name: name of the desired Kinesis stream to consume.
        :param aws_region: The AWS region.
            NOTE: can alternatively set the AWS_REGION environment variable
        :param aws_access_key_id: the AWS access key ID.
            NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
        :param aws_secret_access_key: the AWS secret access key.
            NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
        :param aws_endpoint_url: the endpoint URL to use; only required for connecting
        to a locally hosted Kinesis.
            NOTE: can alternatively set the AWS_ENDPOINT_URL_KINESIS environment variable
        :param shutdown_timeout:
        :param auto_offset_reset: When no previous offset has been recorded, whether to
            start from the beginning ("earliest") or end ("latest") of the stream.
        :param max_records_per_shard: During round-robin consumption, how many records
            to consume per shard (partition) per consume (NOT per-commit).
        :param commit_interval: the time between commits
        :param retry_backoff_secs: how long to back off from doing HTTP calls for a
             shard when Kinesis consumer encounters handled/expected errors.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            name=f"kinesis_{stream_name}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._stream_name = stream_name
        self._credentials: AWSCredentials = {
            "endpoint_url": aws_endpoint_url,
            "region_name": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }

        self._auto_offset_reset = auto_offset_reset
        self._max_records_per_shard = max_records_per_shard
        self._retry_backoff_secs = retry_backoff_secs
        self._checkpointer = KinesisCheckpointer(
            stateful_source=self, commit_interval=commit_interval
        )

    def default_topic(self) -> Topic:
        return Topic(
            name=self.name,
            key_deserializer="str",
            value_deserializer="bytes",
            key_serializer="str",
            value_serializer="bytes",
        )

    def _handle_kinesis_message(self, message: KinesisRecord):
        serialized_msg = self._producer_topic.serialize(
            key=message["PartitionKey"],
            value=message["Data"],
            timestamp_ms=int(message["ApproximateArrivalTimestamp"].timestamp() * 1000),
        )
        self.produce(
            key=serialized_msg.key,
            value=serialized_msg.value,
            timestamp=serialized_msg.timestamp,
        )

    def setup(self):
        self._client = KinesisConsumer(
            stream_name=self._stream_name,
            credentials=self._credentials,
            message_processor=self._handle_kinesis_message,
            auto_offset_reset=self._auto_offset_reset,
            checkpointer=self._checkpointer,
            max_records_per_shard=self._max_records_per_shard,
            backoff_secs=self._retry_backoff_secs,
        ).__enter__()

    def run(self):
        with self._client as consumer:
            while self._running:
                consumer.process_shards()
                consumer.commit()
