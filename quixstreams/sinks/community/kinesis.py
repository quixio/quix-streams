import json
from collections import defaultdict
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from os import getenv
from typing import Any, Callable, Optional

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[kinesis]" to use KinesisSink'
    ) from exc

from quixstreams.models.types import HeadersTuples
from quixstreams.sinks.base import BaseSink
from quixstreams.sinks.base.exceptions import SinkBackpressureError

__all__ = ("KinesisSink", "KinesisStreamNotFoundError")


class KinesisStreamNotFoundError(Exception):
    """Raised when the specified Kinesis stream does not exist."""


class KinesisSink(BaseSink):
    def __init__(
        self,
        stream_name: str,
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        region_name: Optional[str] = getenv("AWS_REGION", getenv("AWS_DEFAULT_REGION")),
        value_serializer: Callable[[Any], str] = json.dumps,
        key_serializer: Callable[[Any], str] = bytes.decode,
        **kwargs,
    ) -> None:
        """
        Initialize the KinesisSink.

        :param stream_name: Kinesis stream name.
        :param aws_access_key_id: AWS access key ID.
        :param aws_secret_access_key: AWS secret access key.
        :param region_name: AWS region name (e.g., 'us-east-1').
        :param value_serializer: Function to serialize the value to string
            (defaults to json.dumps).
        :param key_serializer: Function to serialize the key to string
            (defaults to bytes.decode).
        :param kwargs: Additional keyword arguments passed to boto3.client.
        """
        self._stream_name = stream_name
        self._value_serializer = value_serializer
        self._key_serializer = key_serializer

        self._records = defaultdict(list)  # buffer for records before sending
        self._futures = defaultdict(list)  # buffer for requests in progress

        # Thread pool executor for asynchronous operations. Single thread ensures
        # that records are sent in order at the expense of throughput.
        self._executor = ThreadPoolExecutor(max_workers=1)

        self._kinesis = boto3.client(
            "kinesis",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            **kwargs,
        )

        # Check if the Kinesis stream exists
        try:
            self._kinesis.describe_stream(StreamName=self._stream_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise KinesisStreamNotFoundError(
                    f"Kinesis stream `{self._stream_name}` does not exist."
                )
            raise

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ) -> None:
        """
        Buffer a record for the Kinesis stream.

        Records are buffered until the batch size reaches 500, at which point
        they are sent immediately. If the batch size is less than 500, records
        will be sent when the flush method is called.
        """
        topic_partition = (topic, partition)
        record = {
            "Data": self._value_serializer(value),
            "PartitionKey": self._key_serializer(key),
        }
        self._records[topic_partition].append(record)

        # Kinesis accepts a maximum of 500 records per batch.
        # Submit immediately if 500 records are reached.
        if len(self._records[topic_partition]) == 500:
            records = self._records.pop(topic_partition)
            self._submit(topic_partition, records)

    def flush(self, topic: str, partition: int) -> None:
        """
        Flush all buffered records for a given topic-partition.

        This method sends any outstanding records that have not yet been sent
        because the batch size was less than 500. It waits for all futures to
        complete, ensuring that all records are successfully sent to the Kinesis
        stream.
        """
        topic_partition = (topic, partition)

        # Submit any remaining records
        if records := self._records.pop(topic_partition, None):
            self._submit(topic_partition, records)

        # Wait for all futures to complete
        if futures := self._futures.pop(topic_partition, None):
            done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
            if not_done or any(f.exception() for f in done):
                raise SinkBackpressureError(
                    retry_after=5.0,
                    topic=topic,
                    partition=partition,
                )

    def _submit(
        self, topic_partition: tuple[str, int], records: list[dict[str, str]]
    ) -> None:
        future = self._executor.submit(
            self._kinesis.put_records,
            Records=records,
            StreamName=self._stream_name,
        )
        self._futures[topic_partition].append(future)
