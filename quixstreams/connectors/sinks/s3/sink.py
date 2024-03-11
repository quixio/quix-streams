import logging
from io import BytesIO
from pathlib import PurePath
from typing import Union, Any, Literal, Optional, Dict

import boto3
from botocore.config import Config

from quixstreams.connectors.sinks.base.sinks import BaseBatchingSink
from .exceptions import InvalidS3FormatterError
from .formats import S3SinkBatchFormat, JSONFormat, BytesFormat
from ..base.batching import Batch

logger = logging.getLogger("quixstreams")

# TODO: Figure out the best way to create Quix vs non-Quix app
# TODO: Skipping for now:
#  - What to do with keys and timestamps?
# TODO: Timestamp extraction - will be needed for Influx sink 100%.


S3FormatSpec = Literal["bytes", "json"]
_S3_SINK_FORMATTERS: Dict[S3FormatSpec, S3SinkBatchFormat] = {
    "json": JSONFormat(),
    "bytes": BytesFormat(),
}

LogLevel = Literal[
    "CRITICAL",
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
    "NOTSET",
]


class S3Sink(BaseBatchingSink):
    def __init__(
        self,
        s3_bucket_name: str,
        s3_access_key_id: str,
        s3_secret_access_key: str,
        format: Union[S3FormatSpec, S3SinkBatchFormat],
        batch_max_messages: int,
        batch_max_interval_seconds: float = 0.0,
        consumer_group: str = "quixstreams-s3-sink-default",
        prefix: str = "",
        s3_region_name: Optional[str] = None,
        s3_retry_max_attempts: int = 3,
        s3_connect_timeout: Union[int, float] = 60,
        s3_read_timeout: Union[int, float] = 60,
        s3_loglevel: LogLevel = "INFO",
        *args,
        **kwargs,
    ):
        """
        A sink connector to send data from the Kafka topic to the specified S3 bucket.

        Example usage:

        ```
        sink = S3Sink(
            broker_address="localhost:9092",
            format="bytes",
            topic="topic-name",
            s3_bucket_name="bucket-name",
            s3_access_key_id="<access-key-id>",
            s3_secret_access_key="<access-key-secret>",
            batch_max_messages=100000,
        )
        sink.run()
        ```


        How it works:

        - It reads messages from one or more Kafka topics
        - Each message is added to the batch according to its topic-partition.
        - When the batch is ready, it uploads the batch to the S3 bucket and
          commits the last processed offset to Kafka.
        - On stopping, it closes the underlying Kafka consumer
          and drops the non-ready batches without uploading them.

        Batching:

        - By message count: the batch is ready when its number of messages
          equals to `batch_max_messages`.
          This strategy provides exactly-once delivery guarantees,
          meaning that the data will not be duplicated in the bucket
          in case of sink restart or failure.
          To use this strategy, simply pass the `batch_max_messages`.

        - By message count & time interval: the batch is ready when its number of
          messages equals to `batch_max_messages` OR when `batch_max_interval_seconds`
          interval elapses.
          This strategy guarantees at-least-once delivery because it uses the system
          clock for intervals.
          If the sink stops abruptly after the batch is uploaded but before
          the offset is commited to Kafka, on restart it may form the batch
          of different size, and the same message may end up in different files in S3.

        - Currently, batches can partition data only by Kafka topic-partition.
        - The batch file path format is `<prefix>/<topic>/partition=<partition>/<topic>+<partition>+<batch-start-offset><format-file-extension>`


        Supported formats:

        - JSON: the sink will deserialize message values from JSON and form batches in
          JSON Lines format with one JSON object per line.
          This format can only be used if the messages in the topic are already
          serialized with JSON. Otherwise, it will fail with deserialization error.
          To use JSON format with default parameters, pass `format="json"`.
          To customize the format settings like file extension
          or compression, pass `format=JSONFormat(...)`.


        - Bytes: the sink will take the raw message bytes and add them to the file
          using a separator (newline by default).
          It is agnostic of the source topic format.
          To use Bytes format with default parameters, pass `format="bytes"`.
          To customize the format settings like file extension, compression
          or separator, pass `format=BytesFormat(...)`.

        Note: there is a risk that messages can be read back incorrectly if they
        contain separators (newline by default) themselves.
        You may change the separators by passing
        `format=BytesFormat(separator=b"<your-separator>")`.


        Retries:

        - Retries handled automatically by the underlying `boto3` library.
        - You may adjust the retrying by passing `s3_retry_max_attempts`,
          `s3_connect_timeout`, and `s3_read_timeout` parameters.


        :param broker_address: Kafka broker address.
        :param topic: a topic or a list of topics to read from.
            Can be a `str` or `List[str]`.
        :param consumer_group: Kafka consumer group.
            Default - `"quixstreams-s3-sink-default"`.
        :param auto_offset_reset: Consumer `auto.offset.reset` setting.
            Default - `"latest"`.
        :param consumer_extra_config: a dict with additional params
            for the underlying `Consumer`.

        :param s3_bucket_name: S3 bucket name
        :param s3_access_key_id: S3 access key ID
        :param s3_secret_access_key: S3 secret access key
        :param format: a format for batching data.
            Allowed values: `"json", "bytes"`,
            or an implementation of the `S3SinkBatchFormat` class.
        :param prefix: a path prefix in the bucket
        :param batch_max_messages: max number of messages in the batch, required.
        :param batch_max_interval_seconds: max number of seconds before the batch
            is flushed to the S3 bucket.
            Setting this parameter will result in at-least-once delivery guarantees.

        :param s3_region_name: S3 region name, optional.
        :param s3_retry_max_attempts: a number of retry attempts for failed requests.
            Default - `3`.
        :param s3_connect_timeout: connect timeout for S3 requests. Default - `60`.
        :param s3_read_timeout: read timeout for S3 requests. Default - `60`.


        """
        super().__init__(
            *args,
            **kwargs,
            consumer_group=consumer_group,
            batch_max_messages=batch_max_messages,
            batch_max_interval_seconds=batch_max_interval_seconds,
        )
        self._prefix = prefix
        self._format = self._resolve_format(formatter_spec=format)
        boto3.set_stream_logger(level=logging.getLevelName(s3_loglevel))
        self._s3_bucket_name = s3_bucket_name
        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
            region_name=s3_region_name,
            config=Config(
                read_timeout=s3_read_timeout,
                connect_timeout=s3_connect_timeout,
                retries={"max_attempts": s3_retry_max_attempts, "mode": "standard"},
            ),
        )

    def deserialize_value(self, value: bytes) -> Any:
        """
        Deserialize Kafka message value from bytes into a Python object
        according to the specified format.

        You may override this method to customize the deserialized Python object

        :param value: message value in bytes
        :return: a Python object
        """
        return self._format.deserialize_value(value=value)

    def flush(self, batch: Batch):
        """
        Serialize and upload the ready-to-send batch to the destination S3 bucket.

        The retries are handled internally by "boto3" library according to the
        "s3_retry_max_attempts" value.

        :param batch: the `Batch` object to be flushed
        """
        # Build a file path for the bucket
        path = self.build_bucket_file_path(batch=batch)

        # Serialize batch items to bytes
        batch_value = self._format.serialize_batch_values(values=batch.items)

        # Upload batch to S3
        self.upload(path=path, content=batch_value, total_records=len(batch.items))

    def build_bucket_file_path(self, batch: Batch) -> str:
        """
        Build a file path for the batch in the S3 bucket

        :param batch: the `Batch` object to be flushed
        :return: file path in the bucket as string

        """
        topic, partition = batch.topic, str(batch.partition)
        dir_path = PurePath(self._prefix) / topic / f"partition={partition}"
        filename = (
            f"{topic}+{partition}+{batch.start_offset}{self._format.file_extension}"
        )
        path = dir_path / filename
        return path.as_posix()

    def upload(self, path: str, content: bytes, total_records: int):
        """
        Upload serialized batch to S3 bucket

        :param path: file path in bucket
        :param content: bytes to upload
        :param total_records: total number of records in the batch
        """
        logger.info(
            f"Uploading batch to an S3 bucket "
            f's3_bucket_name="{self._s3_bucket_name}" '
            f'path="{path}" total_records={total_records} '
            f"total_bytes={len(content)}"
        )
        self._s3_client.upload_fileobj(BytesIO(content), self._s3_bucket_name, path)
        logger.info(
            f"Batch upload complete "
            f's3_bucket_name="{self._s3_bucket_name}" '
            f'path="{path}" total_records={total_records} '
            f"total_bytes={len(content)}"
        )

    def _resolve_format(
        self,
        formatter_spec: Union[Literal["bytes"], Literal["json"], S3SinkBatchFormat],
    ) -> S3SinkBatchFormat:
        if isinstance(formatter_spec, S3SinkBatchFormat):
            return formatter_spec

        formatter_obj = _S3_SINK_FORMATTERS.get(formatter_spec)
        if formatter_obj is None:
            raise InvalidS3FormatterError(
                f'Invalid format name "{formatter_obj}". '
                f'Allowed values: "json", "bytes", '
                f"or an instance of {S3SinkBatchFormat.__class__.__name__} "
            )
        return formatter_obj
