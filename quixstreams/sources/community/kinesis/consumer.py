import logging
import time
from typing import Callable, Generator, Literal, Optional, TypedDict

from quixstreams.sources import StatefulSource

try:
    import boto3
    from botocore.exceptions import ClientError
    from mypy_boto3_kinesis import KinesisClient
    from mypy_boto3_kinesis.type_defs import GetShardIteratorOutputTypeDef, ShardTypeDef
    from mypy_boto3_kinesis.type_defs import RecordTypeDef as KinesisRecord
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[kinesis]" to use KinesisSource'
    ) from exc


logger = logging.getLogger(__name__)

_OFFSET_RESET_DICT = {"earliest": "TRIM_HORIZON", "latest": "LATEST"}
AutoOffsetResetType = Literal["earliest", "latest"]


class KinesisStreamShardsNotFound(Exception):
    """Raised when the Kinesis Stream has no shards"""


class KinesisCheckpointer:
    def __init__(self, stateful_source: StatefulSource, commit_interval: float = 5.0):
        self._source = stateful_source
        self._last_committed_at = time.monotonic()
        self._commit_interval = commit_interval

    @property
    def last_committed_at(self) -> float:
        return self._last_committed_at

    def get(self, shard_id: str) -> Optional[str]:
        return self._source.state.get(shard_id)

    def set(self, shard_id: str, sequence_number: str):
        self._source.state.set(shard_id, sequence_number)

    def commit(self, force: bool = False):
        if (
            (now := time.monotonic()) - self._last_committed_at > self._commit_interval
        ) or force:
            self._source.flush()
            self._last_committed_at = now


class AWSCredentials(TypedDict):
    endpoint_url: Optional[str]
    region_name: Optional[str]
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]


class KinesisConsumer:
    """
    Consume all shards for a given Kinesis stream in a batched, round-robin fashion.
    Also handles checkpointing of said stream (requires a `KinesisCheckpointer`).
    """

    def __init__(
        self,
        stream_name: str,
        credentials: AWSCredentials,
        message_processor: Callable[[KinesisRecord], None],
        checkpointer: KinesisCheckpointer,
        auto_offset_reset: AutoOffsetResetType = "latest",
        max_records_per_shard: int = 1000,
        backoff_secs: float = 5.0,
    ):
        self._stream = stream_name
        self._credentials = credentials
        self._message_processor = message_processor
        self._checkpointer = checkpointer
        self._shard_iterators: dict[str, str] = {}
        self._shard_backoff: dict[str, float] = {}
        self._max_records_per_shard = max_records_per_shard
        self._backoff_secs = backoff_secs
        self._auto_offset_reset = _OFFSET_RESET_DICT[auto_offset_reset]
        self._client: Optional[KinesisClient] = None

    def process_shards(self):
        """
        Process records from the Stream shards one by one and checkpoint their
        sequence numbers.
        """
        # Iterate over shards one by one
        for shard_id in self._shard_iterators:
            # Poll records from each shard
            for record in self._poll_records(shard_id=shard_id):
                # Process the record
                self._message_processor(record)
                # Save the sequence number of the processed record
                self._checkpointer.set(shard_id, record["SequenceNumber"])

    def commit(self, force: bool = False):
        """
        Commit the checkpoint and save the progress of the
        """
        self._checkpointer.commit(force=force)

    def __enter__(self) -> "KinesisConsumer":
        if not self._client:
            self._client = boto3.client("kinesis", **self._credentials)
            self._init_shards()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            self._client.close()

    def _list_shards(self) -> list[ShardTypeDef]:
        """List all shards in the stream."""
        shards: list[ShardTypeDef] = []
        response = self._client.list_shards(StreamName=self._stream)
        shards.extend(response["Shards"])
        while "NextToken" in response:  # handle pagination
            response = self._client.list_shards(NextToken=response["NextToken"])
            shards.extend(response["Shards"])
        return shards

    def _get_shard_iterator(self, shard_id: str) -> str:
        if sequence_number := self._checkpointer.get(shard_id):
            kwargs = {
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": sequence_number,
            }
        else:
            kwargs = {
                "ShardIteratorType": self._auto_offset_reset,
            }
        response: GetShardIteratorOutputTypeDef = self._client.get_shard_iterator(
            StreamName=self._stream, ShardId=shard_id, **kwargs
        )
        return response["ShardIterator"]

    def _init_shards(self):
        if not (shards := [shard["ShardId"] for shard in self._list_shards()]):
            raise KinesisStreamShardsNotFound(
                f'Shards not found for stream "{self._stream}"'
            )
        self._shard_iterators = {
            shard: self._get_shard_iterator(shard) for shard in shards
        }

    def _poll_records(self, shard_id: str) -> Generator[KinesisRecord, None, None]:
        """
        Poll records from the Kinesis Stream shard.

        If the shared is backed off, no records will be returned.

        :param shard_id: shard id.
        """
        if (
            backoff_time := self._shard_backoff.get(shard_id, 0.0)
        ) and backoff_time > time.monotonic():
            # The shard is backed off, exit early
            return

        try:
            response = self._client.get_records(
                ShardIterator=self._shard_iterators[shard_id],
                Limit=self._max_records_per_shard,
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.error(f"Error reading from shard {shard_id}: {error_code}")
            if error_code == "ProvisionedThroughputExceededException":
                # The shard is backed off by Kinesis, update the backoff deadline
                self._shard_backoff[shard_id] = time.monotonic() + self._backoff_secs
            elif error_code == "ExpiredIteratorException":
                logger.error(f"Shard iterator expired for shard {shard_id}.")
                raise
            else:
                logger.error(f"Unrecoverable error: {e}")
                raise
        else:
            # Yield records for the shard
            for record in response.get("Records", []):
                yield record

            # Update the shard iterator for the next batch
            self._shard_iterators[shard_id] = response["NextShardIterator"]
            self._shard_backoff[shard_id] = 0
