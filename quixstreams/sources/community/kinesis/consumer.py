import logging
import time
from typing import Callable, Literal, Optional, Protocol, Type

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_kinesis import KinesisClient
from mypy_boto3_kinesis.type_defs import GetShardIteratorOutputTypeDef, ShardTypeDef
from mypy_boto3_kinesis.type_defs import RecordTypeDef as KinesisRecord
from typing_extensions import Self

logger = logging.getLogger(__name__)

_OFFSET_RESET_DICT = {"earliest": "TRIM_HORIZON", "latest": "LATEST"}


class KinesisCheckpointer(Protocol):
    @property
    def last_committed_at(self) -> float: ...

    def begin(self): ...

    def get(self, key: str) -> Optional[str]: ...

    def set(self, key: str, value: str): ...

    def commit(self): ...


class State(KinesisCheckpointer):
    def __init__(self, starting_positions: Optional[dict] = None):
        self._state = starting_positions or {}
        self._cache = {}

    def get(self, key) -> Optional[str]:
        """
        Retrieve the value of a key. First checks the cache,
        then falls back to the state if the key isn't in the cache.
        """
        if key in self._cache:
            return self._cache[key]
        return self._state.get(key)

    def set(self, key, value):
        """
        Sets a value in the cache.
        """
        self._cache[key] = value

    def commit(self):
        """
        Writes all items from the cache into the state and clears the cache.
        """
        self._state.update(self._cache)
        self._cache.clear()


class Authentication:
    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        aws_region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        """
        :param aws_region: The AWS region for the S3 bucket and Glue catalog.
        :param aws_access_key_id: the AWS access key ID.
            NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
            when using AWS Glue.
        :param aws_secret_access_key: the AWS secret access key.
            NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
            when using AWS Glue.
        """
        self.auth = {
            "endpoint_url": endpoint_url,
            "region_name": aws_region,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }


class KinesisConsumer:
    def __init__(
        self,
        stream_name: str,
        auth: Authentication,
        message_processor: Callable,
        auto_offset_reset: Literal["earliest", "latest"] = "latest",
        checkpointer: Optional[Type[KinesisCheckpointer]] = None,
        max_records_per_shard: int = 10,
        backoff_secs: float = 5.0,
    ):
        self._stream = stream_name
        self._auth = auth
        self._message_processor = message_processor
        self._checkpointer: KinesisCheckpointer = checkpointer
        self._shard_iterators: dict[str, str] = {}
        self._shard_backoff: dict[str, float] = {}
        self._max_records_per_shard = max_records_per_shard
        self._backoff_secs = backoff_secs
        self._auto_offset_reset = _OFFSET_RESET_DICT[auto_offset_reset]
        self._client: Optional[KinesisClient] = None
        self._total_processed: int = 0
        self._processed: dict[str, int] = {}

    def _init_client(self):
        self._client = boto3.client("kinesis", **self._auth.auth)

    def _process_record(self, shard_id: str, record: KinesisRecord):
        logger.debug(
            f"ShardId: {shard_id}, Partition Key: {record['PartitionKey']}, Data: {record['Data']}"
        )
        self._message_processor(record)
        self._checkpointer.set(shard_id, record["SequenceNumber"])
        # TODO: remove below once testing done
        self._processed[shard_id] = self._processed.setdefault(shard_id, 0) + 1
        self._total_processed += 1
        logger.debug(f"TOTAL PROCESSED: {self._total_processed}")

    def list_shards(self) -> list[ShardTypeDef]:
        """List all shards in the stream."""
        shards: list[ShardTypeDef] = []
        response = self._client.list_shards(StreamName=self._stream)
        shards.extend(response["Shards"])
        while "NextToken" in response:  # handle pagination
            response = self._client.list_shards(NextToken=response["NextToken"])
            shards.extend(response["Shards"])
        return shards

    def _get_shard_iterator(self, shard_id: str):
        if sequence_number := self._checkpointer.get(shard_id):
            additional_kwargs = {
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": sequence_number,
            }
        else:
            additional_kwargs = {
                "ShardIteratorType": self._auto_offset_reset,
            }
        response: GetShardIteratorOutputTypeDef = self._client.get_shard_iterator(
            StreamName=self._stream, ShardId=shard_id, **additional_kwargs
        )
        return response["ShardIterator"]

    def _init_shards(self):
        if not (shards := [shard["ShardId"] for shard in self.list_shards()]):
            raise ValueError(f"No shards for stream {self._stream}")
        self._shard_iterators = {
            shard: self._get_shard_iterator(shard) for shard in shards
        }

    def commit(self):
        self._checkpointer.commit()

    def _poll_and_process_shard(self, shard_id):
        """
        Read a limited number of records from a shard.

        Args:
            shard_id (str): The ID of the shard.
        Returns:
            str: Updated shard iterator.
        """
        if (
            backoff_time := self._shard_backoff.get(shard_id)
        ) and time.monotonic() < backoff_time:
            return
        try:
            response = self._client.get_records(
                ShardIterator=self._shard_iterators[shard_id],
                Limit=self._max_records_per_shard,
            )

            for record in response.get("Records", []):
                self._process_record(shard_id, record)

            # Update the shard iterator for the next batch
            self._shard_iterators[shard_id] = response["NextShardIterator"]
            self._shard_backoff[shard_id] = 0

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            logger.debug(f"Error reading from shard {shard_id}: {error_code}")
            if error_code == "ProvisionedThroughputExceededException":
                self._shard_backoff[shard_id] = time.monotonic() + self._backoff_secs
            elif error_code == "ExpiredIteratorException":
                logger.debug(f"Shard iterator expired for shard {shard_id}.")
                raise
            else:
                logger.debug(f"Unrecoverable error: {e}")
                raise

    def poll_and_process_shards(self):
        self._checkpointer.begin()
        for shard in self._shard_iterators:
            self._poll_and_process_shard(shard)

    def start(self):
        self._init_client()
        self._checkpointer.begin()
        self._init_shards()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self):
        try:
            self._init_client()
            self._init_shards()
            while True:
                self.poll_and_process_shards()
                self._checkpointer.commit()
        except Exception as e:
            logger.debug(f"KinesisConsumer encountered an error: {e}")
        finally:
            logger.debug("Stopping KinesisConsumer...")
