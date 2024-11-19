import time
from typing import Literal, Optional

from quixstreams.models.topics import Topic
from quixstreams.sources.base import StatefulSource
from quixstreams.state import State

from .consumer import (
    Authentication,
    KinesisCheckpointer,
    KinesisConsumer,
    KinesisRecord,
)

__all__ = ("KinesisSource",)


class SourceCheckpointer(KinesisCheckpointer):
    def __init__(self, stateful_source: StatefulSource, commit_interval: float = 5.0):
        self._source = stateful_source
        self._last_committed_at = time.monotonic()
        self._commit_interval = commit_interval
        self._state: Optional[State] = None

    @property
    def last_committed_at(self) -> float:
        return self._last_committed_at

    def begin(self):
        if not self._state:
            self._state = self._source.state()

    def get(self, key: str) -> Optional[str]:
        return self._state.get(key)

    def set(self, key: str, value: str):
        self._state.set(key, value)

    def commit(self, force: bool = False):
        if (
            (now := time.monotonic()) - self._last_committed_at > self._commit_interval
        ) or force:
            self._source.flush()
            self._last_committed_at = now
            self._state = None


class KinesisSource(StatefulSource):
    def __init__(
        self,
        name: str,
        stream_name: str,
        auth: Authentication,
        shutdown_timeout: float = 10,
        auto_offset_reset: Literal["earliest", "latest"] = "latest",
        max_records_per_shard: int = 10,
        commit_interval: float = 5.0,
        retry_backoff_secs: float = 5.0,
    ):
        self._stream_name = stream_name
        self._auth = auth
        self._auto_offset_reset = auto_offset_reset
        self._max_records_per_shard = max_records_per_shard
        self._retry_backoff_secs = retry_backoff_secs
        self._checkpointer = SourceCheckpointer(self, commit_interval)
        super().__init__(
            name=f"{name}_{self._stream_name}", shutdown_timeout=shutdown_timeout
        )

    def default_topic(self) -> Topic:
        return Topic(
            name=f"kinesis_{self.name}",
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

    def run(self):
        with KinesisConsumer(
            stream_name=self._stream_name,
            auth=self._auth,
            message_processor=self._handle_kinesis_message,
            auto_offset_reset=self._auto_offset_reset,
            checkpointer=self._checkpointer,
            max_records_per_shard=self._max_records_per_shard,
            backoff_secs=self._retry_backoff_secs,
        ) as consumer:
            while self._running:
                consumer.poll_and_process_shards()
                consumer.commit()
