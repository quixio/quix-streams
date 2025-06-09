import typing

from quixstreams.context import message_context
from quixstreams.models.topics.manager import TopicManager
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .base import Join

if typing.TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame


__all__ = ("AsOfJoin",)

DISCARDED = object()


class AsOfJoin(Join):
    def join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame":
        if left.stream_id == right.stream_id:
            raise ValueError(
                "Joining dataframes originating from "
                "the same topic is not yet supported.",
            )
        TopicManager.ensure_topics_copartitioned(*left.topics, *right.topics)

        # ProcessingContext is shared between all dataframes in the same application
        processing_context = left.processing_context

        changelog_config = TopicManager.derive_topic_config(right.topics)
        processing_context.state_manager.register_timestamped_store(
            stream_id=right.stream_id,
            store_name=self._store_name,
            grace_ms=self._grace_ms,
            keep_duplicates=False,
            changelog_config=changelog_config,
        )

        is_inner_join = self._how == "inner"
        merger = self._merger

        def left_func(value, key, timestamp, headers):
            tx: TimestampedPartitionTransaction = (
                processing_context.checkpoint.get_store_transaction(
                    stream_id=right.stream_id,
                    partition=message_context().partition,
                    store_name=self._store_name,
                )
            )

            right_value = tx.get_latest(timestamp=timestamp, prefix=key)
            if is_inner_join and not right_value:
                return DISCARDED
            return merger(value, right_value)

        def right_func(value, key, timestamp, headers):
            tx: TimestampedPartitionTransaction = (
                processing_context.checkpoint.get_store_transaction(
                    stream_id=right.stream_id,
                    partition=message_context().partition,
                    store_name=self._store_name,
                )
            )
            tx.set_for_timestamp(timestamp=timestamp, value=value, prefix=key)

        right = right.update(right_func, metadata=True).filter(lambda value: False)
        left = left.apply(left_func, metadata=True).filter(
            lambda value: value is not DISCARDED
        )
        return left.concat(right)
