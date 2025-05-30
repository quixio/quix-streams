import typing
from datetime import timedelta
from typing import Any, Callable, Literal, Optional, Union, cast, get_args

from quixstreams.context import message_context
from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.models.topics.manager import TopicManager
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .utils import keep_left_merger, keep_right_merger, raise_merger

if typing.TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame


__all__ = ("JoinAsOfHow", "OnOverlap", "JoinAsOf")

DISCARDED = object()
JoinAsOfHow = Literal["inner", "left"]
JoinAsOfHow_choices = get_args(JoinAsOfHow)

OnOverlap = Literal["keep-left", "keep-right", "raise"]
OnOverlap_choices = get_args(OnOverlap)


class JoinAsOf:
    def __init__(
        self,
        how: JoinAsOfHow,
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: Optional[str] = None,
    ):
        if how not in JoinAsOfHow_choices:
            raise ValueError(
                f'Invalid "how" value: {how}. '
                f"Valid choices are: {', '.join(JoinAsOfHow_choices)}."
            )
        self._how = how

        if callable(on_merge):
            self._merger = on_merge
        elif on_merge == "keep-left":
            self._merger = keep_left_merger
        elif on_merge == "keep-right":
            self._merger = keep_right_merger
        elif on_merge == "raise":
            self._merger = raise_merger
        else:
            raise ValueError(
                f'Invalid "on_merge" value: {on_merge}. '
                f"Provide either one of {', '.join(OnOverlap_choices)} or "
                f"a callable to merge records manually."
            )

        self._grace_ms = ensure_milliseconds(grace_ms)
        self._store_name = store_name or "join"

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

        changelog_config = TopicManager.derive_topic_config(right.topics)
        right.processing_context.state_manager.register_timestamped_store(
            stream_id=right.stream_id,
            store_name=self._store_name,
            grace_ms=self._grace_ms,
            keep_duplicates=False,
            changelog_config=changelog_config,
        )

        is_inner_join = self._how == "inner"

        def left_func(value, key, timestamp, headers):
            tx = cast(
                TimestampedPartitionTransaction,
                right.processing_context.checkpoint.get_store_transaction(
                    stream_id=right.stream_id,
                    partition=message_context().partition,
                    store_name=self._store_name,
                ),
            )

            right_value = tx.get_latest(timestamp=timestamp, prefix=key)
            if is_inner_join and not right_value:
                return DISCARDED
            return self._merger(value, right_value)

        def right_func(value, key, timestamp, headers):
            tx = cast(
                TimestampedPartitionTransaction,
                right.processing_context.checkpoint.get_store_transaction(
                    stream_id=right.stream_id,
                    partition=message_context().partition,
                    store_name=self._store_name,
                ),
            )
            tx.set_for_timestamp(timestamp=timestamp, value=value, prefix=key)

        right = right.update(right_func, metadata=True).filter(lambda value: False)
        left = left.apply(left_func, metadata=True).filter(
            lambda value: value is not DISCARDED
        )
        return left.concat(right)
