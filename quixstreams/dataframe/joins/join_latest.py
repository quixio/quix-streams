import typing
from datetime import timedelta
from typing import Any, Callable, Literal, Union, cast, get_args

from quixstreams.context import message_context
from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.models.topics.manager import TopicManager
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .utils import keep_left_merger, keep_right_merger, raise_merger

if typing.TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

DISCARDED = object()
How = Literal["inner", "left"]
How_choices = get_args(How)

OnOverlap = Literal["keep-left", "keep-right", "raise"]
OnOverlap_choices = get_args(OnOverlap)


class JoinLatest:
    def __init__(
        self,
        how: How,
        on_overlap: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: str = "join",
    ):
        if how not in How_choices:
            raise ValueError(
                f'Invalid "how" value: {how}. '
                f"Valid choices are: {', '.join(How_choices)}."
            )
        self._how = how

        if callable(on_overlap):
            self._merger = on_overlap
        elif on_overlap == "keep-left":
            self._merger = keep_left_merger
        elif on_overlap == "keep-right":
            self._merger = keep_right_merger
        elif on_overlap == "raise":
            self._merger = raise_merger
        else:
            raise ValueError(
                f'Invalid "on_overlap" value: {on_overlap}. '
                f"Provide either one of {', '.join(OnOverlap_choices)} or "
                f"a callable to merge records manually."
            )

        self._retention_ms = ensure_milliseconds(grace_ms)
        self._store_name = store_name

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
        left.ensure_topics_copartitioned(*left.topics, *right.topics)

        changelog_config = TopicManager.derive_topic_config(right.topics)
        right.processing_context.state_manager.register_timestamped_store(
            stream_id=right.stream_id,
            store_name=self._store_name,
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

            right_value = tx.get_last(timestamp=timestamp, prefix=key)
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
            tx.set_for_timestamp(
                timestamp=timestamp,
                value=value,
                prefix=key,
                retention_ms=self._retention_ms,
            )

        right = right.update(right_func, metadata=True).filter(lambda value: False)
        left = left.apply(left_func, metadata=True).filter(
            lambda value: value is not DISCARDED
        )
        return left.concat(right)
