from abc import ABC, abstractmethod
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Optional,
    Union,
    cast,
    get_args,
)

from quixstreams.context import message_context
from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.models.topics.manager import TopicManager
from quixstreams.state.rocksdb.timestamped import TimestampedPartitionTransaction

from .utils import keep_left_merger, keep_right_merger, raise_merger

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

__all__ = ("Join", "OnOverlap", "OnOverlap_choices")

OnOverlap = Literal["keep-left", "keep-right", "raise"]
OnOverlap_choices = get_args(OnOverlap)


class Join(ABC):
    def __init__(
        self,
        how: str,
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: Optional[str] = None,
    ) -> None:
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

        self._how = how
        self._grace_ms = ensure_milliseconds(grace_ms)
        self._store_name = store_name or "join"

    def join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame":
        self._validate_dataframes(left, right)
        return self._prepare_join(left, right)

    @abstractmethod
    def _prepare_join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame": ...

    def _validate_dataframes(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> None:
        if left.stream_id == right.stream_id:
            raise ValueError(
                "Joining dataframes originating from "
                "the same topic is not yet supported.",
            )
        TopicManager.ensure_topics_copartitioned(*left.topics, *right.topics)

    def _register_store(
        self,
        sdf: "StreamingDataFrame",
        keep_duplicates: bool,
    ) -> None:
        sdf.processing_context.state_manager.register_timestamped_store(
            stream_id=sdf.stream_id,
            store_name=self._store_name,
            grace_ms=self._grace_ms,
            keep_duplicates=keep_duplicates,
            changelog_config=TopicManager.derive_topic_config(sdf.topics),
        )

    def _get_transaction(
        self, sdf: "StreamingDataFrame"
    ) -> TimestampedPartitionTransaction:
        return cast(
            TimestampedPartitionTransaction,
            sdf.processing_context.checkpoint.get_store_transaction(
                stream_id=sdf.stream_id,
                partition=message_context().partition,
                store_name=self._store_name,
            ),
        )
