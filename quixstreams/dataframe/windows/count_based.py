import logging
from typing import TYPE_CHECKING, Any, Iterable, Optional, TypedDict

from quixstreams.state import WindowedState

from .base import (
    Window,
    WindowAggregateFunc,
    WindowMergeFunc,
    WindowResult,
    default_merge_func,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame


logger = logging.getLogger(__name__)


class WindowMetadata(TypedDict):
    count: int
    start: int
    end: int


class FixedCountWindow(Window):
    STATE_KEY = "data"

    def __init__(
        self,
        name: str,
        count: int,
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
        aggregate_collection: bool,
        dataframe: "StreamingDataFrame",
        merge_func: Optional[WindowMergeFunc] = None,
    ):
        super().__init__(name, dataframe)

        self._max_count = count
        self._aggregate_func = aggregate_func
        self._aggregate_default = [] if aggregate_collection else aggregate_default
        self._aggregate_collection = aggregate_collection
        self._merge_func = merge_func or default_merge_func

    def process_window(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        if self._aggregate_collection:
            return self._process_window_collection(value, timestamp_ms, state)
        return self._process_window(value, timestamp_ms, state)

    def _process_window(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        data = state.get(key=self.STATE_KEY)
        if data is None:
            metadata = WindowMetadata(count=0, start=timestamp_ms, end=timestamp_ms)
            previous_value = self._aggregate_default
        else:
            metadata, previous_value = data

        metadata["count"] += 1
        if timestamp_ms > metadata["end"]:
            metadata["end"] = timestamp_ms

        aggregated = self._aggregate_func(previous_value, value)
        updated_windows = [
            WindowResult(
                start=metadata["start"],
                end=metadata["end"],
                value=self._merge_func(aggregated),
            )
        ]

        if metadata["count"] >= self._max_count:
            state.delete(key=self.STATE_KEY)
            return updated_windows, updated_windows

        state.set(key=self.STATE_KEY, value=(metadata, aggregated))
        return updated_windows, []

    def _process_window_collection(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        data = state.get(key=self.STATE_KEY)
        if data is None:
            metadata = WindowMetadata(count=0, start=timestamp_ms, end=timestamp_ms)
            collection = []
        else:
            metadata, collection = data

        metadata["count"] += 1
        if timestamp_ms > metadata["end"]:
            metadata["end"] = timestamp_ms

        collection.append(value)

        if metadata["count"] >= self._max_count:
            state.delete(key=self.STATE_KEY)
            return [], [
                WindowResult(
                    start=metadata["start"],
                    end=metadata["end"],
                    value=self._merge_func(collection),
                )
            ]

        state.set(key=self.STATE_KEY, value=(metadata, collection))
        return [], []
