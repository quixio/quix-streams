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


class CountWindowData(TypedDict):
    count: int
    start: int
    end: int
    value: Any


class FixedCountWindow(Window):
    STATE_KEY = "metadata"
    VALUE_KEY = "value"

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
            data = CountWindowData(
                count=0,
                start=timestamp_ms,
                end=timestamp_ms,
                value=self._aggregate_default,
            )

        data["count"] += 1
        if timestamp_ms > data["end"]:
            data["end"] = timestamp_ms

        data["value"] = self._aggregate_func(data["value"], value)
        updated_windows = [
            WindowResult(
                start=data["start"],
                end=data["end"],
                value=self._merge_func(data["value"]),
            )
        ]

        if data["count"] >= self._max_count:
            state.delete(key=self.STATE_KEY)
            return updated_windows, updated_windows

        state.set(key=self.STATE_KEY, value=data)
        return updated_windows, []

    def _process_window_collection(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        data = state.get(key=self.STATE_KEY)
        if data is None:
            data = CountWindowData(
                count=0, start=timestamp_ms, end=timestamp_ms, value=None
            )

        data["count"] += 1
        if timestamp_ms < data["start"]:
            data["start"] = timestamp_ms
        elif timestamp_ms > data["end"]:
            data["end"] = timestamp_ms

        if data["count"] < self._max_count:
            state.add_to_collection(data["count"], value)
            state.set(key=self.STATE_KEY, value=data)
            return [], []

        # window is full, closing ...
        state.delete(key=self.STATE_KEY)

        values = state.get_from_collection(start=1, end=10)
        values.append(value)

        result = WindowResult(
            start=data["start"],
            end=data["end"],
            value=self._merge_func(values),
        )
        state.delete_from_collection(start=1, end=10)
        return [], [result]
