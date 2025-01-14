import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Optional,
)

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


class FixedCountWindow(Window):
    def __init__(
        self,
        name: str,
        count: int,
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
        dataframe: "StreamingDataFrame",
        merge_func: Optional[WindowMergeFunc] = None,
    ):
        super().__init__(name, dataframe)

        self._max_count = count
        self._aggregate_func = aggregate_func
        self._aggregate_default = aggregate_default
        self._merge_func = merge_func or default_merge_func

    def process_window(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        data = state.get(key="window")
        if data is None:
            metadata = {"count": 0, "start": timestamp_ms}
            previous_value = self._aggregate_default
        else:
            metadata, previous_value = data

        aggregated = self._aggregate_func(previous_value, value)

        metadata["count"] += 1
        windows = [
            WindowResult(
                start=metadata["start"],
                end=timestamp_ms,
                value=self._merge_func(aggregated),
            )
        ]

        if metadata["count"] >= self._max_count:
            state.delete(key="window")
            return windows, windows

        state.set(key="window", value=(metadata, aggregated))
        return windows, []
