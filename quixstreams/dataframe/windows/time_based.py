import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Optional,
)

from quixstreams.context import message_context
from quixstreams.state import WindowedState

from .base import (
    Window,
    WindowAggregateFunc,
    WindowMergeFunc,
    WindowResult,
    default_merge_func,
    get_window_ranges,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

logger = logging.getLogger(__name__)


class FixedTimeWindow(Window):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        name: str,
        dataframe: "StreamingDataFrame",
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
        aggregate_collection: bool = False,
        merge_func: Optional[WindowMergeFunc] = None,
        step_ms: Optional[int] = None,
    ):
        super().__init__(name, dataframe)

        self._duration_ms = duration_ms
        self._grace_ms = grace_ms
        self._aggregate_func = aggregate_func
        self._aggregate_default = aggregate_default
        self._aggregate_collection = aggregate_collection
        self._merge_func = merge_func or default_merge_func
        self._step_ms = step_ms

    def process_window(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        duration_ms = self._duration_ms
        grace_ms = self._grace_ms
        default = self._aggregate_default
        collect = self._aggregate_collection

        ranges = get_window_ranges(
            timestamp_ms=timestamp_ms,
            duration_ms=duration_ms,
            step_ms=self._step_ms,
        )

        state_ts = state.get_latest_id() or 0
        latest_timestamp = max(timestamp_ms, state_ts)
        max_expired_window_end = latest_timestamp - grace_ms
        max_expired_window_start = max_expired_window_end - duration_ms
        updated_windows: list[WindowResult] = []
        for start, end in ranges:
            if start <= max_expired_window_start:
                self._log_expired_window(
                    window=[start, end],
                    timestamp_ms=timestamp_ms,
                    late_by_ms=max_expired_window_end - timestamp_ms,
                )
                continue

            if collect:
                # When collecting values, we only mark the window existence with None
                # since actual values are stored separately and combined into an array
                # during window expiration.
                state.update_window(start, end, value=None, timestamp_ms=timestamp_ms)
                continue

            current_value = state.get_window(start, end, default=default)
            aggregated = self._aggregate_func(current_value, value)
            state.update_window(start, end, value=aggregated, timestamp_ms=timestamp_ms)
            updated_windows.append(
                WindowResult(start=start, end=end, value=self._merge_func(aggregated))
            )

        if collect:
            state.add_to_collection(value=value, id=timestamp_ms)

        expired_windows: list[WindowResult] = []
        for (start, end), aggregated in state.expire_windows(
            max_start_time=max_expired_window_start,
            collect=collect,
        ):
            expired_windows.append(
                WindowResult(start=start, end=end, value=self._merge_func(aggregated))
            )
        return updated_windows, expired_windows

    def _log_expired_window(self, window, timestamp_ms, late_by_ms) -> None:
        ctx = message_context()
        logger.warning(
            "Skipping window processing for expired window "
            f"timestamp_ms={timestamp_ms} "
            f"window={window} "
            f"late_by_ms={late_by_ms} "
            f"partition={ctx.topic}[{ctx.partition}] "
            f"offset={ctx.offset}"
        )
