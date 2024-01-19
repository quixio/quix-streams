import functools
from typing import Any, Optional, List, TYPE_CHECKING, cast

from quixstreams.context import (
    message_context,
    message_key,
)
from quixstreams.state import (
    StateStoreManager,
    WindowedPartitionTransaction,
    WindowedState,
)
from .base import (
    WindowedDataFrameFunc,
    WindowAggregateFunc,
    WindowMergeFunc,
    WindowResult,
    get_window_ranges,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame, DataFrameFunc


def _default_merge_func(state_value: Any) -> Any:
    return state_value


class FixedTimeWindow:
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        name: str,
        aggregate_func: WindowAggregateFunc,
        dataframe: "StreamingDataFrame",
        merge_func: Optional[WindowMergeFunc] = None,
        step_ms: Optional[int] = None,
    ):
        if not name:
            raise ValueError("Window name must not be empty")

        self._duration_ms = duration_ms
        self._grace_ms = grace_ms
        self._name = name
        self._aggregate_func = aggregate_func
        self._merge_func = merge_func or _default_merge_func
        self._dataframe = dataframe
        self._step_ms = step_ms

    @property
    def name(self) -> str:
        return self._name

    def process_window(
        self, value: Any, state: WindowedState, timestamp_ms: int
    ) -> (List[WindowResult], List[WindowResult]):
        duration_ms = self._duration_ms
        grace_ms = self._grace_ms

        latest_timestamp = state.get_latest_timestamp()
        ranges = get_window_ranges(
            timestamp_ms=timestamp_ms,
            duration_ms=duration_ms,
            step_ms=self._step_ms,
        )

        updated_windows = []
        for start, end in ranges:
            # TODO: Log when the value is late and no window is updated
            if not self._stale(window_end=end, latest_timestamp=latest_timestamp):
                aggregated = self._aggregate_func(
                    start, end, timestamp_ms, value, state
                )
                updated_windows.append(
                    {
                        "start": start,
                        "end": end,
                        "value": self._merge_func(aggregated),
                    }
                )

        expired_windows = []
        for (start, end), aggregated in state.expire_windows(
            duration_ms=duration_ms, grace_ms=grace_ms
        ):
            expired_windows.append(
                {"start": start, "end": end, "value": self._merge_func(aggregated)}
            )
        return updated_windows, expired_windows

    def final(self, expand: bool = True) -> "StreamingDataFrame":
        """
        Apply the window aggregation and return results only when the windows are
        closed.

        The format of returned windows:
        ```python
        {
            "start": <window start time in milliseconds>,
            "end": <window end time in milliseconds>,
            "value: <aggregated window value>,
        }
        ```

        The individual window is closed when the event time
        (the maximum observed timestamp across the partition) passes
        its end timestamp + grace period.
        The closed windows cannot receive updates anymore and are considered final.

        >***NOTE:*** Windows can be closed only within the same message key.
        If some message keys appear irregularly in the stream, the latest windows
        can remain unprocessed until the message the same key is received.

        :param expand: if `True`, each window result will be sent downstream as
            an individual item. Otherwise, the list of window results will be sent.
            Default - `True`
        """
        return self._apply_window(
            lambda value, state, process_window=self.process_window: process_window(
                value=value,
                state=state,
                timestamp_ms=message_context().timestamp.milliseconds,
            )[1],
            expand=expand,
            name=self._name,
        )

    def current(self, expand: bool = True) -> "StreamingDataFrame":
        """
        Apply the window transformation to the StreamingDataFrame to return results
        for each updated window.

        The format of returned windows:
        ```python
        {
            "start": <window start time in milliseconds>,
            "end": <window end time in milliseconds>,
            "value: <aggregated window value>,
        }
        ```

        This method processes streaming data and returns results as they come,
        regardless of whether the window is closed or not.

        :param expand: if `True`, each window result will be sent downstream as
            an individual item. Otherwise, the list of window results will be sent.
            Default - `True`
        """
        return self._apply_window(
            lambda value, state, process_window=self.process_window: process_window(
                value=value,
                state=state,
                timestamp_ms=message_context().timestamp.milliseconds,
            )[0],
            expand=expand,
            name=self._name,
        )

    def register_store(self):
        self._dataframe.state_manager.register_windowed_store(
            topic_name=self._dataframe.topic.name, store_name=self._name
        )

    def _stale(self, window_end: int, latest_timestamp: int) -> bool:
        return latest_timestamp > window_end + self._grace_ms

    def _apply_window(
        self,
        func: WindowedDataFrameFunc,
        name: str,
        expand: bool = False,
    ) -> "StreamingDataFrame":
        self.register_store()

        func = _as_windowed(
            func=func, state_manager=self._dataframe.state_manager, store_name=name
        )

        return self._dataframe.apply(func=func, expand=expand)


def _as_windowed(
    func: WindowedDataFrameFunc, state_manager: StateStoreManager, store_name: str
) -> "DataFrameFunc":
    @functools.wraps(func)
    def wrapper(value: object) -> object:
        transaction = cast(
            WindowedPartitionTransaction,
            state_manager.get_store_transaction(store_name=store_name),
        )
        key = message_key()
        with transaction.with_prefix(prefix=key):
            return func(value, transaction.state)

    return wrapper
