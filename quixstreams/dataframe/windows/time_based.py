import functools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Tuple,
    cast,
)

from quixstreams.context import message_context
from quixstreams.core.stream import (
    TransformExpandedCallback,
)
from quixstreams.processing import ProcessingContext
from quixstreams.state import WindowedPartitionTransaction, WindowedState

from .base import (
    WindowAggregateFunc,
    WindowMergeFunc,
    WindowResult,
    get_window_ranges,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

logger = logging.getLogger(__name__)

TransformRecordCallbackExpandedWindowed = Callable[
    [Any, Any, int, Any, WindowedState], List[Tuple[WindowResult, Any, int, Any]]
]


def _default_merge_func(state_value: Any) -> Any:
    return state_value


class FixedTimeWindow:
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        name: str,
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
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
        self._aggregate_default = aggregate_default
        self._merge_func = merge_func or _default_merge_func
        self._dataframe = dataframe
        self._step_ms = step_ms

    @property
    def name(self) -> str:
        return self._name

    def process_window(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        duration_ms = self._duration_ms
        grace_ms = self._grace_ms
        default = self._aggregate_default

        ranges = get_window_ranges(
            timestamp_ms=timestamp_ms,
            duration_ms=duration_ms,
            step_ms=self._step_ms,
        )

        latest_timestamp = max(timestamp_ms, state.get_latest_timestamp())
        max_expired_window_start = latest_timestamp - duration_ms - grace_ms
        updated_windows = []
        for start, end in ranges:
            if start <= max_expired_window_start:
                self._log_expired_window(
                    window=[start, end],
                    timestamp_ms=timestamp_ms,
                    late_by_ms=max_expired_window_start + 1 - timestamp_ms,
                )
                continue

            current_value = state.get_window(start, end, default=default)
            aggregated = self._aggregate_func(current_value, value)
            state.update_window(start, end, timestamp_ms=timestamp_ms, value=aggregated)
            updated_windows.append(
                {
                    "start": start,
                    "end": end,
                    "value": self._merge_func(aggregated),
                }
            )

        expired_windows = []
        for (start, end), aggregated in state.expire_windows(
            max_start_time=max_expired_window_start
        ):
            expired_windows.append(
                {"start": start, "end": end, "value": self._merge_func(aggregated)}
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

    def final(self) -> "StreamingDataFrame":
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
        """

        def window_callback(
            value: Any, key: Any, timestamp_ms: int, _headers: Any, state: WindowedState
        ) -> List[Tuple[WindowResult, Any, int, Any]]:
            _, expired_windows = self.process_window(
                value=value, timestamp_ms=timestamp_ms, state=state
            )
            # Use window start timestamp as a new record timestamp
            return [(window, key, window["start"], None) for window in expired_windows]

        return self._apply_window(
            func=window_callback,
            name=self._name,
        )

    def current(self) -> "StreamingDataFrame":
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
        """

        def window_callback(
            value: Any, key: Any, timestamp_ms: int, _headers: Any, state: WindowedState
        ) -> List[Tuple[WindowResult, Any, int, Any]]:
            updated_windows, _ = self.process_window(
                value=value, timestamp_ms=timestamp_ms, state=state
            )
            return [(window, key, window["start"], None) for window in updated_windows]

        return self._apply_window(func=window_callback, name=self._name)

    def register_store(self):
        self._dataframe.processing_context.state_manager.register_windowed_store(
            topic_name=self._dataframe.topic.name, store_name=self._name
        )

    def _apply_window(
        self,
        func: TransformRecordCallbackExpandedWindowed,
        name: str,
    ) -> "StreamingDataFrame":
        self.register_store()

        windowed_func = _as_windowed(
            func=func,
            processing_context=self._dataframe.processing_context,
            store_name=name,
        )
        # Manually modify the Stream and clone the source StreamingDataFrame
        # to avoid adding "transform" API to it.
        # Transform callbacks can modify record key and timestamp,
        # and it's prone to misuse.
        stream = self._dataframe.stream.add_transform(func=windowed_func, expand=True)
        return self._dataframe.__dataframe_clone__(stream=stream)


def _noop() -> Any:
    """
    No-operation function for skipping messages due to None keys.

    Messages with None keys are ignored because keys are essential for performing
    accurate and meaningful windowed aggregation.
    """
    return []


def _as_windowed(
    func: TransformRecordCallbackExpandedWindowed,
    processing_context: ProcessingContext,
    store_name: str,
) -> TransformExpandedCallback:
    @functools.wraps(func)
    def wrapper(
        value: Any, key: Any, timestamp: int, headers: Any
    ) -> List[Tuple[WindowResult, Any, int, Any]]:
        ctx = message_context()
        transaction = cast(
            WindowedPartitionTransaction,
            processing_context.checkpoint.get_store_transaction(
                topic=ctx.topic, partition=ctx.partition, store_name=store_name
            ),
        )
        if key is None:
            logger.warning(
                f"Skipping window processing for a message because the key is None, "
                f"partition='{ctx.topic}[{ctx.partition}]' offset='{ctx.offset}'."
            )
            return _noop()
        state = transaction.as_state(prefix=key)
        return func(value, key, timestamp, headers, state)

    return wrapper
