import functools
import logging
from collections import deque
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

        max_expired_window_start = state.get_latest_timestamp() - duration_ms - grace_ms
        updated_windows = []
        for start, end in ranges:
            if start <= max_expired_window_start:
                ctx = message_context()
                logger.warning(
                    f"Skipping window processing for expired window "
                    f"timestamp={timestamp_ms} "
                    f"window=[{start},{end}) "
                    f"max_expired_window_start={max_expired_window_start} "
                    f"partition={ctx.topic}[{ctx.partition}] "
                    f"offset={ctx.offset}"
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

        # `state.update_window` updates the latest timestamp, so fetch it again
        watermark = state.get_latest_timestamp() - duration_ms - grace_ms
        expired_windows = []
        for (start, end), aggregated in state.expire_windows(watermark=watermark):
            expired_windows.append(
                {"start": start, "end": end, "value": self._merge_func(aggregated)}
            )
        return updated_windows, expired_windows

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


class SlidingWindow(FixedTimeWindow):
    def process_window(
        self,
        value: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
        duration = self._duration_ms
        grace = self._grace_ms
        aggregate = self._aggregate_func
        default = self._aggregate_default

        # Sliding windows are inclusive on both ends, so values with
        # timestamps equal to latest_timestamp - duration - grace
        # are still eligible for processing.
        expiration_watermark = state.get_latest_timestamp() - duration - grace - 1
        deletion_watermark = None

        left_start = max(0, timestamp_ms - duration)
        left_end = timestamp_ms

        right_start = timestamp_ms + 1
        right_end = right_start + duration
        right_exists = False

        starts = set([left_start])
        updated_windows = deque()
        iterated_windows = state.get_windows(
            # start_from_ms is exclusive, hence -1
            start_from_ms=max(0, left_start - duration) - 1,
            start_to_ms=right_start,
            # Iterating backwards makes the algorithm more efficient because
            # it starts with the rightmost windows, where existing aggregations
            # are checked. Once the aggregation for the left window is found,
            # the iteration can be terminated early.
            backwards=True,
        )

        for (start, end), (max_timestamp, aggregation) in iterated_windows:
            starts.add(start)

            if start == right_start:
                # Right window already exists; no need to create it
                right_exists = True

            elif end > left_end:
                # Create the right window if it does not exist and will not be empty
                if not right_exists and max_timestamp > timestamp_ms:
                    window = self._update_window(
                        state=state,
                        start=right_start,
                        end=right_end,
                        value=aggregation,
                        timestamp=timestamp_ms,
                        window_timestamp=max_timestamp,
                    )
                    if right_end == max_timestamp:
                        updated_windows.appendleft(window)
                    right_exists = True

                # Update existing window
                window_timestamp = max(timestamp_ms, max_timestamp)
                window = self._update_window(
                    state=state,
                    start=start,
                    end=end,
                    value=aggregate(aggregation, value),
                    timestamp=timestamp_ms,
                    window_timestamp=window_timestamp,
                )
                if end == window_timestamp:
                    updated_windows.appendleft(window)

            elif end == left_end:
                # Backfill the right window for previous messages
                if (
                    # Right window does not already exist
                    (right_start := max_timestamp + 1) not in starts
                    # Current value belongs to the right window
                    and (right_end := right_start + duration) > timestamp_ms
                    # If max_timestamp < timestamp_ms, this window becomes the left window
                    # for the current value, but previously it only served as the right window
                    # for other values. That means that the right window for its max_timestamp
                    # does not exist yet and needs to be created.
                    and max_timestamp < timestamp_ms
                ):
                    self._update_window(
                        state=state,
                        start=right_start,
                        end=right_end,
                        value=aggregate(default, value),
                        timestamp=timestamp_ms,
                        window_timestamp=timestamp_ms,
                    )

                # The left window already exists; updating it is sufficient
                updated_windows.appendleft(
                    self._update_window(
                        state=state,
                        start=start,
                        end=end,
                        value=aggregate(aggregation, value),
                        timestamp=timestamp_ms,
                        window_timestamp=timestamp_ms,
                    )
                )
                break

            elif end < left_end:
                # Backfill the right window for previous messages
                if (
                    # Right window does not already exist
                    (right_start := max_timestamp + 1) not in starts
                    # Current value belongs to the right window
                    and (right_end := right_start + duration) > timestamp_ms
                    # This is similar to right window creation when end == left_end,
                    # but since max_timestamp is lower than timestamp_ms, we check
                    # the expiration watermark instead.
                    and right_start > expiration_watermark
                ):
                    self._update_window(
                        state=state,
                        start=right_start,
                        end=right_end,
                        value=aggregate(default, value),
                        timestamp=timestamp_ms,
                        window_timestamp=timestamp_ms,
                    )

                # Create a left window with existing aggregation if it falls within the window
                if left_start > max_timestamp:
                    aggregation = default
                updated_windows.appendleft(
                    self._update_window(
                        state=state,
                        start=left_start,
                        end=left_end,
                        value=aggregate(aggregation, value),
                        timestamp=timestamp_ms,
                        window_timestamp=timestamp_ms,
                    )
                )

                # At this point, this is the last window that will be considered
                # for existing aggregations. Windows lower than this and lower than
                # the expiration watermark may be deleted.
                deletion_watermark = start - 1
                break

        else:
            # Create the left window as iteration completed without creating (or updating) it
            updated_windows.appendleft(
                self._update_window(
                    state=state,
                    start=left_start,
                    end=left_end,
                    value=aggregate(default, value),
                    timestamp=timestamp_ms,
                    window_timestamp=timestamp_ms,
                )
            )

        # `state.update_window` updates the latest timestamp, so fetch it again
        expiration_watermark = state.get_latest_timestamp() - duration - grace - 1
        expired_windows = [
            {"start": start, "end": end, "value": self._merge_func(aggregation)}
            for (start, end), (max_timestamp, aggregation) in state.expire_windows(
                watermark=expiration_watermark,
                delete=False,
            )
            if end == max_timestamp  # Include only left windows
        ]

        if deletion_watermark is not None:
            deletion_watermark = min(deletion_watermark, expiration_watermark)
        else:
            deletion_watermark = expiration_watermark - duration
        state.delete_windows(watermark=deletion_watermark)

        return updated_windows, expired_windows

    def _update_window(
        self,
        state: WindowedState,
        start: int,
        end: int,
        value: Any,
        timestamp: int,
        window_timestamp: int,
    ) -> dict[str, Any]:
        state.update_window(
            start_ms=start,
            end_ms=end,
            value=value,
            timestamp_ms=timestamp,
            window_timestamp_ms=window_timestamp,
        )
        return {"start": start, "end": end, "value": self._merge_func(value)}
