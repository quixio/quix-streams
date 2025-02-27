import logging
import time
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable, Literal, Optional

from quixstreams.context import message_context
from quixstreams.state import WindowedPartitionTransaction, WindowedState

from .base import (
    Window,
    WindowAggregateFunc,
    WindowKeyResult,
    WindowMergeFunc,
    WindowOnLateCallback,
    WindowResult,
    default_merge_func,
    get_window_ranges,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

logger = logging.getLogger(__name__)


class ClosingStrategy(Enum):
    KEY = "key"
    PARTITION = "partition"

    @classmethod
    def new(cls, value: str) -> "ClosingStrategy":
        try:
            return ClosingStrategy[value.upper()]
        except KeyError:
            raise TypeError(
                'closing strategy must be one of "key" or "partition'
            ) from None


ClosingStrategyValues = Literal["key", "partition"]


class TimeWindow(Window):
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
        on_late: Optional[WindowOnLateCallback] = None,
    ):
        super().__init__(name, dataframe)

        self._duration_ms = duration_ms
        self._grace_ms = grace_ms
        self._aggregate_func = aggregate_func
        self._aggregate_default = aggregate_default
        self._aggregate_collection = aggregate_collection
        self._merge_func = merge_func or default_merge_func
        self._step_ms = step_ms
        self._on_late = on_late

        self._closing_strategy = ClosingStrategy.KEY

    def final(
        self, closing_strategy: ClosingStrategyValues = "key"
    ) -> "StreamingDataFrame":
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


        :param closing_strategy: the strategy to use when closing windows.
            Possible values:
              - `"key"` - messages advance time and close windows with the same key.
              If some message keys appear irregularly in the stream, the latest windows can remain unprocessed until a message with the same key is received.
              - `"partition"` - messages advance time and close windows for the whole partition to which this message key belongs.
              If timestamps between keys are not ordered, it may increase the number of discarded late messages.
              Default - `"key"`.
        """
        self._closing_strategy = ClosingStrategy.new(closing_strategy)
        return super().final()

    def process_window(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        transaction: WindowedPartitionTransaction,
    ) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]:
        state = transaction.as_state(prefix=key)
        duration_ms = self._duration_ms
        grace_ms = self._grace_ms
        default = self._aggregate_default
        collect = self._aggregate_collection

        ranges = get_window_ranges(
            timestamp_ms=timestamp_ms,
            duration_ms=duration_ms,
            step_ms=self._step_ms,
        )

        if self._closing_strategy == ClosingStrategy.PARTITION:
            latest_expired_window_end = transaction.get_latest_expired(prefix=b"")
            latest_timestamp = max(timestamp_ms, latest_expired_window_end)
        else:
            state_ts = state.get_latest_timestamp() or 0
            latest_timestamp = max(timestamp_ms, state_ts)

        max_expired_window_end = latest_timestamp - grace_ms
        max_expired_window_start = max_expired_window_end - duration_ms
        updated_windows: list[WindowKeyResult] = []
        for start, end in ranges:
            if start <= max_expired_window_start:
                late_by_ms = max_expired_window_end - timestamp_ms
                self._on_expired_window(
                    value=value,
                    key=key,
                    start=start,
                    end=end,
                    timestamp_ms=timestamp_ms,
                    late_by_ms=late_by_ms,
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
                (
                    key,
                    WindowResult(
                        start=start, end=end, value=self._merge_func(aggregated)
                    ),
                )
            )

        if collect:
            state.add_to_collection(value=value, id=timestamp_ms)

        if self._closing_strategy == ClosingStrategy.PARTITION:
            expired_windows = self.expire_by_partition(
                transaction, max_expired_window_end, collect
            )
        else:
            expired_windows = self.expire_by_key(
                key, state, max_expired_window_start, collect
            )

        return updated_windows, expired_windows

    def expire_by_partition(
        self,
        transaction: WindowedPartitionTransaction,
        max_expired_end: int,
        collect: bool,
    ) -> Iterable[WindowKeyResult]:
        start = time.monotonic()
        count = 0

        for (
            window_start,
            window_end,
        ), aggregated, key in transaction.expire_all_windows(
            max_end_time=max_expired_end,
            step_ms=self._step_ms if self._step_ms else self._duration_ms,
            collect=collect,
            delete=True,
        ):
            count += 1
            yield (
                key,
                WindowResult(
                    start=window_start,
                    end=window_end,
                    value=self._merge_func(aggregated),
                ),
            )

        if count:
            logger.debug(
                "Expired %s windows in %ss", count, round(time.monotonic() - start, 2)
            )

    def expire_by_key(
        self,
        key: bytes,
        state: WindowedState,
        max_expired_start: int,
        collect: bool,
    ) -> Iterable[WindowKeyResult]:
        for (start, end), aggregated, _ in state.expire_windows(
            max_start_time=max_expired_start,
            collect=collect,
        ):
            yield (
                key,
                WindowResult(start=start, end=end, value=self._merge_func(aggregated)),
            )

    def _on_expired_window(
        self,
        value: Any,
        key: Any,
        start: int,
        end: int,
        timestamp_ms: int,
        late_by_ms: int,
    ) -> None:
        ctx = message_context()
        to_log = True
        # Trigger the "on_late" callback if provided.
        # Log the lateness warning if the callback returns True
        if self._on_late:
            to_log = self._on_late(
                value,
                key,
                timestamp_ms,
                late_by_ms,
                start,
                end,
                self._name,
                ctx.topic,
                ctx.partition,
                ctx.offset,
            )
        if to_log:
            logger.warning(
                "Skipping window processing for the closed window "
                f"timestamp_ms={timestamp_ms} "
                f"window={(start, end)} "
                f"late_by_ms={late_by_ms} "
                f"store_name={self._name} "
                f"partition={ctx.topic}[{ctx.partition}] "
                f"offset={ctx.offset}"
            )
