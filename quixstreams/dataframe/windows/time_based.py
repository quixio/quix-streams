import logging
from typing import TYPE_CHECKING, Any, Iterable, Optional

from quixstreams.context import message_context
from quixstreams.state import WindowedPartitionTransaction
from quixstreams.utils.format import format_timestamp

from .base import (
    Message,
    MultiAggregationWindowMixin,
    SingleAggregationWindowMixin,
    Window,
    WindowAfterUpdateCallback,
    WindowBeforeUpdateCallback,
    WindowKeyResult,
    WindowOnLateCallback,
    get_window_ranges,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

logger = logging.getLogger(__name__)


class TimeWindow(Window):
    def __init__(
        self,
        duration_ms: int,
        grace_ms: int,
        name: str,
        dataframe: "StreamingDataFrame",
        step_ms: Optional[int] = None,
        on_late: Optional[WindowOnLateCallback] = None,
        before_update: Optional[WindowBeforeUpdateCallback] = None,
        after_update: Optional[WindowAfterUpdateCallback] = None,
    ):
        super().__init__(
            name=name,
            dataframe=dataframe,
        )

        self._duration_ms = duration_ms
        self._grace_ms = grace_ms
        self._step_ms = step_ms
        self._on_late = on_late
        self._before_update = before_update
        self._after_update = after_update

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

        """

        def on_update(
            value: Any,
            key: Any,
            timestamp_ms: int,
            _headers: Any,
            transaction: WindowedPartitionTransaction,
        ):
            # Process the window and get windows triggered from callbacks
            _, triggered_windows = self.process_window(
                value=value,
                key=key,
                timestamp_ms=timestamp_ms,
                headers=_headers,
                transaction=transaction,
            )
            # Yield triggered windows (from before_update/after_update callbacks)
            for key, window in triggered_windows:
                yield window, key, window["start"], None

        def on_watermark(
            _value: Any,
            _key: Any,
            timestamp_ms: int,
            _headers: Any,
            transaction: WindowedPartitionTransaction,
        ) -> Iterable[Message]:
            expired_windows = self.expire_by_partition(
                transaction=transaction, timestamp_ms=timestamp_ms
            )

            total_expired = 0
            # Use window start timestamp as a new record timestamp
            for key, window in expired_windows:
                total_expired += 1
                yield window, key, window["start"], None

            ctx = message_context()
            logger.info(
                f"Expired {total_expired} windows after processing "
                f"the watermark at {format_timestamp(timestamp_ms)}. "
                f"window_name={self._name} topic={ctx.topic} "
                f"partition={ctx.partition} timestamp={timestamp_ms}"
            )

        return self._apply_window(
            on_update=on_update,
            on_watermark=on_watermark,
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

        def on_update(
            value: Any,
            key: Any,
            timestamp_ms: int,
            _headers: Any,
            transaction: WindowedPartitionTransaction,
        ):
            # Process the window and get both updated and triggered windows
            updated_windows, triggered_windows = self.process_window(
                value=value,
                key=key,
                timestamp_ms=timestamp_ms,
                headers=_headers,
                transaction=transaction,
            )
            # Use window start timestamp as a new record timestamp
            # Yield both updated and triggered windows
            for key, window in updated_windows:
                yield window, key, window["start"], None
            for key, window in triggered_windows:
                yield window, key, window["start"], None

        def on_watermark(
            _value: Any,
            _key: Any,
            timestamp_ms: int,
            _headers: Any,
            transaction: WindowedPartitionTransaction,
        ) -> Iterable[Message]:
            expired_windows = self.expire_by_partition(
                transaction=transaction, timestamp_ms=timestamp_ms
            )
            # Just exhaust the iterator here
            for _ in expired_windows:
                pass
            return []

        return self._apply_window(
            on_update=on_update,
            on_watermark=on_watermark,
            name=self._name,
        )

    def process_window(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        headers: Any,
        transaction: WindowedPartitionTransaction,
    ) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]:
        """
        Process a window update for the given value and key.

        Returns:
            A tuple of (updated_windows, triggered_windows) where:
            - updated_windows: Windows that were updated but not expired
            - triggered_windows: Windows that were expired early due to before_update/after_update callbacks
        """
        state = transaction.as_state(prefix=key)
        duration_ms = self._duration_ms
        grace_ms = self._grace_ms
        before_update = self._before_update
        after_update = self._after_update

        collect = self.collect
        aggregate = self.aggregate

        ranges = get_window_ranges(
            timestamp_ms=timestamp_ms,
            duration_ms=duration_ms,
            step_ms=self._step_ms,
        )

        latest_expired_window_end = transaction.get_latest_expired(prefix=b"")
        latest_timestamp = max(timestamp_ms, latest_expired_window_end)

        max_expired_window_end = latest_timestamp - grace_ms
        max_expired_window_start = max_expired_window_end - duration_ms
        updated_windows: list[WindowKeyResult] = []
        triggered_windows: list[WindowKeyResult] = []
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

            # When collecting values, we only mark the window existence with None
            # since actual values are stored separately and combined into an array
            # during window expiration.
            aggregated = None

            if aggregate:
                current_value = state.get_window(start, end)
                if current_value is None:
                    current_value = self._initialize_value()

                # Check before_update trigger
                if before_update and before_update(
                    current_value, value, key, timestamp_ms, headers
                ):
                    # Get collected values for the result
                    # Do NOT include the current value - before_update means
                    # we expire BEFORE adding the current value
                    collected = state.get_from_collection(start, end) if collect else []

                    result = self._results(current_value, collected, start, end)
                    triggered_windows.append((key, result))
                    transaction.delete_window(start, end, prefix=key)
                    # Note: We don't delete from collection here - normal expiration
                    # will handle cleanup for both tumbling and hopping windows
                    continue

                aggregated = self._aggregate_value(current_value, value, timestamp_ms)

                # Check after_update trigger
                if after_update and after_update(
                    aggregated, value, key, timestamp_ms, headers
                ):
                    # Get collected values for the result
                    collected = []
                    if collect:
                        collected = state.get_from_collection(start, end)
                        # Add the current value that's being collected
                        collected.append(self._collect_value(value))

                    result = self._results(aggregated, collected, start, end)
                    triggered_windows.append((key, result))
                    transaction.delete_window(start, end, prefix=key)
                    # Note: We don't delete from collection here - normal expiration
                    # will handle cleanup for both tumbling and hopping windows
                    continue

                result = self._results(aggregated, [], start, end)
                updated_windows.append((key, result))
            elif collect and (before_update or after_update):
                # For collect-only windows, get the old collected values
                old_collected = state.get_from_collection(start, end)

                # Check before_update trigger (before adding new value)
                if before_update and before_update(
                    old_collected, value, key, timestamp_ms, headers
                ):
                    # Expire with the current collection (WITHOUT the new value)
                    result = self._results(None, old_collected, start, end)
                    triggered_windows.append((key, result))
                    transaction.delete_window(start, end, prefix=key)
                    # Note: We don't delete from collection here - normal expiration
                    # will handle cleanup for both tumbling and hopping windows
                    continue

                # Check after_update trigger (conceptually after adding new value)
                # For collect, "after update" means after the value would be added
                if after_update:
                    new_collected = [*old_collected, self._collect_value(value)]
                    if after_update(new_collected, value, key, timestamp_ms, headers):
                        result = self._results(None, new_collected, start, end)
                        triggered_windows.append((key, result))
                        transaction.delete_window(start, end, prefix=key)
                        # Note: We don't delete from collection here - normal expiration
                        # will handle cleanup for both tumbling and hopping windows
                        continue

            state.update_window(start, end, value=aggregated, timestamp_ms=timestamp_ms)

        if collect:
            state.add_to_collection(
                value=self._collect_value(value),
                id=timestamp_ms,
            )

        return updated_windows, triggered_windows

    def expire_by_partition(
        self,
        transaction: WindowedPartitionTransaction,
        timestamp_ms: int,
    ) -> Iterable[WindowKeyResult]:
        """
        Expire windows for the whole partition at the given timestamp.

        :param transaction: state transaction object.
        :param timestamp_ms: the current timestamp (inclusive).
        """
        latest_expired_window_end = transaction.get_latest_expired(prefix=b"")
        latest_timestamp = max(timestamp_ms, latest_expired_window_end)
        max_expired_window_end = max(latest_timestamp - self._grace_ms, 0)

        for (
            window_start,
            window_end,
        ), aggregated, collected, key in transaction.expire_all_windows(
            max_end_time=max_expired_window_end,
            step_ms=self._step_ms if self._step_ms else self._duration_ms,
            collect=self.collect,
            delete=True,
        ):
            yield key, self._results(aggregated, collected, window_start, window_end)

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
                "Skipping record processing for the closed window. "
                f"timestamp_ms={format_timestamp(timestamp_ms)} ({timestamp_ms}ms) "
                f"window=[{format_timestamp(start)}, {format_timestamp(end)}) ([{start}ms, {end}ms)) "
                f"late_by={late_by_ms}ms "
                f"store_name={self._name} "
                f"partition={ctx.topic}[{ctx.partition}]"
            )


class TimeWindowSingleAggregation(SingleAggregationWindowMixin, TimeWindow):
    pass


class TimeWindowMultiAggregation(MultiAggregationWindowMixin, TimeWindow):
    pass
