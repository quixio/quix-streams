import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Iterable, Literal, Optional

from quixstreams.context import message_context
from quixstreams.state import WindowedPartitionTransaction, WindowedState

from .base import (
    MultiAggregationWindowMixin,
    SingleAggregationWindowMixin,
    Window,
    WindowKeyResult,
    WindowOnLateCallback,
    WindowResult,
    get_window_ranges,
)
from .triggers import TimeWindowTrigger, TriggerAction

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
        step_ms: Optional[int] = None,
        on_late: Optional[WindowOnLateCallback] = None,
    ):
        super().__init__(
            name=name,
            dataframe=dataframe,
        )

        self._duration_ms = duration_ms
        self._grace_ms = grace_ms
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

    def current(
        self, closing_strategy: ClosingStrategyValues = "key"
    ) -> "StreamingDataFrame":
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

        :param closing_strategy: the strategy to use when closing windows.
            Possible values:
              - `"key"` - messages advance time and close windows with the same key.
              If some message keys appear irregularly in the stream, the latest windows can remain unprocessed until a message with the same key is received.
              - `"partition"` - messages advance time and close windows for the whole partition to which this message key belongs.
              If timestamps between keys are not ordered, it may increase the number of discarded late messages.
              Default - `"key"`.
        """

        self._closing_strategy = ClosingStrategy.new(closing_strategy)
        return super().current()

    def trigger(self, trigger: TimeWindowTrigger) -> "StreamingDataFrame":
        def window_callback(
            value: Any,
            key: Any,
            timestamp_ms: int,
            _headers: Any,
            transaction: WindowedPartitionTransaction,
        ) -> Iterable[tuple[WindowResult, Any, int, Any]]:
            windows = self.process_window_new(
                value=value,
                key=key,
                timestamp_ms=timestamp_ms,
                transaction=transaction,
                trigger=trigger,
            )
            for key, window in windows:
                yield window, key, window["start"], None

        return self._apply_window(func=window_callback, name=self._name)

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

        collect = self.collect
        aggregate = self.aggregate

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

            # When collecting values, we only mark the window existence with None
            # since actual values are stored separately and combined into an array
            # during window expiration.
            aggregated = None
            if aggregate:
                current_value = state.get_window(start, end)
                if current_value is None:
                    current_value = self._initialize_value()

                aggregated = self._aggregate_value(current_value, value, timestamp_ms)
                updated_windows.append(
                    (
                        key,
                        self._results(aggregated, [], start, end),
                    )
                )
            state.update_window(start, end, value=aggregated, timestamp_ms=timestamp_ms)

        if collect:
            state.add_to_collection(
                value=self._collect_value(value),
                id=timestamp_ms,
            )

        if self._closing_strategy == ClosingStrategy.PARTITION:
            expired_windows = self.expire_by_partition(
                transaction, max_expired_window_end, collect
            )
        else:
            expired_windows = self.expire_by_key(
                key, state, max_expired_window_start, collect
            )

        return updated_windows, expired_windows

    def process_window_new(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        transaction: WindowedPartitionTransaction,
        trigger: TimeWindowTrigger,
    ) -> Iterable[WindowKeyResult]:
        window_state = transaction.as_state(prefix=key)
        duration_ms = self._duration_ms
        grace_ms = self._grace_ms

        collect = self.collect
        aggregate = self.aggregate

        ranges = get_window_ranges(
            timestamp_ms=timestamp_ms,
            duration_ms=duration_ms,
            step_ms=self._step_ms,
        )

        if self._closing_strategy == ClosingStrategy.PARTITION:
            latest_expired_window_end = transaction.get_latest_expired(prefix=b"")
            latest_timestamp = max(timestamp_ms, latest_expired_window_end)
        else:
            state_ts = window_state.get_latest_timestamp() or 0
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

            # When collecting values, we only mark the window existence with None
            # since actual values are stored separately and combined into an array
            # during window expiration.
            aggregated = None
            if aggregate:
                current_window = window_state.get_window(start, end)
                if current_window is None:
                    current_window = self._initialize_value()

                aggregated = self._aggregate_value(current_window, value, timestamp_ms)

                updated_window = self._results(aggregated, [], start, end)

                window_state.update_window(
                    start, end, value=aggregated, timestamp_ms=timestamp_ms
                )

                trigger_state = transaction.as_trigger_state(
                    prefix=window_state.prefix, start_ms=start, end_ms=end
                )

                if (
                    action := trigger.on_window_updated(
                        value, start=start, end=end, state=trigger_state
                    )
                ) == TriggerAction.EMIT:
                    updated_windows.append((key, updated_window))
                elif action == TriggerAction.DELETE:
                    #     # TODO: Bump the event time here.
                    #     # TODO: For count-windows, you want to add an element first, and only then emit/delete it.
                    #     #   But for session-close events, do you want this?
                    window_state.delete_window(start, end)
                elif action == TriggerAction.EMIT_AND_DELETE:
                    # TODO: Bump the event time here.
                    # TODO: Do not update windows which are supposed to be deleted
                    # TODO: The deleted window must be returned in the sorted way together with other expired windows.
                    updated_windows.append((key, updated_window))
                    window_state.delete_window(start, end)

        if collect:
            # TODO: Is it correct? Looks like ".collect()" may not skip the late messages
            # TODO: Create windows when "collect" is used too
            # TODO: maybe rethink how "collect" is handled to accommodate the triggers
            window_state.add_to_collection(
                value=self._collect_value(value),
                id=timestamp_ms,
            )

        if self._closing_strategy == ClosingStrategy.PARTITION:
            expired_windows = self.expire_by_partition(
                transaction, max_expired_window_end, collect
            )
        else:
            expired_windows = self.expire_by_key(
                key, window_state, max_expired_window_start, collect
            )

        for key, window in expired_windows:
            # Ignore all other actions except "EMIT" and "EMIT_AND_DELETE"
            # because the time has already moved beyond the window boundaries
            if trigger.on_window_closed() in (
                TriggerAction.EMIT,
                TriggerAction.EMIT_AND_DELETE,
            ):
                yield key, window

        for key, window in updated_windows:
            yield key, window

    def expire_by_partition(
        self,
        transaction: WindowedPartitionTransaction,
        max_expired_end: int,
        collect: bool,
    ) -> Iterable[WindowKeyResult]:
        for (
            window_start,
            window_end,
        ), aggregated, collected, key in transaction.expire_all_windows(
            max_end_time=max_expired_end,
            step_ms=self._step_ms if self._step_ms else self._duration_ms,
            collect=collect,
            delete=True,
        ):
            yield key, self._results(aggregated, collected, window_start, window_end)

    def expire_by_key(
        self,
        key: Any,
        state: WindowedState,
        max_expired_start: int,
        collect: bool,
    ) -> Iterable[WindowKeyResult]:
        for (
            window_start,
            window_end,
        ), aggregated, collected, _ in state.expire_windows(
            max_start_time=max_expired_start,
            collect=collect,
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
                "Skipping window processing for the closed window "
                f"timestamp_ms={timestamp_ms} "
                f"window={(start, end)} "
                f"late_by_ms={late_by_ms} "
                f"store_name={self._name} "
                f"partition={ctx.topic}[{ctx.partition}] "
                f"offset={ctx.offset}"
            )


class TimeWindowSingleAggregation(SingleAggregationWindowMixin, TimeWindow):
    pass


class TimeWindowMultiAggregation(MultiAggregationWindowMixin, TimeWindow):
    pass
