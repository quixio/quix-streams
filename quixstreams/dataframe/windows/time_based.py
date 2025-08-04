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
            yield (key, self._results(aggregated, collected, window_start, window_end))

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


class SessionWindow(Window):
    """
    Session window groups events that occur within a specified timeout period.

    A session starts with the first event and extends each time a new event arrives
    within the timeout period. The session closes after the timeout period with no
    new events.

    Each session window can have different start and end times based on the actual
    events, making sessions dynamic rather than fixed-time intervals.
    """

    def __init__(
        self,
        timeout_ms: int,
        grace_ms: int,
        name: str,
        dataframe: "StreamingDataFrame",
        on_late: Optional[WindowOnLateCallback] = None,
    ):
        super().__init__(
            name=name,
            dataframe=dataframe,
        )

        self._timeout_ms = timeout_ms
        self._grace_ms = grace_ms
        self._on_late = on_late
        self._closing_strategy = ClosingStrategy.KEY

    def final(
        self, closing_strategy: ClosingStrategyValues = "key"
    ) -> "StreamingDataFrame":
        """
        Apply the session window aggregation and return results only when the sessions
        are closed.

        The format of returned sessions:
        ```python
        {
            "start": <session start time in milliseconds>,
            "end": <session end time in milliseconds>,
            "value: <aggregated session value>,
        }
        ```

        The individual session is closed when the event time
        (the maximum observed timestamp across the partition) passes
        the last event timestamp + timeout + grace period.
        The closed sessions cannot receive updates anymore and are considered final.

        :param closing_strategy: the strategy to use when closing sessions.
            Possible values:
              - `"key"` - messages advance time and close sessions with the same key.
              If some message keys appear irregularly in the stream, the latest sessions can remain unprocessed until a message with the same key is received.
              - `"partition"` - messages advance time and close sessions for the whole partition to which this message key belongs.
              If timestamps between keys are not ordered, it may increase the number of discarded late messages.
              Default - `"key"`.
        """
        self._closing_strategy = ClosingStrategy.new(closing_strategy)
        return super().final()

    def current(
        self, closing_strategy: ClosingStrategyValues = "key"
    ) -> "StreamingDataFrame":
        """
        Apply the session window transformation to the StreamingDataFrame to return results
        for each updated session.

        The format of returned sessions:
        ```python
        {
            "start": <session start time in milliseconds>,
            "end": <session end time in milliseconds>,
            "value: <aggregated session value>,
        }
        ```

        This method processes streaming data and returns results as they come,
        regardless of whether the session is closed or not.

        :param closing_strategy: the strategy to use when closing sessions.
            Possible values:
              - `"key"` - messages advance time and close sessions with the same key.
              If some message keys appear irregularly in the stream, the latest sessions can remain unprocessed until a message with the same key is received.
              - `"partition"` - messages advance time and close sessions for the whole partition to which this message key belongs.
              If timestamps between keys are not ordered, it may increase the number of discarded late messages.
              Default - `"key"`.
        """
        self._closing_strategy = ClosingStrategy.new(closing_strategy)
        return super().current()

    def process_window(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        transaction: WindowedPartitionTransaction,
    ) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]:
        state = transaction.as_state(prefix=key)
        timeout_ms = self._timeout_ms
        grace_ms = self._grace_ms

        collect = self.collect
        aggregate = self.aggregate

        # Determine the latest timestamp for expiration logic
        if self._closing_strategy == ClosingStrategy.PARTITION:
            latest_expired_timestamp = transaction.get_latest_expired(prefix=b"")
            latest_timestamp = max(timestamp_ms, latest_expired_timestamp)
        else:
            state_ts = state.get_latest_timestamp() or 0
            latest_timestamp = max(timestamp_ms, state_ts)

        # Calculate session expiry threshold
        session_expiry_threshold = latest_timestamp - grace_ms

        # Check if the event is too late
        if timestamp_ms < session_expiry_threshold:
            late_by_ms = session_expiry_threshold - timestamp_ms
            self._on_expired_session(
                value=value,
                key=key,
                start=timestamp_ms,
                end=timestamp_ms + timeout_ms,
                timestamp_ms=timestamp_ms,
                late_by_ms=late_by_ms,
            )
            return [], []

        # Look for an existing session that can be extended
        can_extend_session = False
        existing_aggregated = None
        old_window_to_delete = None

        # Search for active sessions that can accommodate the new event
        search_start = max(0, timestamp_ms - timeout_ms * 2)
        windows = state.get_windows(
            search_start, timestamp_ms + timeout_ms + 1, backwards=True
        )

        for (window_start, window_end), aggregated_value, _ in windows:
            # Calculate the time gap between the new event and the session's last activity
            session_last_activity = window_end - timeout_ms
            time_gap = timestamp_ms - session_last_activity

            # Check if this session can be extended
            if time_gap <= timeout_ms + grace_ms and timestamp_ms >= window_start:
                session_start = window_start
                session_end = timestamp_ms + timeout_ms
                can_extend_session = True
                existing_aggregated = aggregated_value
                old_window_to_delete = (window_start, window_end)
                break

        # If no extendable session found, start a new one
        if not can_extend_session:
            session_start = timestamp_ms
            session_end = timestamp_ms + timeout_ms

        # Process the event for this session
        updated_windows: list[WindowKeyResult] = []

        # Delete the old window if extending an existing session
        if can_extend_session and old_window_to_delete:
            old_start, old_end = old_window_to_delete
            transaction.delete_window(old_start, old_end, prefix=state._prefix)  # type: ignore # noqa: SLF001

        # Add to collection if needed
        if collect:
            state.add_to_collection(
                value=self._collect_value(value),
                id=timestamp_ms,
            )

        # Update the session window aggregation
        aggregated = None
        if aggregate:
            current_value = (
                existing_aggregated if can_extend_session else self._initialize_value()
            )

            aggregated = self._aggregate_value(current_value, value, timestamp_ms)

        # Output intermediate results for aggregations
        if aggregate:
            updated_windows.append(
                (
                    key,
                    self._results(aggregated, [], session_start, session_end),
                )
            )

        state.update_window(
            session_start, session_end, value=aggregated, timestamp_ms=timestamp_ms
        )

        # Expire old sessions
        if self._closing_strategy == ClosingStrategy.PARTITION:
            expired_windows = self.expire_sessions_by_partition(
                transaction, session_expiry_threshold, collect
            )
        else:
            expired_windows = self.expire_sessions_by_key(
                key, state, session_expiry_threshold, collect
            )

        return updated_windows, expired_windows

    def expire_sessions_by_partition(
        self,
        transaction: WindowedPartitionTransaction,
        expiry_threshold: int,
        collect: bool,
    ) -> Iterable[WindowKeyResult]:
        start = time.monotonic()
        count = 0

        # Import the parsing function to extract message keys from window keys
        from quixstreams.state.rocksdb.windowed.serialization import parse_window_key

        expired_results = []

        # Collect all keys and extract unique prefixes to avoid iteration conflicts
        all_keys = list(transaction.keys())
        seen_prefixes = set()

        for key_bytes in all_keys:
            try:
                prefix, start_ms, end_ms = parse_window_key(key_bytes)
                if prefix not in seen_prefixes:
                    seen_prefixes.add(prefix)
            except (ValueError, IndexError):
                # Skip invalid window key formats
                continue

        # Expire sessions for each unique prefix
        for prefix in seen_prefixes:
            state = transaction.as_state(prefix=prefix)
            prefix_expired = list(
                self.expire_sessions_by_key(prefix, state, expiry_threshold, collect)
            )
            expired_results.extend(prefix_expired)
            count += len(prefix_expired)

        if count:
            logger.debug(
                "Expired %s session windows in %ss",
                count,
                round(time.monotonic() - start, 2),
            )

        return expired_results

    def expire_sessions_by_key(
        self,
        key: Any,
        state: WindowedState,
        expiry_threshold: int,
        collect: bool,
    ) -> Iterable[WindowKeyResult]:
        start = time.monotonic()
        count = 0

        # Get all windows and check which ones have expired
        all_windows = list(
            state.get_windows(0, expiry_threshold + self._timeout_ms, backwards=False)
        )

        windows_to_delete = []
        for (window_start, window_end), aggregated, _ in all_windows:
            # Session expires when the session end time has passed the expiry threshold
            if window_end <= expiry_threshold:
                collected = []
                if collect:
                    collected = state.get_from_collection(window_start, window_end)

                windows_to_delete.append((window_start, window_end))
                count += 1
                yield (
                    key,
                    self._results(aggregated, collected, window_start, window_end),
                )

        # Clean up expired windows
        for window_start, window_end in windows_to_delete:
            state._transaction.delete_window(  # type: ignore # noqa: SLF001
                window_start,
                window_end,
                prefix=state._prefix,  # type: ignore # noqa: SLF001
            )
            if collect:
                state.delete_from_collection(window_end, start=window_start)

        if count:
            logger.debug(
                "Expired %s session windows in %ss",
                count,
                round(time.monotonic() - start, 2),
            )

    def _on_expired_session(
        self,
        value: Any,
        key: Any,
        start: int,
        end: int,
        timestamp_ms: int,
        late_by_ms: int,
    ) -> None:
        try:
            ctx = message_context()
            topic = ctx.topic
            partition = ctx.partition
            offset = ctx.offset
        except:
            # In test environments, message context might not be available
            topic = "unknown"
            partition = -1
            offset = -1

        to_log = True

        # Trigger the "on_late" callback if provided
        if self._on_late:
            to_log = self._on_late(
                value,
                key,
                timestamp_ms,
                late_by_ms,
                start,
                end,
                self._name,
                topic,
                partition,
                offset,
            )
        if to_log:
            logger.warning(
                "Skipping session processing for the closed session "
                f"timestamp_ms={timestamp_ms} "
                f"session={(start, end)} "
                f"late_by_ms={late_by_ms} "
                f"store_name={self._name} "
                f"partition={topic}[{partition}] "
                f"offset={offset}"
            )


class TimeWindowSingleAggregation(SingleAggregationWindowMixin, TimeWindow):
    pass


class TimeWindowMultiAggregation(MultiAggregationWindowMixin, TimeWindow):
    pass


class SessionWindowSingleAggregation(SingleAggregationWindowMixin, SessionWindow):
    pass


class SessionWindowMultiAggregation(MultiAggregationWindowMixin, SessionWindow):
    pass
