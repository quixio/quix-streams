from typing import Any, Iterable

from quixstreams.state import WindowedState

from .base import WindowResult
from .time_based import FixedTimeWindow


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
        latest_timestamp = max(timestamp_ms, state.get_latest_timestamp())
        expiration_max_start_time = latest_timestamp - duration - grace - 1
        deletion_max_start_time = None

        left_start = max(0, timestamp_ms - duration)
        left_end = timestamp_ms

        right_start = timestamp_ms + 1
        right_end = right_start + duration
        right_exists = False

        starts = set([left_start])
        updated_windows = []
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
                        updated_windows.append(window)
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
                    updated_windows.append(window)

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
                updated_windows.append(
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
                    and right_start > expiration_max_start_time
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
                updated_windows.append(
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
                deletion_max_start_time = start - 1
                break

        else:
            # Create the left window as iteration completed without creating (or updating) it
            updated_windows.append(
                self._update_window(
                    state=state,
                    start=left_start,
                    end=left_end,
                    value=aggregate(default, value),
                    timestamp=timestamp_ms,
                    window_timestamp=timestamp_ms,
                )
            )

        expired_windows = [
            {"start": start, "end": end, "value": self._merge_func(aggregation)}
            for (start, end), (max_timestamp, aggregation) in state.expire_windows(
                max_start_time=expiration_max_start_time,
                delete=False,
            )
            if end == max_timestamp  # Include only left windows
        ]

        if deletion_max_start_time is not None:
            deletion_max_start_time = min(
                deletion_max_start_time, expiration_max_start_time
            )
        else:
            deletion_max_start_time = expiration_max_start_time - duration
        state.delete_windows(max_start_time=deletion_max_start_time)

        return reversed(updated_windows), expired_windows

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
