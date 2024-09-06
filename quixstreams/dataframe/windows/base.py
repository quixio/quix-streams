from collections import deque
from typing import Any, Deque, Optional, Callable, Tuple

from typing_extensions import TypedDict

from quixstreams.state import WindowedState


class WindowResult(TypedDict):
    start: int
    end: int
    value: Any


WindowAggregateFunc = Callable[[int, int, int, Any, WindowedState], Any]
WindowMergeFunc = Callable[[Any], Any]


def get_window_ranges(
    timestamp_ms: int, duration_ms: int, step_ms: Optional[int] = None
) -> Deque[Tuple[int, int]]:
    """
    Get a list of window ranges for the given timestamp.
    :param timestamp_ms: timestamp in milliseconds
    :param duration_ms: window duration in milliseconds
    :param step_ms: window step in milliseconds for hopping windows, optional.
    :return: a list of (<start>, <end>) tuples
    """
    if not step_ms:
        step_ms = duration_ms

    window_ranges = deque()
    current_window_start = timestamp_ms - (timestamp_ms % step_ms)

    while (
        current_window_start > timestamp_ms - duration_ms and current_window_start >= 0
    ):
        window_end = current_window_start + duration_ms
        window_ranges.appendleft((current_window_start, window_end))
        current_window_start -= step_ms

    return window_ranges
