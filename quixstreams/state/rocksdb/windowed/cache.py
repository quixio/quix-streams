import bisect
from typing import Dict, List, Set, Generator, Tuple


class WindowExpirationCache:
    def __init__(self, grace_period: float):
        self._windows_by_expiration: Dict[
            bytes, Dict[float, Set[Tuple[float, float]]]
        ] = {}
        self._expiration_timestamps: Dict[bytes, List[float]] = {}
        self._grace_period = grace_period

    def add(self, message_key: bytes, start: float, end: float):
        windows_by_timestamps = self._windows_by_expiration.setdefault(message_key, {})
        expire_at = end + self._grace_period
        if expire_at not in windows_by_timestamps:
            windows_by_timestamps[expire_at] = {(start, end)}
            expiration_timestamps = self._expiration_timestamps.setdefault(
                message_key, []
            )
            bisect.insort_left(expiration_timestamps, expire_at)
        else:
            windows_by_timestamps[expire_at].add((start, end))

    def get_expired(
        self, message_key: bytes, now: float
    ) -> Generator[Tuple[float, float], None, None]:
        expiration_timestamps = self._expiration_timestamps.get(message_key, [])
        windows = self._windows_by_expiration.get(message_key, {})
        for timestamp in expiration_timestamps:
            if timestamp > now:
                break
            for window in windows[timestamp]:
                yield window

    def expire(self, message_key: bytes, now: float):
        heap = self._expiration_timestamps.get(message_key, [])
        timestamps = self._windows_by_expiration.get(message_key, {})
        while heap:
            timestamp = heap[0]
            if timestamp > now:
                break
            heap.pop(0)
            timestamps.pop(timestamp, None)
