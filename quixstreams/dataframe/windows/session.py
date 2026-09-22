from typing import TYPE_CHECKING, Any, Iterable, Optional

from quixstreams.state import (
    WindowDetail,
    WindowedPartitionTransaction,
    WindowedState,
)

from .base import (
    MultiAggregationWindowMixin,
    SingleAggregationWindowMixin,
    WindowAfterUpdateCallback,
    WindowBeforeUpdateCallback,
    WindowKeyResult,
    WindowOnLateCallback,
)
from .time_based import ClosingStrategy, TimeWindow

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

# Partition expiry checkpoint written when the partition holds no open session;
# no watermark can reach it.
_NO_OPEN_SESSIONS = 2**64 - 1


class SessionWindow(TimeWindow):
    """
    Session window groups events separated by no more than `inactivity_gap_ms`.

    A session starts with the first event and extends every time another event
    arrives within `inactivity_gap_ms` of the session's boundaries. For a given
    message key the stored sessions are always **maximal, disjoint and
    non-adjacent**: no two events more than one gap apart share a session, and two
    consecutive sessions are always more than one gap apart. An out-of-order event
    that falls within one gap of two open sessions **merges** them.

    Sessions are stored and emitted half-open, `[start, end)`, like every other
    window type: `end` is the timestamp of the last event plus one.

    An event is late only when `ts < watermark - gap - grace`, and a session
    closes once the watermark passes `last event + 2 * gap + grace`. The two
    bounds differ by one gap because the earliest admissible event, arriving at
    `watermark - gap - grace`, can still extend a session whose last event is one
    gap before it. `grace_ms` only delays closing; it never affects which session
    an event joins.
    """

    def __init__(
        self,
        inactivity_gap_ms: int,
        grace_ms: int,
        name: str,
        dataframe: "StreamingDataFrame",
        on_late: Optional[WindowOnLateCallback] = None,
        before_update: Optional[WindowBeforeUpdateCallback] = None,
        after_update: Optional[WindowAfterUpdateCallback] = None,
    ):
        super().__init__(
            name=name,
            dataframe=dataframe,
            on_late=on_late,
            before_update=before_update,
            after_update=after_update,
        )

        self._inactivity_gap_ms = inactivity_gap_ms
        self._grace_ms = grace_ms

    def process_window(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        headers: Any,
        transaction: WindowedPartitionTransaction,
    ) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]:
        state = transaction.as_state(prefix=key)
        gap = self._inactivity_gap_ms
        grace = self._grace_ms

        collect = self.collect
        aggregate = self.aggregate

        by_partition = self._closing_strategy == ClosingStrategy.PARTITION

        # Advance the watermark of the active scope.
        if by_partition:
            # Fold in this key's own watermark so history recorded under
            # `closing_strategy="key"` keeps counting after a switch to
            # `"partition"`, whose slot starts at 0.
            watermark = transaction.advance_partition_timestamp(
                max(timestamp_ms, state.get_latest_timestamp() or 0)
            )
        else:
            watermark = max(timestamp_ms, state.get_latest_timestamp() or 0)

        late_before = watermark - gap - grace
        close_before = late_before - gap

        # Drop late events.
        if timestamp_ms < late_before:
            self._on_expired_window(
                value=value,
                key=key,
                start=timestamp_ms,
                end=timestamp_ms + 1,
                timestamp_ms=timestamp_ms,
                late_by_ms=late_before - timestamp_ms,
            )
            return [], []

        # Probe the two immediate neighbours in start order. Stored sessions are
        # disjoint and more than one gap apart, so at most those two can match.
        previous = next(
            state.iter_windows(start_to_ms=timestamp_ms, backwards=True), None
        )
        following = next(state.iter_windows(start_from_ms=timestamp_ms + 1), None)

        matched: list[WindowDetail] = []
        for candidate in (previous, following):
            if candidate is not None and self._matches(candidate, timestamp_ms, gap):
                matched.append(candidate)

        # Assign, extend or merge. `matched` is ordered earlier-start first,
        # which is the `(a, b)` contract of `BaseAggregator.merge()`.
        if len(matched) == 2:
            (previous_start, previous_end), previous_agg, _ = matched[0]
            (following_start, following_end), following_agg, _ = matched[1]
            session_start = min(previous_start, timestamp_ms)
            session_end = max(following_end, timestamp_ms + 1)
            state.delete_window(previous_start, previous_end)
            state.delete_window(following_start, following_end)
            if aggregate:
                # A session persisted by a collect-only window stores `None`;
                # treat it as not initialized yet, like `FixedTimeWindow` does.
                if previous_agg is None:
                    previous_agg = self._initialize_value()
                if following_agg is None:
                    following_agg = self._initialize_value()
                aggregated = self._merge_values(
                    self._aggregate_value(previous_agg, value, timestamp_ms),
                    following_agg,
                )
            else:
                aggregated = None
        elif len(matched) == 1:
            (matched_start, matched_end), matched_agg, _ = matched[0]
            session_start = min(matched_start, timestamp_ms)
            session_end = max(matched_end, timestamp_ms + 1)
            if (session_start, session_end) != (matched_start, matched_end):
                # The store key encodes (start, end), so a resized session is
                # written under a new key and the old one must be removed.
                state.delete_window(matched_start, matched_end)
            if aggregate:
                if matched_agg is None:
                    matched_agg = self._initialize_value()
                aggregated = self._aggregate_value(matched_agg, value, timestamp_ms)
            else:
                aggregated = None
        else:
            session_start, session_end = timestamp_ms, timestamp_ms + 1
            aggregated = (
                self._aggregate_value(self._initialize_value(), value, timestamp_ms)
                if aggregate
                else None
            )

        if collect:
            state.add_to_collection(value=self._collect_value(value), id=timestamp_ms)

        state.update_window(
            session_start, session_end, value=aggregated, timestamp_ms=timestamp_ms
        )

        # A larger `grace_ms` across a restart can admit an event below this key's
        # expiry cursor, where `expire_by_key`'s scan would never see it again.
        cursor = state.get_expiry_checkpoint()
        if cursor is not None and session_start <= cursor:
            state.set_expiry_checkpoint(session_start - 1)

        updated_windows: list[WindowKeyResult] = []
        if aggregate:
            updated_windows.append(
                (key, self._results(aggregated, [], session_start, session_end))
            )

        # Close what is due.
        expired_windows: list[WindowKeyResult]
        if by_partition:
            # An unset checkpoint means "sweep unconditionally", so the
            # checkpoint is only ever lowered - see `expire_by_partition`.
            checkpoint = transaction.get_expiry_checkpoint()
            expiry_candidate = session_end + 2 * gap + grace
            if checkpoint is not None and expiry_candidate < checkpoint:
                transaction.set_expiry_checkpoint(expiry_candidate)
            expired_windows = self.expire_by_partition(
                transaction, watermark, close_before, collect, key
            )
        else:
            expired_windows = self.expire_by_key(key, state, close_before, collect)

        return updated_windows, expired_windows

    @staticmethod
    def _matches(window: WindowDetail, timestamp_ms: int, gap: int) -> bool:
        """
        An event belongs to a stored session when it is no more than one
        inactivity gap away from either of the session's boundaries.
        """
        (start, end), _, _ = window
        return start - gap <= timestamp_ms and end + gap > timestamp_ms

    def expire_by_key(
        self,
        key: Any,
        state: WindowedState,
        close_before: int,
        collect: bool,
    ) -> list[WindowKeyResult]:
        """
        Close every session of a single key whose `end <= close_before`.

        Sessions of one key are disjoint, so their `end` increases along start
        order and the closable ones form a prefix of that order: the scan starts at
        the persisted cursor and stops at the first session that is still open.

        :param key: The message key whose sessions are closed.
        :param state: The windowed state scoped to `key`.
        :param close_before: Sessions ending at or below this value are closed.
        :param collect: If True, collected values are attached and then deleted.
        :return: The closed sessions, with their deletes already applied.
        """
        cursor = state.get_expiry_checkpoint()
        scan_from = 0 if cursor is None else cursor + 1

        closing: list[tuple[tuple[int, int], Any]] = []
        for (start, end), aggregated, _ in state.iter_windows(start_from_ms=scan_from):
            if end > close_before:
                break
            closing.append(((start, end), aggregated))

        if not closing:
            return []

        results: list[WindowKeyResult] = []
        for (start, end), aggregated in closing:
            collected = state.get_from_collection(start, end) if collect else []
            results.append((key, self._results(aggregated, collected, start, end)))

        for (start, end), _ in closing:
            state.delete_window(start, end)
            if collect:
                state.delete_from_collection(end=end, start=start)

        # The cursor is the start of the last expired session; `process_window`
        # re-lowers it whenever it writes a session at or below it.
        state.set_expiry_checkpoint(closing[-1][0][0])
        return results

    def expire_by_partition(
        self,
        transaction: WindowedPartitionTransaction,
        watermark: int,
        close_before: int,
        collect: bool,
        message_key: Any,
    ) -> list[WindowKeyResult]:
        """
        Close the due sessions of every key in the partition.

        A persisted checkpoint - the earliest watermark value at which some session
        in the partition may close - gates the sweep, so the common per-message
        cost is `O(1)`. An unset checkpoint means "sweep unconditionally". Between
        sweeps the checkpoint is only ever lowered, and each sweep recomputes it.

        >***NOTE:*** A sweep itself costs one pass over the stored window keys to
        enumerate prefixes, plus one seek per prefix to expire.

        :param transaction: The windowed partition transaction to sweep.
        :param watermark: The current partition watermark.
        :param close_before: Sessions ending at or below this value are closed.
        :param collect: If True, collected values are attached and then deleted.
        :param message_key: The key of the record being processed, passed to
            `key_from_prefix()` to map each swept prefix back to a message key.
        :return: The closed sessions of every key, with their deletes applied.
        """
        checkpoint = transaction.get_expiry_checkpoint()
        if checkpoint is not None and watermark < checkpoint:
            return []

        expire_after = 2 * self._inactivity_gap_ms + self._grace_ms
        results: list[WindowKeyResult] = []
        next_checkpoint = _NO_OPEN_SESSIONS

        for prefix in transaction.iter_prefixes():
            state = transaction.as_state(prefix=prefix)
            key = transaction.key_from_prefix(prefix, message_key)
            results.extend(self.expire_by_key(key, state, close_before, collect))

            cursor = state.get_expiry_checkpoint()
            scan_from = 0 if cursor is None else cursor + 1
            first_open = next(state.iter_windows(start_from_ms=scan_from), None)
            if first_open is not None:
                (_, end), _, _ = first_open
                next_checkpoint = min(next_checkpoint, end + expire_after)

        transaction.set_expiry_checkpoint(next_checkpoint)
        return results


class SessionWindowSingleAggregation(SingleAggregationWindowMixin, SessionWindow):
    pass


class SessionWindowMultiAggregation(MultiAggregationWindowMixin, SessionWindow):
    pass
