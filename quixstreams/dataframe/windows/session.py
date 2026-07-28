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

# Sentinel stored as the partition expiry checkpoint when the partition holds no
# open session at all: no watermark can ever reach it, so the sweep is skipped
# until the next session is written. Matches the 8-byte big-endian encoding used
# for window timestamps in RocksDB, so it round-trips through the state store.
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
    closes once the watermark passes `last event + 2 * gap + grace`. The extra
    `gap` in the closing rule is what keeps the two rules consistent: an
    admissible event may arrive with a timestamp as low as
    `watermark - gap - grace` and extend a session whose last event is up to one
    gap before it, so a session may only close once no admissible event can
    reach it any more. `grace_ms` plays no part in assigning events to sessions
    - it only delays closing - which is what makes the default `grace_ms=0`
    safe: it still leaves a full inactivity gap of out-of-order tolerance.
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
        # `before_update` / `after_update` are accepted to satisfy the common
        # window constructor contract, but session windows do not support
        # trigger callbacks yet: a bridging event has no single "current value"
        # to offer `before_update` (it may match two sessions), and a forced
        # early close breaks the "maximal, disjoint, non-adjacent" guarantee of
        # the emitted sessions. `SessionWindowDefinition` rejects non-None
        # callbacks at build time, mirroring the sliding-window guard.
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

        # 1. Advance the watermark of the active scope. It is monotonic and
        #    persisted, so a key that goes silent keeps its watermark and the
        #    partition scope keeps a clock that is independent of any single key.
        if by_partition:
            # The current key's persisted watermark is folded in so that
            # history recorded under `closing_strategy="key"` keeps counting
            # after a switch to `"partition"`: the partition slot starts at 0,
            # and without the fold a replayed event could land inside a session
            # that was already emitted under the key strategy and produce an
            # overlapping duplicate. In steady partition-mode operation the
            # fold is a no-op, because every per-key watermark was itself
            # already observed through this very slot.
            watermark = transaction.advance_partition_timestamp(
                max(timestamp_ms, state.get_latest_timestamp() or 0)
            )
        else:
            watermark = max(timestamp_ms, state.get_latest_timestamp() or 0)

        # Events below `late_before` are dropped as late; sessions with
        # `end <= close_before` are closed. The two thresholds are one `gap`
        # apart: an admissible event (`ts >= late_before`) can still extend a
        # session whose last event is at most one gap before it, so a session
        # may only close once even the earliest admissible event can no longer
        # reach it - `last + gap < late_before`, i.e. `end <= late_before - gap`.
        # Sharing a single threshold would let an event at exactly
        # `ts == late_before` be accepted after its session was closed and open
        # a second session less than one gap after it, breaking the
        # non-adjacency guarantee documented above.
        late_before = watermark - gap - grace
        close_before = late_before - gap

        # 2. Lateness. Note that `gap` is part of the formula: with grace_ms=0
        #    there is still a full gap of out-of-order tolerance.
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

        # 3. Probe the two immediate neighbours of `timestamp_ms` in start order.
        #    Because stored sessions are disjoint and more than one gap apart, at
        #    most those two can match - see the proof in the spec (section 4.4).
        #    RocksDB orders window keys by (prefix, start, end), so each probe is a
        #    single O(log n) seek.
        previous = next(
            state.iter_windows(start_to_ms=timestamp_ms, backwards=True), None
        )
        following = next(state.iter_windows(start_from_ms=timestamp_ms + 1), None)

        matched: list[WindowDetail] = []
        for candidate in (previous, following):
            if candidate is not None and self._matches(candidate, timestamp_ms, gap):
                matched.append(candidate)

        # 4. Assign / extend / merge. `matched` is ordered [previous, following],
        #    i.e. earlier-start first, which is exactly the (a, b) contract of
        #    `BaseAggregator.merge()`.
        if len(matched) == 2:
            (previous_start, previous_end), previous_agg, _ = matched[0]
            (following_start, following_end), following_agg, _ = matched[1]
            session_start = min(previous_start, timestamp_ms)
            session_end = max(following_end, timestamp_ms + 1)
            state.delete_window(previous_start, previous_end)
            state.delete_window(following_start, following_end)
            if aggregate:
                # A session persisted by a collect-only window stores `None`.
                # Treat it as "not initialized yet" - like `FixedTimeWindow`
                # does - so that adding an aggregation to an existing store
                # re-aggregates instead of crashing on the stored `None`.
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
                # The RocksDB key encodes (start, end), so a resized session is
                # written under a new key and the old one must be removed.
                state.delete_window(matched_start, matched_end)
            if aggregate:
                # See the `None` note in the two-match branch above.
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

        # A configuration change across a restart (e.g. a larger `grace_ms`)
        # can make an event admissible below this key's persisted expiry
        # cursor and write a session that `expire_by_key`'s
        # `scan_from = cursor + 1` would never see again. Re-lower the cursor
        # so the new session stays visible to expiry. Within one configuration
        # this never triggers: accepted events always sort above every expired
        # session (see the cursor note in `expire_by_key`).
        cursor = state.get_expiry_checkpoint()
        if cursor is not None and session_start <= cursor:
            state.set_expiry_checkpoint(session_start - 1)

        updated_windows: list[WindowKeyResult] = []
        if aggregate:
            updated_windows.append(
                (key, self._results(aggregated, [], session_start, session_end))
            )

        # 5. Expire.
        expired_windows: list[WindowKeyResult]
        if by_partition:
            # Lower the partition checkpoint so that a brand-new key cannot be
            # missed by the gate below. An unset (`None`) checkpoint already
            # means "sweep unconditionally" and must stay unset: replacing it
            # with this session's own candidate - which always exceeds this
            # message's watermark - would gate the very sweep this call is
            # about to run and defer already-due sessions of other keys.
            # See `expire_by_partition`.
            checkpoint = transaction.get_expiry_checkpoint()
            expiry_candidate = session_end + 2 * gap + grace
            if checkpoint is not None and expiry_candidate < checkpoint:
                transaction.set_expiry_checkpoint(expiry_candidate)
            expired_windows = self.expire_by_partition(
                transaction, watermark, close_before, collect
            )
        else:
            expired_windows = self.expire_by_key(key, state, close_before, collect)

        return updated_windows, expired_windows

    @staticmethod
    def _matches(window: WindowDetail, timestamp_ms: int, gap: int) -> bool:
        """
        An event at `timestamp_ms` belongs to a stored session `[start, end)` iff
        it is no more than one inactivity gap away from either of its boundaries:
        `start - gap <= ts <= (end - 1) + gap`.
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
        The cost is therefore `O(log n + expired)` rather than `O(windows of key)`.

        Returns a materialised list with the deletes already applied, so callers
        do not have to drain a generator to trigger its side effects.
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

        # The cursor is the start of the last expired session. Within one
        # configuration it can never skip a live session: any accepted event
        # has `ts >= close_before + gap > expired.last`, so no new or resized
        # session can start at or below it. If a configuration change (e.g. a
        # larger `grace_ms`) makes an older event admissible again,
        # `process_window` re-lowers the cursor when it writes below it.
        state.set_expiry_checkpoint(closing[-1][0][0])
        return results

    def expire_by_partition(
        self,
        transaction: WindowedPartitionTransaction,
        watermark: int,
        close_before: int,
        collect: bool,
    ) -> list[WindowKeyResult]:
        """
        Close the due sessions of every key in the partition.

        The sweep is gated by a persisted checkpoint - the earliest watermark value
        at which some session in the partition may close - so the common
        per-message cost is `O(1)`. An unset checkpoint means "sweep
        unconditionally". Between sweeps the checkpoint is only ever lowered,
        and each sweep recomputes it exactly, so it is always at or below the
        true minimum: a due close is never skipped, and a stale checkpoint
        costs one extra sweep rather than a wrong result.

        >***NOTE:*** A sweep itself costs one pass over the stored window keys
        (to enumerate prefixes) plus one seek per prefix (to expire). There is
        no cross-prefix ordering by window end in the primary key space, so no
        seek trick avoids it; the gate amortises it over a gap's worth of event
        time in the common "few keys, many events" case. A `(end, prefix, start)`
        index column family is the follow-up if that is ever not enough.
        """
        checkpoint = transaction.get_expiry_checkpoint()
        if checkpoint is not None and watermark < checkpoint:
            return []

        # A session with end `E` may close once `watermark - 2 * gap - grace
        # >= E` (see the threshold note in `process_window`), so the earliest
        # watermark at which it can close is `E + 2 * gap + grace`.
        expire_after = 2 * self._inactivity_gap_ms + self._grace_ms
        results: list[WindowKeyResult] = []
        next_checkpoint = _NO_OPEN_SESSIONS

        for prefix in transaction.iter_prefixes():
            state = transaction.as_state(prefix=prefix)
            results.extend(self.expire_by_key(prefix, state, close_before, collect))

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
