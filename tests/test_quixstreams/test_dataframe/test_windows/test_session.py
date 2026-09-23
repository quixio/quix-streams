from typing import Optional
from unittest.mock import Mock, patch

import pytest

import quixstreams.dataframe.windows.aggregations as agg
from quixstreams.core.stream.exceptions import InvalidOperation
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.dataframe.windows.base import WindowOnLateCallback
from quixstreams.dataframe.windows.definitions import (
    SessionWindowDefinition,
    TumblingTimeWindowDefinition,
)
from quixstreams.dataframe.windows.time_based import ClosingStrategy
from quixstreams.state.rocksdb.partition import RocksDBStorePartition
from quixstreams.state.rocksdb.windowed.transaction import (
    WindowedRocksDBPartitionTransaction,
)


@pytest.fixture()
def session_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        inactivity_gap_ms: int,
        grace_ms: int = 0,
        on_late: Optional[WindowOnLateCallback] = None,
    ) -> SessionWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = SessionWindowDefinition(
            inactivity_gap_ms=inactivity_gap_ms,
            grace_ms=grace_ms,
            dataframe=sdf,
            on_late=on_late,
        )
        return window_def

    return factory


def process(window, value, key, transaction, timestamp_ms, headers=None):
    updated, expired = window.process_window(
        value=value,
        key=key,
        timestamp_ms=timestamp_ms,
        headers=headers,
        transaction=transaction,
    )
    return list(updated), list(expired)


def stored_windows(transaction, key):
    """
    Read every window currently stored for `key` as a sorted list of
    `(start, end, raw_aggregation_state)` tuples.

    `start_from_ms=-1` is required because `get_windows()` has an *exclusive*
    lower bound, so `0` would hide a session starting at timestamp 0.
    """
    return sorted(
        (start, end, value)
        for (start, end), value, _ in transaction.get_windows(
            start_from_ms=-1, start_to_ms=10**12, prefix=key
        )
    )


def _count_rows_read_by_iter_prefixes(store):
    """
    Open a *fresh* transaction (so no result can be served from an
    uncommitted update-cache) and consume `iter_prefixes()`, counting every
    row `RocksDBStorePartition.iter_items` yields while it runs.

    Counting yielded rows (not call counts) is what makes this immune to a
    rewrite that swaps `keys()`/`get_windows()` for a different scanning
    primitive: any implementation that reads every window row in the
    column family shows up here, regardless of which method does the
    reading.
    """
    original_iter_items = RocksDBStorePartition.iter_items
    rows_read = 0

    def counting_iter_items(self, *args, **kwargs):
        nonlocal rows_read
        for item in original_iter_items(self, *args, **kwargs):
            rows_read += 1
            yield item

    with patch.object(RocksDBStorePartition, "iter_items", counting_iter_items):
        with store.start_partition_transaction(0) as tx:
            list(tx.iter_prefixes())
    return rows_read


class NonMergeableSum(agg.Aggregator):
    """
    A user-defined aggregator that does not implement `merge()`.
    """

    def initialize(self) -> int:
        return 0

    def agg(self, old: int, new: int, timestamp: int) -> int:
        return old + new

    def result(self, value: int) -> int:
        return value


# Hard-coded arrival order for the session invariant test. Mixes in-order and
# out-of-order arrivals; every out-of-order event stays within the grace period
# so none of them may be dropped as late.
INVARIANT_EVENTS = [
    (1000, b"k1"),
    (2000, b"k2"),
    (6000, b"k1"),
    (5000, b"k2"),
    (11000, b"k1"),
    (9000, b"k2"),
    (16000, b"k1"),
    (30000, b"k2"),
    (21000, b"k1"),
    (25000, b"k2"),
    (26000, b"k1"),
    (50000, b"k2"),
    (31000, b"k1"),
    (54000, b"k2"),
    (3000, b"k1"),
    (58000, b"k2"),
    (40000, b"k1"),
    (47000, b"k2"),
    (36000, b"k1"),
    (100000, b"k2"),
    (50000, b"k1"),
    (104000, b"k2"),
    (45000, b"k1"),
    (96000, b"k2"),
    (60000, b"k1"),
    (150000, b"k2"),
    (70000, b"k1"),
    (154000, b"k2"),
    (66000, b"k1"),
    (145000, b"k2"),
    (80000, b"k1"),
    (200000, b"k2"),
    (90000, b"k1"),
    (204000, b"k2"),
    (85000, b"k1"),
    (195000, b"k2"),
    (100000, b"k1"),
    (250000, b"k2"),
    (95000, b"k1"),
    (254000, b"k2"),
]


class TestSessionWindow:
    @pytest.mark.parametrize(
        "timeout, grace, provided_name, func_name, expected_name",
        [
            (
                30000,
                5000,
                "custom_window",
                "sum",
                "custom_window_session_window_30000_sum",
            ),
            (30000, 5000, None, "sum", "session_window_30000_sum"),
            (15000, 5000, None, "count", "session_window_15000_count"),
        ],
    )
    def test_session_window_definition_get_name(
        self,
        timeout,
        grace,
        provided_name,
        func_name,
        expected_name,
        dataframe_factory,
    ):
        swd = SessionWindowDefinition(
            inactivity_gap_ms=timeout,
            grace_ms=grace,
            dataframe=dataframe_factory(),
            name=provided_name,
        )
        name = swd._get_name(func_name)
        assert name == expected_name

    def test_multiaggregation(
        self,
        session_window_definition_factory,
        state_manager,
    ):
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        ).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Mean(),
            max=agg.Max(),
            min=agg.Min(),
            collect=agg.Collect(),
        )
        window.final(closing_strategy="key")
        assert window.name == "session_window_10000"

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"
        with store.start_partition_transaction(0) as tx:
            # First event starts a session
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 1000,
                        "end": 1001,  # exclusive: last event ts + 1
                        "count": 1,
                        "sum": 1,
                        "mean": 1.0,
                        "max": 1,
                        "min": 1,
                        "collect": [],
                    },
                )
            ]

            # Second event within timeout extends the session
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=5000
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 1000,
                        "end": 5001,  # exclusive: last event ts + 1
                        "count": 2,
                        "sum": 5,
                        "mean": 2.5,
                        "max": 4,
                        "min": 1,
                        "collect": [],
                    },
                )
            ]

            # Third event outside timeout starts new session, expires previous.
            # A session closes once the watermark passes end + 2*gap + grace
            # (1000 + 5001 + 2*10000 + 1000 = 26001 -> wait: end(5001) + 2*gap
            # (20000) + grace(1000) = 26001), so the watermark-advancing event
            # is pushed to 26001 (was 26000) to still close it here.
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=26001
            )
            assert expired == [
                (
                    key,
                    {
                        "start": 1000,
                        "end": 5001,  # exclusive: last event ts + 1
                        "count": 2,
                        "sum": 5,
                        "mean": 2.5,
                        "max": 4,
                        "min": 1,
                        "collect": [1, 4],
                    },
                )
            ]
            assert updated == [
                (
                    key,
                    {
                        "start": 26001,
                        "end": 26002,  # exclusive: last event ts + 1
                        "count": 1,
                        "sum": 2,
                        "mean": 2.0,
                        "max": 2,
                        "min": 2,
                        "collect": [],
                    },
                )
            ]

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_sessionwindow_count(
        self, expiration, session_window_definition_factory, state_manager
    ):
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.count()
        assert window.name == "session_window_10000_count"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            # Start session
            process(window, value=0, key=key, transaction=tx, timestamp_ms=1000)
            # Add to session
            updated, expired = process(
                window, value=0, key=key, transaction=tx, timestamp_ms=5000
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 2
        assert updated[0][1]["start"] == 1000
        assert updated[0][1]["end"] == 5001  # exclusive: last event ts + 1
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_sessionwindow_sum(
        self, expiration, session_window_definition_factory, state_manager
    ):
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.sum()
        assert window.name == "session_window_10000_sum"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=1000)
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=5000
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 5
        assert updated[0][1]["start"] == 1000
        assert updated[0][1]["end"] == 5001  # exclusive: last event ts + 1
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_sessionwindow_mean(
        self, expiration, session_window_definition_factory, state_manager
    ):
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.mean()
        assert window.name == "session_window_10000_mean"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=1000)
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=5000
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 3.0
        assert updated[0][1]["start"] == 1000
        assert updated[0][1]["end"] == 5001  # exclusive: last event ts + 1
        assert not expired

    def test_sessionwindow_reduce_without_merger_raises(
        self, session_window_definition_factory
    ):
        """
        `Reduce`'s reducer is `(accumulator, raw_value) -> accumulator`, not
        `(R, R) -> R`, so it cannot double as a merger. Without an explicit
        `merger=`, building a session window on top of `reduce()` raises
        `InvalidOperation` instead of silently producing wrong merges later.
        """
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        with pytest.raises(InvalidOperation, match="do not implement `merge`"):
            window_def.reduce(
                reducer=lambda agg, current: agg + [current],
                initializer=lambda value: [value],
            )

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_sessionwindow_reduce_with_merger(
        self, expiration, session_window_definition_factory, state_manager
    ):
        """
        Passing `merger=` makes `Reduce` mergeable, so
        `session_window(...).reduce(...)` keeps building and running.
        """
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
            merger=lambda a, b: a + b,
        )
        assert window.name == "session_window_10000_reduce"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=1000)
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=5000
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == [2, 3]
        assert updated[0][1]["start"] == 1000
        assert updated[0][1]["end"] == 5001  # exclusive: last event ts + 1
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_sessionwindow_max(
        self, expiration, session_window_definition_factory, state_manager
    ):
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.max()
        assert window.name == "session_window_10000_max"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=1000)
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=5000
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 5
        assert updated[0][1]["start"] == 1000
        assert updated[0][1]["end"] == 5001  # exclusive: last event ts + 1
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_sessionwindow_min(
        self, expiration, session_window_definition_factory, state_manager
    ):
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.min()
        assert window.name == "session_window_10000_min"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=5, key=key, transaction=tx, timestamp_ms=1000)
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=5000
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 2
        assert updated[0][1]["start"] == 1000
        assert updated[0][1]["end"] == 5001  # exclusive: last event ts + 1
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_sessionwindow_collect(
        self, expiration, session_window_definition_factory, state_manager
    ):
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.collect()
        assert window.name == "session_window_10000_collect"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=1, key=key, transaction=tx, timestamp_ms=1000)
            process(window, value=2, key=key, transaction=tx, timestamp_ms=5000)
            process(window, value=3, key=key, transaction=tx, timestamp_ms=8000)
            # Event outside timeout triggers session closure. A session
            # closes once the watermark passes end + 2*gap + grace
            # (8001 + 20000 + 1000 = 29001), so the watermark-advancing
            # event is pushed to 29001 (was 25000) to still close it here.
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=29001
            )
        assert not updated
        assert expired == [(key, {"start": 1000, "end": 8001, "value": [1, 2, 3]})]

    def test_session_end_is_exclusive(
        self, session_window_definition_factory, state_manager
    ):
        """
        A stored session's `end` is exclusive (`last_event_ts + 1`), matching
        every other window type.
        """
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=0
        ).sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"
        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=1000)
            updated, _ = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=5000
            )
        assert updated[0][1]["end"] == 5001

    def test_session_end_is_exclusive_collect_no_leaked_values(
        self, session_window_definition_factory, state_manager
    ):
        """
        The `collect()` variant of the exclusive `end`: `delete_from_collection`
        deletes exactly `[start, end)`, so the session's last collected value is
        not leaked.

        A second, unrelated key advances the partition watermark so the
        triggering event does not itself add a value under the expiring
        key's prefix - which lets the assertion be a literal "no values left
        under this key's prefix" rather than "minus the one value that
        naturally belongs to the still-open triggering session".
        """
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=0
        ).collect()
        window.final(closing_strategy="partition")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"
        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=1000)
            process(window, value=2, key=key, transaction=tx, timestamp_ms=5000)
            process(window, value=3, key=key, transaction=tx, timestamp_ms=8000)
            _, expired = process(
                window, value=4, key=b"other_key", transaction=tx, timestamp_ms=100000
            )
            remaining_values = tx.get_from_collection(start=-1, end=10**12, prefix=key)
        assert expired == [(key, {"start": 1000, "end": 8001, "value": [1, 2, 3]})]
        assert remaining_values == []

    @pytest.mark.parametrize(
        "timeout, grace, name",
        [
            (-10000, 1000, "test"),  # timeout < 0
            (10000, -1000, "test"),  # grace < 0
            (0, 1000, "test"),  # timeout == 0
        ],
    )
    def test_session_window_def_init_invalid(
        self, timeout, grace, name, dataframe_factory
    ):
        with pytest.raises(ValueError):
            SessionWindowDefinition(
                inactivity_gap_ms=timeout,
                grace_ms=grace,
                name=name,
                dataframe=dataframe_factory(),
            )

    def test_session_window_def_init_invalid_type(self, dataframe_factory):
        with pytest.raises(TypeError):
            SessionWindowDefinition(
                inactivity_gap_ms="invalid",  # should be int
                grace_ms=1000,
                name="test",
                dataframe=dataframe_factory(),
            )

    def test_session_window_def_init_invalid_grace_type(self, dataframe_factory):
        """
        `grace_ms` gets the same `isinstance` check as `inactivity_gap_ms`.
        """
        with pytest.raises(TypeError):
            SessionWindowDefinition(
                inactivity_gap_ms=10000,
                grace_ms="invalid",  # should be int
                name="test",
                dataframe=dataframe_factory(),
            )

    @pytest.mark.parametrize("callback_kwarg", ["before_update", "after_update"])
    def test_session_window_def_init_rejects_trigger_callbacks(
        self, callback_kwarg, dataframe_factory
    ):
        """
        Session windows do not support trigger callbacks (`before_update` /
        `after_update`), mirroring the sliding-window rejection at
        `definitions.py` (introduced by PR #1044, "Early Window Expiration
        with Triggers"). `sdf.session_window()` doesn't expose these kwargs,
        so `SessionWindowDefinition` is constructed directly here.
        """
        with pytest.raises(ValueError, match="trigger callbacks"):
            SessionWindowDefinition(
                inactivity_gap_ms=10000,
                grace_ms=0,
                name="test",
                dataframe=dataframe_factory(),
                **{callback_kwarg: lambda *args: False},
            )

    def test_session_window_def_init_builds_without_trigger_callbacks(
        self, dataframe_factory
    ):
        """A session window still builds normally when both `before_update`
        and `after_update` are left `None` (the default)."""
        window_def = SessionWindowDefinition(
            inactivity_gap_ms=10000,
            grace_ms=0,
            name="test",
            dataframe=dataframe_factory(),
            before_update=None,
            after_update=None,
        )
        window = window_def.sum()
        assert window.name == "test_session_window_10000_sum"

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_session_window_process_timeout_behavior(
        self,
        expiration,
        session_window_definition_factory,
        state_manager,
    ):
        """Test that sessions properly timeout and new sessions start correctly"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=5000, grace_ms=0
        )
        window = window_def.sum()
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Start session 1
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert updated[0][1]["start"] == 1000
            assert updated[0][1]["end"] == 1001  # exclusive: last event ts + 1
            assert not expired

            # Add to session 1 (within timeout)
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=4000
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3
            assert updated[0][1]["start"] == 1000
            assert updated[0][1]["end"] == 4001  # exclusive: last event ts + 1
            assert not expired

            # Start session 2 (outside timeout) - should expire session 1
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=15000
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 5
            assert updated[0][1]["start"] == 15000
            assert updated[0][1]["end"] == 15001  # exclusive: last event ts + 1

            assert len(expired) == 1
            assert expired[0][1]["value"] == 3
            assert expired[0][1]["start"] == 1000
            assert expired[0][1]["end"] == 4001  # exclusive: last event ts + 1

    def test_session_window_grace_period(
        self, session_window_definition_factory, state_manager
    ):
        """
        `grace` plays no part in assignment or merging, only in delaying
        closing. An event beyond `gap` but still inside `gap + grace` opens a
        *new* session instead of extending the old one; the old session stays
        open until the watermark passes its closing threshold, then is emitted
        through `final()` like any other session.
        """
        window_def = session_window_definition_factory(
            inactivity_gap_ms=5000, grace_ms=2000
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Start session 1
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000
            assert not expired

            # 8000 is more than `gap` (5000) after 1000, so it must start a
            # *second*, separate session - grace does not widen assignment.
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=8000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 8000
            assert updated[0][1]["value"] == 2
            assert not expired

            # A session closes once the watermark passes
            # `end + 2*gap + grace`. Both prior sessions' `end` (1001 and
            # 8001) are at or below `8001 + 2*5000 + 2000 = 20001`, so the
            # watermark-advancing event is pushed to 20001 (was 16000) so
            # both still close together here.
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=20001
            )
            assert updated[0][1]["start"] == 20001
            assert {(e[1]["start"], e[1]["value"]) for e in expired} == {
                (1000, 1),
                (8000, 2),
            }

    def test_session_window_multiple_keys(
        self, session_window_definition_factory, state_manager
    ):
        """Test that different keys maintain separate sessions"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=5000, grace_ms=0
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            # Start session for key1
            updated, expired = process(
                window, value=1, key=key1, transaction=tx, timestamp_ms=1000
            )
            assert len(updated) == 1
            assert updated[0][0] == key1
            assert updated[0][1]["value"] == 1
            assert not expired

            # Start session for key2
            updated, expired = process(
                window, value=10, key=key2, transaction=tx, timestamp_ms=2000
            )
            assert len(updated) == 1
            assert updated[0][0] == key2
            assert updated[0][1]["value"] == 10
            assert not expired

            # Add to key1 session
            updated, expired = process(
                window, value=2, key=key1, transaction=tx, timestamp_ms=3000
            )
            assert len(updated) == 1
            assert updated[0][0] == key1
            assert updated[0][1]["value"] == 3
            assert not expired

            # Add to key2 session
            updated, expired = process(
                window, value=20, key=key2, transaction=tx, timestamp_ms=4000
            )
            assert len(updated) == 1
            assert updated[0][0] == key2
            assert updated[0][1]["value"] == 30
            assert not expired

    def test_session_partition_expiration(
        self, session_window_definition_factory, state_manager
    ):
        """Test partition-level session expiration"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=5000, grace_ms=1000
        )
        window = window_def.sum()
        window.final(closing_strategy="partition")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            # Start sessions for both keys
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=1000)
            process(window, value=10, key=key2, transaction=tx, timestamp_ms=2000)

            # Add to both sessions
            process(window, value=2, key=key1, transaction=tx, timestamp_ms=3000)
            process(window, value=20, key=key2, transaction=tx, timestamp_ms=4000)

            # Event that advances partition time far enough to close both
            # sessions. A session closes once the watermark passes
            # `end + 2*gap + grace`; the later of the two ends (key2's
            # 4001) needs `4001 + 2*5000 + 1000 = 15001`, so the
            # watermark-advancing event is pushed to 15001 (was 15000).
            updated, expired = process(
                window, value=3, key=key1, transaction=tx, timestamp_ms=15001
            )

            # Should get new session for key1
            assert len(updated) == 1
            assert updated[0][0] == key1
            assert updated[0][1]["value"] == 3
            assert updated[0][1]["start"] == 15001

            # Should expire sessions for both keys
            expired_keys = {exp[0] for exp in expired}
            assert key1 in expired_keys
            assert key2 in expired_keys

    def test_session_window_late_events(
        self, session_window_definition_factory, state_manager, mock_message_context
    ):
        """Test handling of late events that arrive after session closure"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=5000, grace_ms=1000
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Start and finish a session
            process(window, value=1, key=key, transaction=tx, timestamp_ms=1000)
            process(window, value=2, key=key, transaction=tx, timestamp_ms=3000)

            # Start new session that will cause first to expire
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=15000
            )
            assert len(expired) == 1
            assert expired[0][1]["value"] == 3

            # Now send a late event that would belong to the first session
            # Should be ignored due to being too late
            updated, expired = process(
                window, value=10, key=key, transaction=tx, timestamp_ms=2500
            )
            # Should not affect any sessions since it's too late
            assert not updated
            assert not expired

    def test_session_window_current_mode(
        self, session_window_definition_factory, state_manager
    ):
        """Test session window with current() mode"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=5000, grace_ms=0
        )
        window = window_def.sum()
        window.current(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Start session - should get update immediately
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert not expired

            # Add to session - should get update immediately
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=3000
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3
            assert not expired

    def test_session_window_overlapping_sessions(
        self, session_window_definition_factory, state_manager
    ):
        """Test that sessions don't overlap for the same key"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=0
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Start session 1
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            session1_end = updated[0][1]["end"]

            # Event within timeout - extends session 1
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=5000
            )
            new_end = updated[0][1]["end"]
            assert new_end > session1_end  # Session extended
            assert updated[0][1]["value"] == 3  # Accumulated value

            # Event far in future - starts session 2, expires session 1
            updated, expired = process(
                window, value=10, key=key, transaction=tx, timestamp_ms=30000
            )
            assert len(expired) == 1
            assert expired[0][1]["value"] == 3  # Final value of session 1
            assert len(updated) == 1
            assert updated[0][1]["value"] == 10  # New session 2 starts fresh
            assert updated[0][1]["start"] == 30000

    def test_session_window_merge_sessions(
        self, session_window_definition_factory, state_manager
    ):
        """
        An out-of-order event that falls within `inactivity_gap_ms` of two
        separate open sessions merges them into one, combining the aggregation
        state via `BaseAggregator.merge()`.
        """
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=5000
        )
        window = window_def.count()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Session A
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert updated == [(key, {"start": 1000, "end": 1001, "value": 1})]
            assert not expired

            # 15000 is more than one gap after session A's last event (1000),
            # so it starts a separate session B rather than extending A.
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=15000
            )
            assert updated == [(key, {"start": 15000, "end": 15001, "value": 1})]
            assert not expired
            assert len(stored_windows(tx, key)) == 2

            # 8000 is within one gap of both A ([1000, 1001)) and B
            # ([15000, 15001)) -> merge into a single session.
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=8000
            )
            assert updated == [(key, {"start": 1000, "end": 15001, "value": 3})]
            assert not expired
            assert stored_windows(tx, key) == [(1000, 15001, 3)]

            # The merged session keeps extending like any other session.
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=20000
            )
            assert updated == [(key, {"start": 1000, "end": 20001, "value": 4})]
            assert not expired

            # Advance the watermark far enough to close the merged session:
            # close_before = 100000 - gap(10000) - grace(5000) = 85000, and
            # the merged session's end (20001) is well below that.
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100000
            )
            assert updated == [(key, {"start": 100000, "end": 100001, "value": 1})]
            assert expired == [(key, {"start": 1000, "end": 20001, "value": 4})]

    def test_session_window_bridging_event_scenario(
        self, session_window_definition_factory, state_manager
    ):
        """
        Session A and Session B, separated by more than one gap, are merged by
        a bridging event that falls within one gap of both.

        Session A: events at 1000 and 10000 -> [1000, 10001), value 15.
        Session B: event at 25000 -> [25000, 25001), value 10 (25000 is more
        than gap(10000) past A's last event, 10000, so it does NOT extend A).
        Bridge at 15000: within gap of both A (10001 + 10000 = 20001 > 15000)
        and B (25000 - 10000 = 15000 <= 15000, inclusive) -> merge into
        [1000, 25001), value 15 + 20 (aggregated into A first) merged with
        B's 10 = 45.
        """
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=20000
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Session A: starts, then extends.
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=1000
            )
            assert updated == [(key, {"start": 1000, "end": 1001, "value": 5})]
            assert not expired

            updated, expired = process(
                window, value=10, key=key, transaction=tx, timestamp_ms=10000
            )
            assert updated == [(key, {"start": 1000, "end": 10001, "value": 15})]
            assert not expired

            # Session B: 25000 is more than one gap past A's last event
            # (10000), so it must NOT extend A.
            updated, expired = process(
                window, value=10, key=key, transaction=tx, timestamp_ms=25000
            )
            assert updated == [(key, {"start": 25000, "end": 25001, "value": 10})]
            assert not expired
            assert len(stored_windows(tx, key)) == 2

            # Bridging event: within one gap of both A and B -> merge.
            updated, expired = process(
                window, value=20, key=key, transaction=tx, timestamp_ms=15000
            )
            assert updated == [(key, {"start": 1000, "end": 25001, "value": 45})]
            assert not expired
            assert stored_windows(tx, key) == [(1000, 25001, 45)]

            # Advance far enough to close the merged session. A session
            # closes once the watermark passes `end + 2*gap + grace`
            # (25001 + 2*10000 + 20000 = 65001), so the watermark-advancing
            # event is pushed to 65001 (was 60000).
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=65001
            )
            assert updated == [(key, {"start": 65001, "end": 65002, "value": 1})]
            assert expired == [(key, {"start": 1000, "end": 25001, "value": 45})]

    def test_session_window_string_key_extension(
        self, session_window_definition_factory, state_manager
    ):
        """
        Test session window extension with string keys.

        This test specifically verifies that session extension works correctly
        when using string keys (which need to be serialized to bytes internally).

        This test would have caught the original TypeError bug where
        `transaction.delete_window()` was called with a string key instead of
        the properly serialized bytes prefix.
        """
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            # Use a string key instead of bytes to trigger the serialization path
            key = "user_123"

            # Start a session
            updated, expired = process(
                window, value=100, key=key, transaction=tx, timestamp_ms=1000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000
            assert updated[0][1]["end"] == 1001  # exclusive: last event ts + 1
            assert updated[0][1]["value"] == 100
            assert not expired

            # Extend the session - this should trigger the delete_window call
            # that would have failed with the original bug
            updated, expired = process(
                window, value=200, key=key, transaction=tx, timestamp_ms=5000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000  # Session extended, same start
            assert updated[0][1]["end"] == 5001  # exclusive: last event ts + 1
            assert updated[0][1]["value"] == 300  # 100 + 200
            assert not expired

            # Extend the session again to make sure it still works
            updated, expired = process(
                window, value=50, key=key, transaction=tx, timestamp_ms=8000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000  # Session extended again
            assert updated[0][1]["end"] == 8001  # exclusive: last event ts + 1
            assert updated[0][1]["value"] == 350  # 100 + 200 + 50
            assert not expired

            # Test with a different string key to make sure multiple keys work
            key2 = "user_456"
            updated, expired = process(
                window, value=75, key=key2, transaction=tx, timestamp_ms=9000
            )
            assert len(updated) == 1
            assert updated[0][0] == key2  # Different key
            assert updated[0][1]["start"] == 9000
            assert updated[0][1]["end"] == 9001  # exclusive: last event ts + 1
            assert updated[0][1]["value"] == 75
            assert not expired

            # Expire the first session by advancing time far enough
            updated, expired = process(
                window, value=25, key=key, transaction=tx, timestamp_ms=30000
            )

            # Should have expired the first session
            assert len(expired) == 1
            assert expired[0][0] == key
            assert expired[0][1]["start"] == 1000
            assert expired[0][1]["end"] == 8001  # exclusive: last event ts + 1
            assert expired[0][1]["value"] == 350

            # Should have started a new session for the first key
            assert len(updated) == 1
            assert updated[0][0] == key
            assert updated[0][1]["start"] == 30000
            assert updated[0][1]["end"] == 30001  # exclusive: last event ts + 1
            assert updated[0][1]["value"] == 25

    def test_out_of_order_events_end_time(
        self, session_window_definition_factory, state_manager
    ):
        """Test that out-of-order events correctly maintain the latest timestamp as end time"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=5000
        )
        window = window_def.sum()
        window.final(closing_strategy="key")

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # 1. Start session with event at timestamp 1000
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert updated[0][1]["start"] == 1000
            assert updated[0][1]["end"] == 1001  # exclusive: last event ts + 1
            assert updated[0][1]["value"] == 1

            # 2. Add event at timestamp 8000 (in order)
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=8000
            )
            assert updated[0][1]["start"] == 1000
            assert updated[0][1]["end"] == 8001  # exclusive: latest event + 1
            assert updated[0][1]["value"] == 3

            # 3. Add OUT-OF-ORDER event at timestamp 3000 (before 8000)
            # This should be accepted (within grace period) but should NOT change the end time
            updated, expired = process(
                window, value=10, key=key, transaction=tx, timestamp_ms=3000
            )
            assert updated[0][1]["start"] == 1000
            # KEY TEST: End time should remain 8001, not become 3001!
            assert updated[0][1]["end"] == 8001
            assert updated[0][1]["value"] == 13

            # 4. Add event NEWER than current end (timestamp 9000)
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=9000
            )
            assert updated[0][1]["start"] == 1000
            # NOW the end time should update to 9001
            assert updated[0][1]["end"] == 9001
            assert updated[0][1]["value"] == 17

    @pytest.mark.parametrize("strategy", ["key", "partition"])
    def test_expiry_paths_do_not_scan_the_partition(
        self, strategy, session_window_definition_factory, state_manager
    ):
        """
        Neither expiry path may perform work proportional to the partition
        keyspace on every message. `get_windows` is the materializing primitive
        an `expire_windows`-style scan uses; `keys` is a full-partition scan.
        Twenty in-order messages on one key must trigger neither, under
        `final("key")` or `final("partition")`.
        """
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=0
        ).sum()
        window.final(closing_strategy=strategy)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with patch.object(
            WindowedRocksDBPartitionTransaction,
            "get_windows",
            autospec=True,
            side_effect=WindowedRocksDBPartitionTransaction.get_windows,
        ) as get_windows_spy:
            with patch.object(
                WindowedRocksDBPartitionTransaction,
                "keys",
                autospec=True,
                side_effect=WindowedRocksDBPartitionTransaction.keys,
            ) as keys_spy:
                with store.start_partition_transaction(0) as tx:
                    for i in range(20):
                        process(
                            window,
                            value=1,
                            key=key,
                            transaction=tx,
                            timestamp_ms=i * 100,
                        )

        assert get_windows_spy.call_count == 0
        assert keys_spy.call_count == 0

    @pytest.mark.xfail(
        strict=True,
        reason="partition-mode expiry sweep enumerates prefixes with a linear "
        "pass over the stored window keys; an (end, prefix, start) index column "
        "family would bound it by the number of prefixes",
    )
    def test_expiry_scan_cost_grows_with_windows_per_key_not_with_key_count(
        self, session_window_definition_factory, state_manager
    ):
        """
        A partition-mode expiry sweep should cost the number of message-key
        prefixes, not the number of stored windows.

        `test_expiry_paths_do_not_scan_the_partition` above only counts calls to
        `keys()` and `get_windows()`; the prefix enumerator reads window rows
        through `RocksDBStorePartition.iter_items`, which neither spy observes.
        This test counts rows read by that primitive instead, so it stays valid
        whichever method does the scanning: one key with many never-closing
        sessions must not cost more than many keys with one session each.
        """
        windows_per_key = 200
        num_keys = 200

        # Workload 1: one key, `windows_per_key` disjoint sessions that never
        # close (grace is huge), so all of them remain stored simultaneously
        # under a single message-key prefix.
        one_key_window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=10**9
        ).sum()
        one_key_window.final(closing_strategy="partition")
        one_key_store = state_manager.get_store(
            stream_id="test", store_name=one_key_window.name
        )
        one_key_store.assign_partition(0)
        key = b"only_key"
        with one_key_store.start_partition_transaction(0) as tx:
            for i in range(windows_per_key):
                process(
                    one_key_window,
                    value=1,
                    key=key,
                    transaction=tx,
                    timestamp_ms=i * 20000,
                )
        one_key_rows = _count_rows_read_by_iter_prefixes(one_key_store)

        # Workload 2: `num_keys` distinct keys, one never-closing session
        # each - same total window count as workload 1, spread over many
        # prefixes instead of one. A different `inactivity_gap_ms` gives
        # this a distinct store name from workload 1.
        many_keys_window = session_window_definition_factory(
            inactivity_gap_ms=10001, grace_ms=10**9
        ).sum()
        many_keys_window.final(closing_strategy="partition")
        many_keys_store = state_manager.get_store(
            stream_id="test", store_name=many_keys_window.name
        )
        many_keys_store.assign_partition(0)
        with many_keys_store.start_partition_transaction(0) as tx:
            for i in range(num_keys):
                process(
                    many_keys_window,
                    value=1,
                    key=f"key_{i}".encode(),
                    transaction=tx,
                    timestamp_ms=1000,
                )
        many_keys_rows = _count_rows_read_by_iter_prefixes(many_keys_store)

        # Sanity check on the measurement harness itself: the many-keys
        # workload's row count should scale with the number of keys (one
        # window per key), confirming `_count_rows_read_by_iter_prefixes`
        # actually observes DB reads rather than reading 0 rows for both
        # workloads by accident.
        assert many_keys_rows >= num_keys, (
            f"expected roughly {num_keys} rows for {num_keys} single-window "
            f"keys, measured {many_keys_rows}"
        )

        # The actual guard: a sweep over ONE key must cost a small constant,
        # not grow with how many windows that one key has open.
        bound = 3  # small constant factor, independent of windows_per_key
        assert one_key_rows <= bound, (
            f"expiry sweep for a single key with {windows_per_key} open "
            f"sessions read {one_key_rows} rows (bound={bound}); "
            f"for comparison, {num_keys} keys with one session each read "
            f"{many_keys_rows} rows. Cost is proportional to the number of "
            "stored windows, not to the number of message-key prefixes - "
            "`_iter_db_prefixes` is a full linear pass over every window "
            "row in the column family."
        )

    # ------------------------------------------------------------------
    # Regression tests for session assignment, lateness and merging.
    # ------------------------------------------------------------------

    def test_long_session_does_not_fragment(
        self, session_window_definition_factory, state_manager
    ):
        """
        A session that stays active for longer than 2 * inactivity_gap_ms must
        remain a single session.

        Events arrive every 5s with a 10s gap, so every event belongs to the
        session opened at 1000 and nothing ever closes.
        """
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=0
        ).sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            for timestamp_ms in (1000, 6000, 11000, 16000, 21000, 26000):
                updated, expired = process(
                    window, value=1, key=key, transaction=tx, timestamp_ms=timestamp_ms
                )
                assert (
                    updated[0][1]["start"] == 1000
                ), f"session fragmented at ts={timestamp_ms}"
                assert expired == [], f"session closed early at ts={timestamp_ms}"

            windows = stored_windows(tx, key)

        assert len(windows) == 1
        assert windows[0][2] == 6

    def test_partition_strategy_rejects_events_before_partition_watermark(
        self, session_window_definition_factory, state_manager, mock_message_context
    ):
        """
        With `closing_strategy="partition"` the partition watermark is live and
        monotonic, so an event below `watermark - gap - grace` is dropped and
        reported through the `on_late` callback.
        """
        on_late = Mock(return_value=False)
        window = session_window_definition_factory(
            inactivity_gap_ms=5000, grace_ms=1000, on_late=on_late
        ).sum()
        window.final(closing_strategy="partition")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=b"k1", transaction=tx, timestamp_ms=1000)
            # This raises the partition watermark to 500000 for every key.
            process(window, value=1, key=b"k2", transaction=tx, timestamp_ms=500000)

            # close_before = 500000 - 5000 - 1000 = 494000, so ts=2000 is late.
            updated, expired = process(
                window, value=99, key=b"k1", transaction=tx, timestamp_ms=2000
            )
            assert (updated, expired) == ([], [])
            assert on_late.call_count == 1
            # on_late is called positionally: (value, key, timestamp_ms, late_by_ms, ...)
            assert on_late.call_args[0][3] == 492000

            assert not [w for w in stored_windows(tx, b"k1") if w[0] == 2000]

    def test_out_of_order_event_inside_gap_is_accepted_with_zero_grace(
        self, session_window_definition_factory, state_manager, mock_message_context
    ):
        """
        An event is late only when `ts < watermark - gap - grace`, so the default
        `grace_ms=0` still leaves a full inactivity gap of out-of-order
        tolerance.

        ts=18000 arrives after ts=26000 but is only 8000ms behind, so it must
        extend the open session backwards rather than be dropped.
        """
        on_late = Mock(return_value=False)
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=0, on_late=on_late
        ).sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=1000)
            process(window, value=1, key=key, transaction=tx, timestamp_ms=26000)

            updated, _ = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=18000
            )
            assert on_late.call_count == 0
            assert [result["start"] for _, result in updated] == [18000]
            assert len(stored_windows(tx, key)) == 1

    def test_bridging_event_merges_two_sessions(
        self, session_window_definition_factory, state_manager
    ):
        """
        An out-of-order event that falls within the inactivity gap of two open
        sessions merges them into one with the aggregation states combined.

        gap=10000, grace=20000. `grace` plays no part in assignment, so ts=20000
        must open a *second* session before ts=10000 bridges the two.
        """
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=20000
        ).sum()
        window.current(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=1000)

            # 20000 is 19000ms after the previous event, i.e. more than one gap
            # away, so it starts a new session even though it is inside `grace`.
            updated, _ = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=20000
            )
            assert updated[0][1]["start"] == 20000
            assert len(stored_windows(tx, key)) == 2

            # 10000 is within one gap of both sessions -> merge.
            updated, _ = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=10000
            )
            windows = stored_windows(tx, key)
            assert len(windows) == 1
            assert (windows[0][0], windows[0][1]) == (1000, 20001)
            assert updated == [(key, {"start": 1000, "end": 20001, "value": 3})]

    def test_bridging_event_merges_two_sessions_multiaggregation(
        self, session_window_definition_factory, state_manager
    ):
        """
        A merged session combines every aggregation state, and collected values
        need no merging because they are range-fetched from the merged session's
        hull.
        """
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=20000
        ).agg(count=agg.Count(), mean=agg.Mean(), collect=agg.Collect())
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=1000)

            updated, _ = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=20000
            )
            assert updated[0][1]["start"] == 20000
            assert len(stored_windows(tx, key)) == 2

            process(window, value=1, key=key, transaction=tx, timestamp_ms=10000)
            assert len(stored_windows(tx, key)) == 1

            # The merged session's last event is 20000, so it closes once the
            # watermark passes 20000 + gap + grace = 50000.
            _, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=200000
            )

        assert len(expired) == 1
        merged = expired[0][1]
        assert merged["start"] == 1000
        assert merged["count"] == 3
        assert merged["mean"] == 1.0
        assert merged["collect"] == [1, 1, 1]

    def test_session_starting_at_epoch_zero_extends(
        self, session_window_definition_factory, state_manager
    ):
        """
        A session whose start is timestamp 0 must still be found by the
        backwards probe and extended.
        """
        window = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=0
        ).sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=0)
            updated, _ = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            windows = stored_windows(tx, key)

        assert updated[0][1]["start"] == 0
        assert len(windows) == 1
        assert (windows[0][0], windows[0][1]) == (0, 1001)

    def test_stored_sessions_are_never_within_the_gap(
        self, session_window_definition_factory, state_manager, mock_message_context
    ):
        """
        For a given message key, stored sessions are disjoint and non-adjacent:
        no two consecutive stored sessions are within `inactivity_gap_ms` of each
        other.

        Feeds a hard-coded stream of 40 in-order and out-of-order events across
        two keys, then checks the invariant on whatever state remains.
        """
        gap_ms = 10000
        window = session_window_definition_factory(
            inactivity_gap_ms=gap_ms, grace_ms=30000
        ).count()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            for timestamp_ms, key in INVARIANT_EVENTS:
                process(
                    window, value=1, key=key, transaction=tx, timestamp_ms=timestamp_ms
                )
            per_key = {key: stored_windows(tx, key) for key in (b"k1", b"k2")}

        for key, windows in per_key.items():
            for earlier, later in zip(windows, windows[1:]):
                assert (
                    later[0] >= earlier[1]
                ), f"{key!r}: sessions {earlier[:2]} and {later[:2]} overlap"
                # `end` is exclusive, so the last event of `earlier` is end - 1.
                assert later[0] - (earlier[1] - 1) > gap_ms, (
                    f"{key!r}: sessions {earlier[:2]} and {later[:2]} are within "
                    f"{gap_ms}ms of each other and should have been one session"
                )

    def test_non_mergeable_aggregation_rejected_at_definition_time(
        self, session_window_definition_factory, dataframe_factory, state_manager
    ):
        """
        An aggregation that cannot be merged is rejected by
        `SessionWindowDefinition._create_window` with `InvalidOperation`, at
        pipeline-definition time.
        """

        def reducer(aggregated, current):
            return aggregated + [current]

        def initializer(value):
            return [value]

        def merger(a, b):
            return a + b

        with pytest.raises(InvalidOperation, match="do not implement `merge`"):
            session_window_definition_factory(
                inactivity_gap_ms=10000, grace_ms=1000
            ).reduce(reducer=reducer, initializer=initializer)

        with pytest.raises(InvalidOperation, match="do not implement `merge`"):
            session_window_definition_factory(
                inactivity_gap_ms=10000, grace_ms=1000
            ).agg(value=NonMergeableSum())

        # Mergeable aggregations must keep building.
        session_window_definition_factory(inactivity_gap_ms=10000, grace_ms=1000).agg(
            value=agg.Sum()
        )
        session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        ).reduce(reducer=reducer, initializer=initializer, merger=merger)

        # Back-compat: the same non-mergeable aggregator still builds and runs
        # on a non-session window.
        tumbling = TumblingTimeWindowDefinition(
            duration_ms=10000,
            grace_ms=0,
            dataframe=dataframe_factory(
                state_manager=state_manager, registry=DataFrameRegistry()
            ),
        ).agg(value=NonMergeableSum())
        tumbling.final()
        store = state_manager.get_store(stream_id="test", store_name=tumbling.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, _ = process(
                tumbling, value=5, key=b"key", transaction=tx, timestamp_ms=1000
            )
        assert updated[0][1]["value"] == 5

    # ------------------------------------------------------------------
    # Regression tests for the closing/lateness threshold split, merging of
    # uninitialized state, expiry cursors and aggregator mergeability.
    # ------------------------------------------------------------------

    def test_close_before_shared_by_lateness_and_closing_allows_adjacent_sessions(
        self, session_window_definition_factory, state_manager
    ):
        """
        Closing and lateness must not share one cutoff. If they did, a session
        whose `end` lands exactly on the cutoff would close as soon as the
        watermark reached it while a boundary event at that same timestamp is
        still accepted; unable to extend the now-closed session, it would open a
        brand-new one immediately after - two emitted sessions 1ms apart, which
        breaks the non-adjacency invariant.
        """
        gap, grace = 50, 0
        window = session_window_definition_factory(
            inactivity_gap_ms=gap, grace_ms=grace
        ).sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            process(window, value=1, key=key, transaction=tx, timestamp_ms=1000)
            # watermark=1051, close_before=1001: [1000,1001) has end==close_before
            # and closes, even though timestamp 1001 (below) is itself accepted.
            _, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1051
            )
            # ts=1001 == close_before is not late, but its session is already
            # gone, so it starts a session immediately adjacent to the one
            # that just closed.
            updated, _ = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1001
            )

        sessions = sorted(
            (result["start"], result["end"])
            for _, result in list(expired) + list(updated)
        )
        for earlier, later in zip(sessions, sessions[1:]):
            assert later[0] - (earlier[1] - 1) > gap, (
                f"sessions {earlier} and {later} are within {gap}ms of each "
                f"other and should have been a single session: {sessions}"
            )

    def test_multiaggregation_reaggregation_of_none_value(
        self, session_window_definition_factory, state_manager
    ):
        """
        A session written by a collect-only window persists `value=None`. When
        the same store is later driven with an aggregator added, the
        single-match branch must treat that `None` as "not yet initialized" -
        exactly as `FixedTimeWindow` does - rather than crashing
        `_aggregate_value`.
        """
        definition = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=0
        )
        collect_only = definition.agg(items=agg.Collect())
        collect_only.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=collect_only.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            process(collect_only, value=1, key=key, transaction=tx, timestamp_ms=1000)

        reaggregated = definition.agg(items=agg.Collect(), n=agg.Count())
        # Same store: `.agg()` always names via `func_name=None`, independent
        # of which aggregators/collectors are set.
        # Do NOT call `.final()` again - the store is already registered
        # under this name (`collect_only.final(...)` above did that); the
        # default closing strategy ("key") is what we want here too.
        assert reaggregated.name == collect_only.name

        with store.start_partition_transaction(0) as tx:
            # 1500 matches [1000, 1001) (single match) and extends it. `n`
            # has no prior state under this key (the persisted value is the
            # collect-only window's `None`), so it must start fresh at 1,
            # not crash.
            updated, _ = process(
                reaggregated,
                value=1,
                key=key,
                transaction=tx,
                timestamp_ms=1500,
            )
        assert updated == [(key, {"start": 1000, "end": 1501, "n": 1, "items": []})]

    def test_merge_of_none_value(
        self, session_window_definition_factory, state_manager
    ):
        """
        The same stored `None` reaches `_merge_values` when one of two bridged
        sessions was written by a collect-only window and the other by an
        aggregating one: merging a fresh aggregate with the persisted `None`
        must treat it as uninitialized instead of crashing.

        `grace_ms=20000` (rather than 0) is required so that session A
        survives long enough to still be open when the bridging event
        arrives: `expire_by_key` runs on every `process_window` call under
        `closing_strategy="key"`, and with `grace_ms=0` writing session B at
        ts=20000 would itself push `close_before` past A's `end` and expire
        it before the bridge ever happens.
        """
        definition = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=20000
        )
        counted = definition.agg(n=agg.Count())
        counted.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=counted.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            # Session A: aggregate=True, persists a proper dict.
            process(counted, value=1, key=key, transaction=tx, timestamp_ms=1000)

        # Neither `collect_only` nor `bridging` below call `.final()` again:
        # the store is already registered under this name (`counted.final(
        # ...)` above did that), and the default closing strategy ("key") is
        # what we want for both.
        collect_only = definition.agg(items=agg.Collect())
        assert collect_only.name == counted.name
        with store.start_partition_transaction(0) as tx:
            # Session B: aggregate=False, persists `value=None`. 20000 is
            # more than one gap (10000) past A's last event (1000), so it
            # cannot extend A, and the large grace keeps A from closing.
            process(collect_only, value=1, key=key, transaction=tx, timestamp_ms=20000)

        bridging = definition.agg(n=agg.Count())
        assert bridging.name == counted.name
        with store.start_partition_transaction(0) as tx:
            # 10000 is within one gap of both A ([1000,1001)) and B
            # ([20000,20001)) -> merge. `_aggregate_value` on A's proper
            # dict succeeds (count 1 -> 2); B's persisted `None` must merge
            # as if it were uninitialized (count 0), not crash.
            updated, _ = process(
                bridging, value=1, key=key, transaction=tx, timestamp_ms=10000
            )
        assert updated == [(key, {"start": 1000, "end": 20001, "n": 2})]

    def test_partition_checkpoint_none_defers_already_due_sessions(
        self, session_window_definition_factory, state_manager
    ):
        """
        An unset partition expiry checkpoint means "sweep now". If the first
        partition-mode message overwrote it with its own session's due time -
        always ahead of that message's own watermark - it would gate the very
        sweep it precedes and defer already-overdue sessions of other keys.
        """
        gap, grace = 5000, 100
        definition = session_window_definition_factory(
            inactivity_gap_ms=gap, grace_ms=grace
        )

        key_mode = definition.sum()
        key_mode.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=key_mode.name)
        store.assign_partition(0)

        # Two already-overdue sessions, written under "key" mode.
        with store.start_partition_transaction(0) as tx:
            process(key_mode, value=1, key=b"k1", transaction=tx, timestamp_ms=1000)
            process(key_mode, value=1, key=b"k2", transaction=tx, timestamp_ms=2000)

        partition_mode = definition.sum()
        # Do NOT call `.final()` again - the store is already registered
        # under this name; set the closing strategy directly instead of
        # going through `_apply_window`'s `register_store()`.
        partition_mode._closing_strategy = ClosingStrategy.PARTITION
        assert partition_mode.name == key_mode.name  # same underlying store

        with store.start_partition_transaction(0) as tx:
            # close_before = 1_000_000 - 5000 - 100 = 994_900, well past both
            # sessions' end (1001 and 2001).
            _, expired = process(
                partition_mode,
                value=1,
                key=b"k3",
                transaction=tx,
                timestamp_ms=1_000_000,
            )

        expired_keys = {key for key, _ in expired}
        assert {b"k1", b"k2"} <= expired_keys, (
            "already-overdue sessions were not swept on the first "
            f"partition-mode message; expired={expired}"
        )

    def test_cursor_not_invalidated_when_grace_regresses_across_reopen(
        self, session_window_definition_factory, state_manager
    ):
        """
        `grace_ms` is not part of a session window's store name, so reopening
        the same store with a larger `grace_ms` can make a timestamp that used
        to be late acceptable again and write a new session starting below the
        persisted per-key expiry cursor. `expire_by_key`'s scan starts at
        `cursor + 1`, so without re-lowering the cursor that session would sit
        permanently below the scan's lower bound and never close.
        """
        gap = 10
        definition = session_window_definition_factory(
            inactivity_gap_ms=gap, grace_ms=0
        )
        strict = definition.sum()
        strict.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=strict.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            process(strict, value=1, key=key, transaction=tx, timestamp_ms=100)
            # 200 is more than one gap (10) past 100 -> new session; [100,101)
            # closes and the per-key expiry cursor becomes 100.
            _, expired = process(
                strict, value=1, key=key, transaction=tx, timestamp_ms=200
            )
            assert expired == [(key, {"start": 100, "end": 101, "value": 1})]

        lenient = session_window_definition_factory(
            inactivity_gap_ms=gap, grace_ms=1000
        ).sum()
        # Do NOT call `.final()` again - the store is already registered
        # under this name; the default closing strategy ("key") is what we
        # want here too.
        assert lenient.name == strict.name  # grace_ms is not part of the name

        with store.start_partition_transaction(0) as tx:
            # close_before = 200 (persisted watermark) - 10 - 1000 = -810, so
            # 95 is accepted; it doesn't match [200,201) and starts a new
            # session below the persisted cursor (100).
            updated, _ = process(
                lenient, value=1, key=key, transaction=tx, timestamp_ms=95
            )
            assert updated == [(key, {"start": 95, "end": 96, "value": 1})]

            # Drive the watermark far enough forward, across two more
            # messages on the same key, that [95, 96) is trivially closable.
            # [95, 96) must close as soon as the watermark passes it, which
            # happens on the very next message (ts=1_000_000) - accumulate
            # `expired` across both subsequent calls rather than checking
            # only the last one, since a correct fix emits it there and it
            # is gone by ts=2_000_000.
            _, expired_1 = process(
                lenient, value=1, key=key, transaction=tx, timestamp_ms=1_000_000
            )
            _, expired_2 = process(
                lenient, value=1, key=key, transaction=tx, timestamp_ms=2_000_000
            )
            expired = expired_1 + expired_2

        assert any(
            result["start"] == 95 for _, result in expired
        ), f"session [95, 96) was never emitted; expired batches={expired}"

    def test_partition_watermark_ignores_prior_key_mode_history(
        self,
        session_window_definition_factory,
        state_manager,
        mock_message_context,
    ):
        """
        `closing_strategy="key"` advances the watermark from the per-key
        persisted timestamp, `"partition"` from a separate partition-wide slot.
        Reopening the same store under `"partition"` after sessions were written
        under `"key"` must not ignore the per-key history, or a late duplicate
        would be accepted and produce a second session overlapping a span that
        was already closed and emitted.
        """
        gap = 10
        definition = session_window_definition_factory(
            inactivity_gap_ms=gap, grace_ms=0
        )
        key = b"c"

        key_mode = definition.count()
        key_mode.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=key_mode.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            process(key_mode, value=1, key=key, transaction=tx, timestamp_ms=100)
            process(key_mode, value=1, key=key, transaction=tx, timestamp_ms=105)
            # 1000 is more than one gap past 105 -> closes and emits [100,106).
            _, expired = process(
                key_mode, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert expired == [(key, {"start": 100, "end": 106, "value": 2})]

        on_late = Mock(return_value=False)
        partition_mode = session_window_definition_factory(
            inactivity_gap_ms=gap, grace_ms=0, on_late=on_late
        ).count()
        # Do NOT call `.final()` again - the store is already registered
        # under this name; set the closing strategy directly instead of
        # going through `_apply_window`'s `register_store()`.
        partition_mode._closing_strategy = ClosingStrategy.PARTITION
        assert partition_mode.name == key_mode.name  # same underlying store

        with store.start_partition_transaction(0) as tx:
            # ts=105 already lies inside the emitted [100, 106) session, so a
            # correct implementation must treat it as late relative to that
            # key's true history, not accept it as a new session.
            updated, _ = process(
                partition_mode, value=1, key=key, transaction=tx, timestamp_ms=105
            )

        assert on_late.called or not updated, (
            "ts=105 falls inside the already-emitted [100, 106) session but "
            f"was accepted as a new, overlapping session: updated={updated}"
        )

    def test_reduce_mergeable_true_when_merge_overridden_without_merger_kwarg(
        self, session_window_definition_factory
    ):
        """
        `BaseAggregator.mergeable` derives from whether `merge()` was
        overridden. `Reduce.mergeable` must honour the same rule: a `Reduce`
        subclass that overrides `merge()` directly instead of passing `merger=`
        is mergeable, and a session window must accept it.
        """

        class MergingReduce(agg.Reduce):
            def merge(self, a, b):
                return a + b

        reducer_agg = MergingReduce(
            reducer=lambda accumulated, current: accumulated + [current],
            initializer=lambda value: [value],
        )

        assert reducer_agg.mergeable is True

    def test_reduce_mergeable_true_when_merge_overridden_without_merger_kwarg_builds(
        self, session_window_definition_factory
    ):
        """Companion to the property-level red test above: the same
        aggregator must be able to build a session window without raising
        `InvalidOperation`."""

        class MergingReduce(agg.Reduce):
            def merge(self, a, b):
                return a + b

        reducer_agg = MergingReduce(
            reducer=lambda accumulated, current: accumulated + [current],
            initializer=lambda value: [value],
        )

        session_window_definition_factory(inactivity_gap_ms=10000, grace_ms=1000).agg(
            value=reducer_agg
        )

    def test_reduce_conflicting_merge_override_not_caught_at_definition_time(
        self, session_window_definition_factory
    ):
        """
        The mirror of the case above: a `Reduce` subclass supplying *both*
        `merger=` and a `merge()` override that ignores it. Method resolution
        makes the override win, so the two disagree; that conflict must be
        caught at window-definition time, not on the first bridging merge at
        runtime.
        """

        class BrokenMergeReduce(agg.Reduce):
            def merge(self, a, b):
                raise RuntimeError("ignores merger=, overrides merge() directly")

        broken = BrokenMergeReduce(
            reducer=lambda accumulated, current: accumulated + [current],
            initializer=lambda value: [value],
            merger=lambda a, b: a + b,
        )

        raised_at_definition_time = False
        try:
            session_window_definition_factory(
                inactivity_gap_ms=10000, grace_ms=1000
            ).agg(value=broken)
        except Exception:
            raised_at_definition_time = True

        assert raised_at_definition_time, (
            "a Reduce subclass supplying both `merger=` and a conflicting "
            "`merge()` override built successfully; the conflict must be "
            "rejected at definition time, not when a bridging merge runs it"
        )
