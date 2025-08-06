import pytest

import quixstreams.dataframe.windows.aggregations as agg
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.dataframe.windows.definitions import SessionWindowDefinition


@pytest.fixture()
def session_window_definition_factory(state_manager, dataframe_factory):
    def factory(inactivity_gap_ms: int, grace_ms: int = 0) -> SessionWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = SessionWindowDefinition(
            inactivity_gap_ms=inactivity_gap_ms, grace_ms=grace_ms, dataframe=sdf
        )
        return window_def

    return factory


def process(window, value, key, transaction, timestamp_ms):
    updated, expired = window.process_window(
        value=value, key=key, transaction=transaction, timestamp_ms=timestamp_ms
    )
    return list(updated), list(expired)


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
                        "end": 1000,  # timestamp of last event
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
                        "end": 5000,  # timestamp of last event
                        "count": 2,
                        "sum": 5,
                        "mean": 2.5,
                        "max": 4,
                        "min": 1,
                        "collect": [],
                    },
                )
            ]

            # Third event outside timeout starts new session, expires previous
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=26000
            )
            assert expired == [
                (
                    key,
                    {
                        "start": 1000,
                        "end": 5000,  # timestamp of last event
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
                        "start": 26000,
                        "end": 26000,  # timestamp of last event
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
        assert updated[0][1]["end"] == 5000  # timestamp of last event
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
        assert updated[0][1]["end"] == 5000  # timestamp of last event
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
        assert updated[0][1]["end"] == 5000  # timestamp of last event
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_sessionwindow_reduce(
        self, expiration, session_window_definition_factory, state_manager
    ):
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
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
        assert updated[0][1]["end"] == 5000  # timestamp of last event
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
        assert updated[0][1]["end"] == 5000  # timestamp of last event
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
        assert updated[0][1]["end"] == 5000  # timestamp of last event
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
            # Event outside timeout triggers session closure
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=25000
            )
        assert not updated
        assert expired == [(key, {"start": 1000, "end": 8000, "value": [1, 2, 3]})]

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
            assert updated[0][1]["end"] == 1000  # timestamp of last event
            assert not expired

            # Add to session 1 (within timeout)
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=4000
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3
            assert updated[0][1]["start"] == 1000
            assert updated[0][1]["end"] == 4000  # timestamp of last event
            assert not expired

            # Start session 2 (outside timeout) - should expire session 1
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=15000
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 5
            assert updated[0][1]["start"] == 15000
            assert updated[0][1]["end"] == 15000  # timestamp of last event

            assert len(expired) == 1
            assert expired[0][1]["value"] == 3
            assert expired[0][1]["start"] == 1000
            assert expired[0][1]["end"] == 4000  # timestamp of last event

    def test_session_window_grace_period(
        self, session_window_definition_factory, state_manager
    ):
        """Test that grace period allows late events"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=5000, grace_ms=2000
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Start session
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000
            assert not expired

            # Event that would normally expire the session, but within grace
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=8000
            )
            # Session should still be active due to grace period
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3
            assert not expired

            # Event outside grace period - should expire session
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=16000
            )
            assert len(expired) == 1
            assert expired[0][1]["value"] == 3
            assert expired[0][1]["start"] == 1000

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

            # Event that advances partition time beyond grace period
            # Should expire sessions for both keys
            updated, expired = process(
                window, value=3, key=key1, transaction=tx, timestamp_ms=15000
            )

            # Should get new session for key1
            assert len(updated) == 1
            assert updated[0][0] == key1
            assert updated[0][1]["value"] == 3
            assert updated[0][1]["start"] == 15000

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
        """Test that an event can merge two previously separate sessions"""
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=1000
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Create first session
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=1000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000
            assert updated[0][1]["end"] == 1000  # timestamp of last event
            assert updated[0][1]["value"] == 1
            assert not expired

            # Create second session that doesn't expire the first one yet
            # (13000 is still within timeout + grace of first session: 11000 + 1000 = 12000)
            # Actually, let's make it further: 20000ms to ensure two separate sessions
            updated, expired = process(
                window, value=10, key=key, transaction=tx, timestamp_ms=20000
            )
            # First session should now be expired
            assert len(expired) == 1
            assert expired[0][1]["start"] == 1000
            assert expired[0][1]["end"] == 1000  # timestamp of last event
            assert expired[0][1]["value"] == 1

            assert len(updated) == 1
            assert updated[0][1]["start"] == 20000
            assert updated[0][1]["end"] == 20000  # timestamp of last event
            assert updated[0][1]["value"] == 10

            # Add another event to the second session
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=25000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 20000
            assert updated[0][1]["end"] == 25000  # timestamp of last event
            assert updated[0][1]["value"] == 15  # 10 + 5
            assert not expired

            # Now test the limitation: we'll create a third session that could theoretically
            # merge with the second session if there was a bridging event
            # But since sessions don't auto-merge, they'll remain separate
            updated, expired = process(
                window, value=100, key=key, transaction=tx, timestamp_ms=50000
            )
            # Second session should be expired
            assert len(expired) == 1
            assert expired[0][1]["start"] == 20000
            assert expired[0][1]["end"] == 25000  # timestamp of last event
            assert expired[0][1]["value"] == 15

            # Third session starts
            assert len(updated) == 1
            assert updated[0][1]["start"] == 50000
            assert updated[0][1]["end"] == 50000  # timestamp of last event
            assert updated[0][1]["value"] == 100

    def test_session_window_bridging_event_scenario(
        self, session_window_definition_factory, state_manager
    ):
        """
        Test scenario where an event arrives that could theoretically bridge two sessions.

        This test documents the current behavior where sessions don't auto-merge,
        even when a bridging event could logically connect them.

        Scenario:
        1. Session A: [1000, 11000] with value 5
        2. Session B: [15000, 25000] with value 10
        3. Bridging event at 12000ms that:
           - Can extend Session A to [1000, 22000]
           - Now overlaps with Session B [15000, 25000]
           - Ideally should merge into single session [1000, 25000] with value 15+bridge_value

        Current behavior: Session A gets extended, Session B remains separate
        Ideal behavior: Sessions A and B get merged when bridging event arrives
        """
        window_def = session_window_definition_factory(
            inactivity_gap_ms=10000, grace_ms=2000
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"

            # Create Session A
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=1000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000
            assert updated[0][1]["end"] == 1000  # timestamp of last event
            assert updated[0][1]["value"] == 5
            assert not expired

            # Create Session B - close enough that it doesn't expire Session A
            # Session A expires when time > 11000 + 2000 = 13000
            # So event at 12000 should keep Session A alive
            updated, expired = process(
                window, value=10, key=key, transaction=tx, timestamp_ms=12000
            )
            # This should extend Session A since 12000 is within timeout+grace of Session A
            # Session A last activity was at 1000, so it expires at 1000+10000+2000=13000
            # Event at 12000 is before 13000, so it should extend Session A
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000  # Session A extended
            assert updated[0][1]["end"] == 12000  # timestamp of last event
            assert updated[0][1]["value"] == 15  # 5 + 10
            assert not expired

            # Now create what would be Session B if Session A hadn't been extended
            updated, expired = process(
                window, value=20, key=key, transaction=tx, timestamp_ms=15000
            )
            # This should extend the already extended Session A further
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000  # Still Session A
            assert updated[0][1]["end"] == 15000  # timestamp of last event
            assert updated[0][1]["value"] == 35  # 5 + 10 + 20
            assert not expired

            # Final event to expire the session
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=40000
            )
            assert len(expired) == 1
            assert expired[0][1]["start"] == 1000
            assert expired[0][1]["end"] == 15000  # timestamp of last event
            assert expired[0][1]["value"] == 35  # All events combined

            assert len(updated) == 1
            assert updated[0][1]["start"] == 40000
            assert updated[0][1]["value"] == 1

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
            assert updated[0][1]["end"] == 1000  # timestamp of last event
            assert updated[0][1]["value"] == 100
            assert not expired

            # Extend the session - this should trigger the delete_window call
            # that would have failed with the original bug
            updated, expired = process(
                window, value=200, key=key, transaction=tx, timestamp_ms=5000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000  # Session extended, same start
            assert updated[0][1]["end"] == 5000  # timestamp of last event
            assert updated[0][1]["value"] == 300  # 100 + 200
            assert not expired

            # Extend the session again to make sure it still works
            updated, expired = process(
                window, value=50, key=key, transaction=tx, timestamp_ms=8000
            )
            assert len(updated) == 1
            assert updated[0][1]["start"] == 1000  # Session extended again
            assert updated[0][1]["end"] == 8000  # timestamp of last event
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
            assert updated[0][1]["end"] == 9000  # timestamp of last event
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
            assert expired[0][1]["end"] == 8000  # timestamp of last event
            assert expired[0][1]["value"] == 350

            # Should have started a new session for the first key
            assert len(updated) == 1
            assert updated[0][0] == key
            assert updated[0][1]["start"] == 30000
            assert updated[0][1]["end"] == 30000  # timestamp of last event
            assert updated[0][1]["value"] == 25
