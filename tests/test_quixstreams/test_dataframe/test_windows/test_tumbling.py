import pytest

import quixstreams.dataframe.windows.aggregations as agg
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.dataframe.windows import (
    TumblingTimeWindowDefinition,
)


@pytest.fixture()
def tumbling_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        duration_ms: int,
        grace_ms: int = 0,
        before_update=None,
        after_update=None,
    ) -> TumblingTimeWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = TumblingTimeWindowDefinition(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            dataframe=sdf,
            before_update=before_update,
            after_update=after_update,
        )
        return window_def

    return factory


def process(window, value, key, transaction, timestamp_ms, headers=None):
    updated, triggered = window.process_window(
        value=value,
        key=key,
        timestamp_ms=timestamp_ms,
        headers=headers,
        transaction=transaction,
    )
    expired = window.expire_by_partition(
        transaction=transaction, timestamp_ms=timestamp_ms
    )
    # Combine triggered windows (from callbacks) with time-expired windows
    all_expired = list(triggered) + list(expired)
    return list(updated), all_expired


class TestTumblingWindow:
    def test_tumbling_window_with_after_update_trigger(
        self, tumbling_window_definition_factory, state_manager
    ):
        # Define a trigger that expires the window when the sum reaches 9 or more
        def trigger_on_sum_9(aggregated, value, key, timestamp, headers) -> bool:
            return aggregated >= 9

        window_def = tumbling_window_definition_factory(
            duration_ms=100, grace_ms=0, after_update=trigger_on_sum_9
        )
        window = window_def.sum()
        window.final()

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            # Add value=2, sum becomes 2, delta from 0 is 2, should not trigger
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=50
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert not expired

            # Add value=2, sum becomes 4, delta from 2 is 2, should not trigger
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=60
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 4
            assert not expired

            # Add value=5, sum becomes 9, delta from 4 is 5, should trigger (>= 5)
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=70
            )
            assert not updated  # Window was triggered
            assert len(expired) == 1
            assert expired[0][1]["value"] == 9
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100

            # Next value should start a new window
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=80
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3
            assert not expired

    def test_tumbling_window_with_before_update_trigger(
        self, tumbling_window_definition_factory, state_manager
    ):
        """Test that before_update callback works and triggers before aggregation."""

        # Define a trigger that expires the window before adding a value
        # if the sum would exceed 10
        def trigger_before_exceeding_10(
            aggregated, value, key, timestamp, headers
        ) -> bool:
            return (aggregated + value) > 10

        window_def = tumbling_window_definition_factory(
            duration_ms=100, grace_ms=0, before_update=trigger_before_exceeding_10
        )
        window = window_def.sum()
        window.final()

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            # Add value=3, sum becomes 3, would not exceed 10, should not trigger
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=50
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3
            assert not expired

            # Add value=5, sum becomes 8, would not exceed 10, should not trigger
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=60
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 8
            assert not expired

            # Add value=3, would make sum 11 which exceeds 10, should trigger BEFORE adding
            # So the expired window should have value=8 (not 11)
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=70
            )
            assert not updated  # Window was triggered
            assert len(expired) == 1
            assert expired[0][1]["value"] == 8  # Before the update (not 11)
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100

            # Next value should start a new window
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=80
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert not expired

    def test_tumbling_window_collect_with_after_update_trigger(
        self, tumbling_window_definition_factory, state_manager
    ):
        """Test that after_update callback works with collect."""

        # Define a trigger that expires the window when we collect 3 or more items
        def trigger_on_count_3(aggregated, value, key, timestamp, headers) -> bool:
            # For collect, aggregated is the list of collected values
            return len(aggregated) >= 3

        window_def = tumbling_window_definition_factory(
            duration_ms=100, grace_ms=0, after_update=trigger_on_count_3
        )
        window = window_def.collect()
        window.final()

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            # Add first value - should not trigger (count=1)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=50
            )
            assert not updated  # collect doesn't emit on updates
            assert not expired

            # Add second value - should not trigger (count=2)
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=60
            )
            assert not updated
            assert not expired

            # Add third value - should trigger (count=3)
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=70
            )
            assert not updated
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3]
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100

            # Next value at t=80 still belongs to window [0, 100)
            # Window is "resurrected" because collection values weren't deleted
            # (we let normal expiration handle cleanup for simplicity)
            # Window [0, 100) now has [1, 2, 3, 4] = 4 items - TRIGGERS AGAIN
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=80
            )
            assert not updated
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3, 4]
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100

    def test_tumbling_window_collect_with_before_update_trigger(
        self, tumbling_window_definition_factory, state_manager
    ):
        """Test that before_update callback works with collect."""

        # Define a trigger that expires the window before adding a value
        # if the collection would reach 3 or more items
        def trigger_before_count_3(aggregated, value, key, timestamp, headers) -> bool:
            # For collect, aggregated is the list of collected values BEFORE adding the new value
            return len(aggregated) + 1 >= 3

        window_def = tumbling_window_definition_factory(
            duration_ms=100, grace_ms=0, before_update=trigger_before_count_3
        )
        window = window_def.collect()
        window.final()

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            # Add first value - should not trigger (count would be 1)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=50
            )
            assert not updated  # collect doesn't emit on updates
            assert not expired

            # Add second value - should not trigger (count would be 2)
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=60
            )
            assert not updated
            assert not expired

            # Add third value - should trigger BEFORE adding (count would be 3)
            # Expired window should have [1, 2] (not [1, 2, 3])
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=70
            )
            assert not updated
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2]  # Before adding the third value
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100

            # Next value should start accumulating in the same window again
            # (window was deleted but collection values remain until natural expiration)
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=80
            )
            assert not updated
            # Window [0, 100) is "resurrected" with [1, 2, 3]
            # Adding value 4 would make it 4 items, triggers again
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3]  # Before adding 4
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100

    def test_tumbling_window_agg_and_collect_with_before_update_trigger(
        self, tumbling_window_definition_factory, state_manager
    ):
        """Test before_update with BOTH aggregation and collect.

        This verifies that:
        1. The triggered window does NOT include the triggering value in collect
        2. The triggering value IS still added to collection storage for future
        3. The aggregated value is BEFORE the triggering value
        """
        import quixstreams.dataframe.windows.aggregations as agg

        # Trigger when count would reach 3
        def trigger_before_count_3(agg_dict, value, key, timestamp, headers) -> bool:
            # In multi-aggregation, keys are like 'count/Count', 'sum/Sum'
            # Find the count aggregation value
            for k, v in agg_dict.items():
                if k.startswith("count"):
                    return v + 1 >= 3
            return False

        window_def = tumbling_window_definition_factory(
            duration_ms=100, grace_ms=0, before_update=trigger_before_count_3
        )
        window = window_def.agg(count=agg.Count(), sum=agg.Sum(), collect=agg.Collect())
        window.final()

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            # Add value=1, count becomes 1
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=50
            )
            assert len(updated) == 1
            assert not expired

            # Add value=2, count becomes 2
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=60
            )
            assert len(updated) == 1
            assert not expired

            # Add value=3, would make count 3
            # Should trigger BEFORE adding
            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=70
            )
            assert not updated  # Window was triggered
            assert len(expired) == 1

            assert expired[0][1]["count"] == 2  # Before the update (not 3)
            assert expired[0][1]["sum"] == 3  # Before the update (1+2, not 1+2+3)
            # CRITICAL: collect should NOT include the triggering value (3)
            assert expired[0][1]["collect"] == [1, 2]
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100

            # Next value should start a new window
            # But the triggering value (3) should still be in storage
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=80
            )
            assert len(updated) == 1
            assert not expired

            # Force window expiration to see what was collected
            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 1
            # The collection should include the triggering value (3) that was added to storage
            # even though it wasn't in the triggered window result
            assert expired[0][1]["collect"] == [1, 2, 3, 4]  # All values before t=110

    @pytest.mark.parametrize(
        "duration, grace, provided_name, func_name, expected_name",
        [
            (10, 5, "custom_window", "sum", "custom_window_tumbling_window_10_sum"),
            (10, 5, None, "sum", "tumbling_window_10_sum"),
            (15, 5, None, "count", "tumbling_window_15_count"),
        ],
    )
    def test_tumbling_window_definition_get_name(
        self,
        duration,
        grace,
        provided_name,
        func_name,
        expected_name,
        dataframe_factory,
    ):
        twd = TumblingTimeWindowDefinition(
            duration_ms=duration,
            grace_ms=grace,
            dataframe=dataframe_factory(),
            name=provided_name,
        )
        name = twd._get_name(func_name)
        assert name == expected_name

    def test_multiaggregation(
        self,
        tumbling_window_definition_factory,
        state_manager,
    ):
        window = tumbling_window_definition_factory(duration_ms=10).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Mean(),
            max=agg.Max(),
            min=agg.Min(),
            collect=agg.Collect(),
        )
        window.final()
        assert window.name == "tumbling_window_10"

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=2
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 0,
                        "end": 10,
                        "count": 1,
                        "sum": 1,
                        "mean": 1.0,
                        "max": 1,
                        "min": 1,
                        "collect": [],
                    },
                )
            ]

            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=4
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 0,
                        "end": 10,
                        "count": 2,
                        "sum": 5,
                        "mean": 2.5,
                        "max": 4,
                        "min": 1,
                        "collect": [],
                    },
                )
            ]

            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=12
            )
            assert expired == [
                (
                    key,
                    {
                        "start": 0,
                        "end": 10,
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
                        "start": 10,
                        "end": 20,
                        "count": 1,
                        "sum": 2,
                        "mean": 2.0,
                        "max": 2,
                        "min": 2,
                        "collect": [],
                    },
                )
            ]

        # Update window definition
        # * delete an aggregation (min)
        # * change aggregation but keep the name with new aggregation (mean -> max)
        # * add new aggregations (sum2, collect2)
        window = tumbling_window_definition_factory(duration_ms=10).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Max(),
            max=agg.Max(),
            collect=agg.Collect(),
            sum2=agg.Sum(),
            collect2=agg.Collect(),
        )
        assert window.name == "tumbling_window_10"  # still the same window and store
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=13
            )
            assert not expired
            assert (
                updated
                == [
                    (
                        key,
                        {
                            "start": 10,
                            "end": 20,
                            "count": 2,
                            "sum": 3,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 2,
                            "collect": [],
                            "collect2": [],
                        },
                    )
                ]
            )

            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=22
            )
            assert (
                expired
                == [
                    (
                        key,
                        {
                            "start": 10,
                            "end": 20,
                            "count": 2,
                            "sum": 3,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 2,
                            "collect": [2, 1],
                            "collect2": [
                                2,
                                1,
                            ],  # Collect2 has all the values as they were fully collected before the update
                        },
                    )
                ]
            )
            assert updated == [
                (
                    key,
                    {
                        "start": 20,
                        "end": 30,
                        "count": 1,
                        "sum": 2,
                        "sum2": 2,
                        "mean": 2,
                        "max": 2,
                        "collect": [],
                        "collect2": [],
                    },
                )
            ]

    def test_tumblingwindow_count(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.count()
        assert window.name == "tumbling_window_10_count"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=0, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=0, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 2
        assert not expired

    def test_tumblingwindow_sum(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.sum()
        assert window.name == "tumbling_window_10_sum"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 3
        assert not expired

    def test_tumblingwindow_mean(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.mean()
        assert window.name == "tumbling_window_10_mean"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 1.5
        assert not expired

    def test_tumblingwindow_reduce(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        assert window.name == "tumbling_window_10_reduce"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == [2, 1]
        assert not expired

    def test_tumblingwindow_max(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.max()
        assert window.name == "tumbling_window_10_max"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 2
        assert not expired

    def test_tumblingwindow_min(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.min()
        assert window.name == "tumbling_window_10_min"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0][1]["value"] == 1
        assert not expired

    def test_tumblingwindow_collect(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.collect()
        assert window.name == "tumbling_window_10_collect"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=1, key=key, transaction=tx, timestamp_ms=100)
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            process(window, value=3, key=key, transaction=tx, timestamp_ms=101)
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=200
            )
        assert not updated
        assert expired == [(key, {"start": 100, "end": 110, "value": [1, 2, 3]})]

    @pytest.mark.parametrize(
        "duration, grace, name",
        [
            (-10, 5, "test"),  # duration < 0
            (10, -5, "test"),  # grace < 0
        ],
    )
    def test_tumbling_window_def_init_invalid(
        self, duration, grace, name, dataframe_factory
    ):
        with pytest.raises(ValueError):
            TumblingTimeWindowDefinition(
                duration_ms=duration,
                grace_ms=grace,
                name=name,
                dataframe=dataframe_factory(),
            )

    def test_tumbling_window_process_window_expired(
        self,
        tumbling_window_definition_factory,
        state_manager,
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=0)
        window = window_def.sum()
        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            # Add item to the window [100, 110)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert updated[0][1]["start"] == 100
            assert updated[0][1]["end"] == 110
            assert not expired

            # Now add item to the window [110, 120)
            # The window [100, 110) is now expired and should be returned
            updated, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=110
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert updated[0][1]["start"] == 110
            assert updated[0][1]["end"] == 120

            assert len(expired) == 1
            assert expired[0][1]["value"] == 1
            assert expired[0][1]["start"] == 100
            assert expired[0][1]["end"] == 110

    def test_tumbling_partition_expiration(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=2)
        window = window_def.sum()
        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            # items for key1
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=100)
            process(window, value=3, key=key1, transaction=tx, timestamp_ms=105)

            # items for key2
            process(window, value=5, key=key2, transaction=tx, timestamp_ms=102)
            process(window, value=7, key=key2, transaction=tx, timestamp_ms=108)

            # don't expire key1 due to grace period
            updated, expired = process(
                window, value=2, key=key1, transaction=tx, timestamp_ms=110
            )

            assert updated == [
                (key1, {"start": 110, "end": 120, "value": 2}),
            ]
            assert expired == []

            # an older key 2 is accepted
            updated, expired = process(
                window, value=2, key=key2, transaction=tx, timestamp_ms=109
            )
            assert updated == [
                (key2, {"start": 100, "end": 110, "value": 14}),
            ]
            assert expired == []

            # Expire key1
            updated, expired = process(
                window, value=2, key=key1, transaction=tx, timestamp_ms=112
            )

            assert updated == [
                (key1, {"start": 110, "end": 120, "value": 4}),
            ]
            assert expired == [
                (key1, {"start": 100, "end": 110, "value": 4}),
                (key2, {"start": 100, "end": 110, "value": 14}),
            ]
