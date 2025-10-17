import functools

import pytest

import quixstreams.dataframe.windows.aggregations as agg
from quixstreams.dataframe import DataFrameRegistry
from quixstreams.dataframe.windows import (
    HoppingCountWindowDefinition,
    HoppingTimeWindowDefinition,
)
from quixstreams.dataframe.windows.time_based import ClosingStrategy


@pytest.fixture()
def hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        duration_ms: int,
        step_ms: int,
        grace_ms: int = 0,
        before_update=None,
        after_update=None,
    ) -> HoppingTimeWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = HoppingTimeWindowDefinition(
            duration_ms=duration_ms,
            step_ms=step_ms,
            grace_ms=grace_ms,
            dataframe=sdf,
            before_update=before_update,
            after_update=after_update,
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


class TestHoppingWindow:
    def test_hopping_window_with_after_update_trigger(
        self, hopping_window_definition_factory, state_manager
    ):
        # Define a trigger that expires windows when the sum reaches 100 or more
        def trigger_on_sum_100(aggregated, value, key, timestamp, headers) -> bool:
            return aggregated >= 100

        window_def = hopping_window_definition_factory(
            duration_ms=100, step_ms=50, grace_ms=100, after_update=trigger_on_sum_100
        )
        window = window_def.sum()
        window.final(closing_strategy="key")

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            _process = functools.partial(
                process, window=window, key=key, transaction=tx
            )

            # Step 1: Add value=90 at timestamp 50ms
            # Creates windows [0, 100) and [50, 150) with sum 90 each
            updated, expired = _process(value=90, timestamp_ms=50)
            assert len(updated) == 2
            assert updated[0][1]["value"] == 90
            assert updated[0][1]["start"] == 0
            assert updated[0][1]["end"] == 100
            assert updated[1][1]["value"] == 90
            assert updated[1][1]["start"] == 50
            assert updated[1][1]["end"] == 150
            assert not expired

            # Step 2: Add value=5 at timestamp 110ms
            # With grace_ms=100, [0, 100) does NOT expire naturally yet
            # [0, 100): stays 90 (timestamp 110 is outside [0, 100), not updated)
            # [50, 150): 90 -> 95 (< 100, NOT TRIGGERED)
            # [100, 200): newly created with sum 5
            updated, expired = _process(value=5, timestamp_ms=110)
            assert len(updated) == 2
            assert updated[0][1]["value"] == 95
            assert updated[0][1]["start"] == 50
            assert updated[0][1]["end"] == 150
            assert updated[1][1]["value"] == 5
            assert updated[1][1]["start"] == 100
            assert updated[1][1]["end"] == 200
            # No windows expired (grace period keeps [0, 100) alive)
            assert not expired

            # Step 3: Add value=5 at timestamp 90ms (late message)
            # Timestamp 90 belongs to BOTH [0, 100) and [50, 150)
            # [0, 100): 90 -> 95 (< 100, NOT TRIGGERED)
            # [50, 150): 95 -> 100 (>= 100, TRIGGERED!)
            updated, expired = _process(value=5, timestamp_ms=90)
            # Only [0, 100) remains in updated (not triggered, 95 < 100)
            # Only [50, 150) was triggered (100 >= 100)
            assert len(updated) == 1
            assert updated[0][1]["value"] == 95
            assert updated[0][1]["start"] == 0
            assert updated[0][1]["end"] == 100
            assert len(expired) == 1
            assert expired[0][1]["value"] == 100
            assert expired[0][1]["start"] == 50
            assert expired[0][1]["end"] == 150

    def test_hopping_window_with_before_update_trigger(
        self, hopping_window_definition_factory, state_manager
    ):
        """Test that before_update callback works for hopping windows."""

        # Define a trigger that expires windows before adding a value
        # if the sum would exceed 50
        def trigger_before_exceeding_50(
            aggregated, value, key, timestamp, headers
        ) -> bool:
            return (aggregated + value) > 50

        window_def = hopping_window_definition_factory(
            duration_ms=100,
            step_ms=50,
            grace_ms=100,
            before_update=trigger_before_exceeding_50,
        )
        window = window_def.sum()
        window.final(closing_strategy="key")

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            # Helper to process and return results
            def _process(value, timestamp_ms):
                return process(
                    window,
                    value=value,
                    key=key,
                    transaction=tx,
                    timestamp_ms=timestamp_ms,
                )

            # Step 1: Add value=10 at timestamp 50ms
            # Belongs to windows [0, 100) and [50, 150) (hopping windows overlap)
            # Both windows: Sum=10, doesn't exceed 50, no trigger
            updated, expired = _process(value=10, timestamp_ms=50)
            assert len(updated) == 2
            assert updated[0][1]["value"] == 10
            assert updated[0][1]["start"] == 0
            assert updated[1][1]["value"] == 10
            assert updated[1][1]["start"] == 50
            assert not expired

            # Step 2: Add value=20 at timestamp 60ms
            # Belongs to windows [0, 100) and [50, 150)
            # Both windows: Sum=30, doesn't exceed 50, no trigger
            updated, expired = _process(value=20, timestamp_ms=60)
            assert len(updated) == 2
            assert updated[0][1]["value"] == 30  # [0, 100)
            assert updated[1][1]["value"] == 30  # [50, 150)
            assert not expired

            # Step 3: Add value=25 at timestamp 70ms
            # Belongs to windows [0, 100) and [50, 150)
            # Both windows: Sum would be 55 which exceeds 50, should trigger BEFORE adding
            # Both expired windows should have value=30 (not 55)
            updated, expired = _process(value=25, timestamp_ms=70)
            assert not updated
            assert len(expired) == 2
            assert expired[0][1]["value"] == 30  # [0, 100) before the update
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100
            assert expired[1][1]["value"] == 30  # [50, 150) before the update
            assert expired[1][1]["start"] == 50
            assert expired[1][1]["end"] == 150

            # Step 4: Add value=5 at timestamp 100ms
            # Belongs to windows [50, 150) and [100, 200)
            # Window [50, 150) sum=5, doesn't trigger
            # Window [100, 200) sum=5, doesn't trigger
            updated, expired = _process(value=5, timestamp_ms=100)
            assert len(updated) == 2
            # Results should be for both windows
            assert not expired

    def test_hopping_window_collect_with_after_update_trigger(
        self, hopping_window_definition_factory, state_manager
    ):
        """Test that after_update callback works with collect for hopping windows."""

        # Define a trigger that expires windows when we collect 3 or more items
        def trigger_on_count_3(aggregated, value, key, timestamp, headers) -> bool:
            return len(aggregated) >= 3

        window_def = hopping_window_definition_factory(
            duration_ms=100, step_ms=50, grace_ms=100, after_update=trigger_on_count_3
        )
        window = window_def.collect()
        window.final(closing_strategy="key")

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            _process = functools.partial(
                process, window=window, key=key, transaction=tx
            )

            # Step 1: Add first value at timestamp 50ms
            # Creates windows [0, 100) and [50, 150) with 1 item each
            updated, expired = _process(value=1, timestamp_ms=50)
            assert not updated  # collect doesn't emit on updates
            assert not expired

            # Step 2: Add second value at timestamp 60ms
            # Both windows now have 2 items
            updated, expired = _process(value=2, timestamp_ms=60)
            assert not updated
            assert not expired

            # Step 3: Add third value at timestamp 70ms
            # Both windows now have 3 items - BOTH SHOULD TRIGGER
            updated, expired = _process(value=3, timestamp_ms=70)
            assert not updated
            assert len(expired) == 2
            # Window [0, 100) triggered
            assert expired[0][1]["value"] == [1, 2, 3]
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100
            # Window [50, 150) triggered
            assert expired[1][1]["value"] == [1, 2, 3]
            assert expired[1][1]["start"] == 50
            assert expired[1][1]["end"] == 150

            # Step 4: Add fourth value at timestamp 110ms
            # Timestamp 110 belongs to windows [50, 150) and [100, 200)
            # Window [50, 150) is "resurrected" because collection values weren't deleted
            # (for hopping windows, we don't delete collection on trigger to preserve
            # values for overlapping windows)
            # Window [50, 150) now has [1, 2, 3, 4] = 4 items - TRIGGERS AGAIN!
            # Window [100, 200) has [4] = 1 item - doesn't trigger
            updated, expired = _process(value=4, timestamp_ms=110)
            assert not updated
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3, 4]
            assert expired[0][1]["start"] == 50
            assert expired[0][1]["end"] == 150

    def test_hopping_window_collect_with_before_update_trigger(
        self, hopping_window_definition_factory, state_manager
    ):
        """Test that before_update callback works with collect for hopping windows."""

        # Define a trigger that expires windows before adding a value
        # if the collection would reach 3 or more items
        def trigger_before_count_3(aggregated, value, key, timestamp, headers) -> bool:
            # For collect, aggregated is the list of collected values BEFORE adding
            return len(aggregated) + 1 >= 3

        window_def = hopping_window_definition_factory(
            duration_ms=100,
            step_ms=50,
            grace_ms=100,
            before_update=trigger_before_count_3,
        )
        window = window_def.collect()
        window.final(closing_strategy="key")

        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"

        with store.start_partition_transaction(0) as tx:
            # Helper to process and return results
            def _process(value, timestamp_ms):
                return process(
                    window,
                    value=value,
                    key=key,
                    transaction=tx,
                    timestamp_ms=timestamp_ms,
                )

            # Step 1: Add value=1 at timestamp 50ms
            # Belongs to windows [0, 100) and [50, 150)
            # Both windows would have 1 item, no trigger
            updated, expired = _process(value=1, timestamp_ms=50)
            assert not updated  # collect doesn't emit on updates
            assert not expired

            # Step 2: Add value=2 at timestamp 60ms
            # Belongs to windows [0, 100) and [50, 150)
            # Both windows would have 2 items, no trigger
            updated, expired = _process(value=2, timestamp_ms=60)
            assert not updated
            assert not expired

            # Step 3: Add value=3 at timestamp 70ms
            # Belongs to windows [0, 100) and [50, 150)
            # Both windows would have 3 items, triggers BEFORE adding
            # Both windows should have [1, 2] (not [1, 2, 3])
            updated, expired = _process(value=3, timestamp_ms=70)
            assert not updated
            assert len(expired) == 2
            # Window [0, 100)
            assert expired[0][1]["value"] == [1, 2]
            assert expired[0][1]["start"] == 0
            assert expired[0][1]["end"] == 100
            # Window [50, 150)
            assert expired[1][1]["value"] == [1, 2]
            assert expired[1][1]["start"] == 50
            assert expired[1][1]["end"] == 150

            # Step 4: Add value=4 at timestamp 110ms
            # Belongs to windows [50, 150) and [100, 200)
            # Window [50, 150) resurrected with [1, 2, 3] - would be 4 items, triggers
            # Window [100, 200) would have 1 item, no trigger
            updated, expired = _process(value=4, timestamp_ms=110)
            assert not updated
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3]  # Before adding 4
            assert expired[0][1]["start"] == 50
            assert expired[0][1]["end"] == 150

    @pytest.mark.parametrize(
        "duration, grace, step, provided_name, func_name, expected_name",
        [
            (10, 5, 3, "custom_window", "sum", "custom_window_hopping_window_10_3_sum"),
            (10, 5, 3, None, "sum", "hopping_window_10_3_sum"),
            (15, 5, 3, None, "count", "hopping_window_15_3_count"),
        ],
    )
    def test_hopping_window_definition_get_name(
        self,
        duration,
        grace,
        step,
        provided_name,
        func_name,
        expected_name,
        dataframe_factory,
    ):
        twd = HoppingTimeWindowDefinition(
            duration_ms=duration,
            grace_ms=grace,
            step_ms=step,
            dataframe=dataframe_factory(),
            name=provided_name,
        )
        name = twd._get_name(func_name)
        assert name == expected_name

    def test_multiaggregation(
        self,
        hopping_window_definition_factory,
        state_manager,
    ):
        window = hopping_window_definition_factory(duration_ms=10, step_ms=5).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Mean(),
            max=agg.Max(),
            min=agg.Min(),
            collect=agg.Collect(),
        )
        window.final()
        assert window.name == "hopping_window_10_5"

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
                ),
            ]

            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=6
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
                ),
                (
                    key,
                    {
                        "start": 5,
                        "end": 15,
                        "count": 1,
                        "sum": 4,
                        "mean": 4,
                        "max": 4,
                        "min": 4,
                        "collect": [],
                    },
                ),
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
                ),
            ]
            assert updated == [
                (
                    key,
                    {
                        "start": 5,
                        "end": 15,
                        "count": 2,
                        "sum": 6,
                        "mean": 3.0,
                        "max": 4,
                        "min": 2,
                        "collect": [],
                    },
                ),
                (
                    key,
                    {
                        "start": 10,
                        "end": 20,
                        "count": 1,
                        "sum": 2,
                        "mean": 2,
                        "max": 2,
                        "min": 2,
                        "collect": [],
                    },
                ),
            ]

        # Update window definition
        # * delete an aggregation (min)
        # * change aggregation but keep the name with new aggregation (mean -> max)
        # * add new aggregations (sum2, collect2)
        window = hopping_window_definition_factory(duration_ms=10, step_ms=5).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Max(),
            max=agg.Max(),
            collect=agg.Collect(),
            sum2=agg.Sum(),
            collect2=agg.Collect(),
        )
        assert window.name == "hopping_window_10_5"  # still the same window and store
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=16
            )
            assert (
                expired
                == [
                    (
                        key,
                        {
                            "start": 5,
                            "end": 15,
                            "count": 2,
                            "sum": 6,
                            "sum2": 0,  # sum2 only aggregates the values after the update. Use initial value.
                            "mean": None,  # mean was replace by max. The aggregation restarts with the new values. Use initial value.
                            "max": 4,
                            "collect": [4, 2],
                            "collect2": [
                                4,
                                2,
                            ],  # Collect2 has all the values as they were fully collected before the update
                        },
                    )
                ]
            )
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
                    ),
                    (
                        key,
                        {
                            "start": 15,
                            "end": 25,
                            "count": 1,
                            "sum": 1,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 1,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                ]
            )

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_hoppingwindow_count(
        self, expiration, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.count()
        assert window.name == "hopping_window_10_5_count"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 2
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 2
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_hoppingwindow_sum(
        self, expiration, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.sum()
        assert window.name == "hopping_window_10_5_sum"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 3
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 3
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_hoppingwindow_mean(
        self, expiration, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.mean()
        assert window.name == "hopping_window_10_5_mean"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 1.5
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 1.5
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_hoppingwindow_reduce(
        self, expiration, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        assert window.name == "hopping_window_10_5_reduce"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == [1]
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == [1]
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_hoppingwindow_max(
        self, expiration, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.max()
        assert window.name == "hopping_window_10_5_max"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 1
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 1
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_hoppingwindow_min(
        self, expiration, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.min()
        assert window.name == "hopping_window_10_5_min"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0][1]["value"] == 1
        assert updated[0][1]["start"] == 95
        assert updated[0][1]["end"] == 105

        assert updated[1][1]["value"] == 1
        assert updated[1][1]["start"] == 100
        assert updated[1][1]["end"] == 110
        assert not expired

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_hoppingwindow_collect(
        self, expiration, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.collect()
        assert window.name == "hopping_window_10_5_collect"

        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            process(window, value=1, key=key, transaction=tx, timestamp_ms=100)
            process(window, value=2, key=key, transaction=tx, timestamp_ms=100)
            process(window, value=3, key=key, transaction=tx, timestamp_ms=101)
            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=110
            )

        assert not updated
        assert expired == [
            (key, {"start": 95, "end": 105, "value": [1, 2, 3]}),
            (key, {"start": 100, "end": 110, "value": [1, 2, 3]}),
        ]

    @pytest.mark.parametrize(
        "duration, grace, step, name",
        [
            (-10, 5, 3, "test"),  # duration < 0
            (10, -5, 3, "test"),  # grace < 0
            (10, 5, -1, "test"),  # step < 0
            (10, 5, 0, "test"),  # step == 0
            (10, 5, 10, "test"),  # step == duration
            (10, 5, 11, "test"),  # step > duration
        ],
    )
    def test_hopping_window_def_init_invalid(
        self, duration, grace, step, name, dataframe_factory
    ):
        with pytest.raises(ValueError):
            HoppingTimeWindowDefinition(
                duration_ms=duration,
                grace_ms=grace,
                step_ms=step,
                name=name,
                dataframe=dataframe_factory(),
            )

    @pytest.mark.parametrize("expiration", ("key", "partition"))
    def test_hopping_window_process_window_expired(
        self,
        expiration,
        hopping_window_definition_factory,
        state_manager,
    ):
        window_def = hopping_window_definition_factory(
            duration_ms=10, grace_ms=0, step_ms=5
        )
        window = window_def.sum()
        window.final(closing_strategy=expiration)
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"
        with store.start_partition_transaction(0) as tx:
            # Add item to the windows [95, 105) and [100, 110)
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 1
            assert updated[0][1]["start"] == 95
            assert updated[0][1]["end"] == 105
            assert updated[1][1]["value"] == 1
            assert updated[1][1]["start"] == 100
            assert updated[1][1]["end"] == 110

            assert not expired

            # Now add item to the windows [105, 115) and [110, 120)
            # The windows [95, 105) and [100, 110) are now expired
            # and should be returned
            _, expired = process(
                window, value=2, key=key, transaction=tx, timestamp_ms=110
            )
            expired = expired
            assert len(expired) == 2
            assert expired[0][1]["value"] == 1
            assert expired[0][1]["start"] == 95
            assert expired[0][1]["end"] == 105
            assert expired[1][1]["value"] == 1
            assert expired[1][1]["start"] == 100
            assert expired[1][1]["end"] == 110

    def test_hopping_partition_expiration(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(
            duration_ms=10, grace_ms=2, step_ms=5
        )
        window = window_def.sum()
        window.final(closing_strategy="partition")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            # items for key1
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=100)
            process(window, value=3, key=key1, transaction=tx, timestamp_ms=106)

            # items for key2
            process(window, value=5, key=key2, transaction=tx, timestamp_ms=102)
            process(window, value=7, key=key2, transaction=tx, timestamp_ms=108)

            # don't expire key1 due to grace period
            updated, expired = process(
                window, value=2, key=key1, transaction=tx, timestamp_ms=110
            )

            assert updated == [
                (key1, {"start": 105, "end": 115, "value": 5}),
                (key1, {"start": 110, "end": 120, "value": 2}),
            ]
            assert expired == []

            # an older key 2 is accepted
            updated, expired = process(
                window, value=2, key=key2, transaction=tx, timestamp_ms=109
            )
            assert updated == [
                (key2, {"start": 100, "end": 110, "value": 14}),
                (key2, {"start": 105, "end": 115, "value": 9}),
            ]
            assert expired == []

            # expire key1
            updated, expired = process(
                window, value=3, key=key1, transaction=tx, timestamp_ms=112
            )
            assert updated == [
                (key1, {"start": 105, "end": 115, "value": 8}),
                (key1, {"start": 110, "end": 120, "value": 5}),
            ]
            assert expired == [
                (key1, {"start": 100, "end": 110, "value": 4}),
                (key2, {"start": 100, "end": 110, "value": 14}),
            ]

    def test_hopping_key_expiration_to_partition(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(
            duration_ms=10, grace_ms=0, step_ms=5
        )
        window = window_def.sum()
        window.final(closing_strategy="key")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            process(window, value=1, key=key1, transaction=tx, timestamp_ms=100)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=102)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=105)
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=106)

        window._closing_strategy = ClosingStrategy.PARTITION
        with store.start_partition_transaction(0) as tx:
            key3 = b"key3"

            process(window, value=1, key=key1, transaction=tx, timestamp_ms=107)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=108)
            updated, expired = process(
                window, value=1, key=key3, transaction=tx, timestamp_ms=114
            )

        assert updated == [
            (key3, {"start": 105, "end": 115, "value": 1}),
            (key3, {"start": 110, "end": 120, "value": 1}),
        ]
        assert expired == [
            (key1, {"start": 100, "end": 110, "value": 3}),
            (key2, {"start": 100, "end": 110, "value": 3}),
        ]

    def test_hopping_partition_expiration_to_key(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(
            duration_ms=10, grace_ms=0, step_ms=5
        )
        window = window_def.sum()
        window.final(closing_strategy="partition")
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key1 = b"key1"
            key2 = b"key2"

            process(window, value=1, key=key1, transaction=tx, timestamp_ms=100)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=102)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=105)
            process(window, value=1, key=key1, transaction=tx, timestamp_ms=106)

        window._closing_strategy = ClosingStrategy.KEY
        with store.start_partition_transaction(0) as tx:
            key3 = b"key3"

            process(window, value=1, key=key1, transaction=tx, timestamp_ms=107)
            process(window, value=1, key=key2, transaction=tx, timestamp_ms=108)
            updated, expired = process(
                window, value=1, key=key3, transaction=tx, timestamp_ms=114
            )

            assert updated == [
                (key3, {"start": 105, "end": 115, "value": 1}),
                (key3, {"start": 110, "end": 120, "value": 1}),
            ]
            assert expired == []

            updated, expired = process(
                window, value=1, key=key1, transaction=tx, timestamp_ms=116
            )
            assert updated == [
                (key1, {"start": 110, "end": 120, "value": 1}),
                (key1, {"start": 115, "end": 125, "value": 1}),
            ]
            assert expired == [
                (key1, {"start": 100, "end": 110, "value": 3}),
                (key1, {"start": 105, "end": 115, "value": 2}),
            ]


@pytest.fixture()
def count_hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(count: int, step: int) -> HoppingCountWindowDefinition:
        sdf = dataframe_factory(
            state_manager=state_manager, registry=DataFrameRegistry()
        )
        window_def = HoppingCountWindowDefinition(dataframe=sdf, count=count, step=step)
        return window_def

    return factory


class TestCountHoppingWindow:
    @pytest.mark.parametrize(
        "count, step, name",
        [
            (-10, 1, "test"),
            (0, 1, "test"),
            (1, 1, "test"),
            (2, 0, "test"),
            (2, -1, "test"),
        ],
    )
    def test_init_invalid(self, count, step, name, dataframe_factory):
        with pytest.raises(ValueError):
            HoppingCountWindowDefinition(
                count=count,
                step=step,
                name=name,
                dataframe=dataframe_factory(),
            )

    def test_multiaggregation(
        self,
        count_hopping_window_definition_factory,
        state_manager,
    ):
        window = count_hopping_window_definition_factory(count=3, step=2).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Mean(),
            max=agg.Max(),
            min=agg.Min(),
            collect=agg.Collect(),
        )
        window.final()
        assert window.name == "hopping_count_window"

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
                        "start": 2,
                        "end": 2,
                        "count": 1,
                        "sum": 1,
                        "mean": 1.0,
                        "max": 1,
                        "min": 1,
                        "collect": [],
                    },
                ),
            ]

            updated, expired = process(
                window, value=5, key=key, transaction=tx, timestamp_ms=6
            )
            assert not expired
            assert updated == [
                (
                    key,
                    {
                        "start": 2,
                        "end": 6,
                        "count": 2,
                        "sum": 6,
                        "mean": 3.0,
                        "max": 5,
                        "min": 1,
                        "collect": [],
                    },
                ),
            ]

            updated, expired = process(
                window, value=3, key=key, transaction=tx, timestamp_ms=12
            )
            assert expired == [
                (
                    key,
                    {
                        "start": 2,
                        "end": 12,
                        "count": 3,
                        "sum": 9,
                        "mean": 3.0,
                        "max": 5,
                        "min": 1,
                        "collect": [1, 5, 3],
                    },
                ),
            ]
            assert updated == [
                (
                    key,
                    {
                        "start": 2,
                        "end": 12,
                        "count": 3,
                        "sum": 9,
                        "mean": 3,
                        "max": 5,
                        "min": 1,
                        "collect": [],
                    },
                ),
                (
                    key,
                    {
                        "start": 12,
                        "end": 12,
                        "count": 1,
                        "sum": 3,
                        "mean": 3,
                        "max": 3,
                        "min": 3,
                        "collect": [],
                    },
                ),
            ]

        # Update window definition
        # * delete an aggregation (min)
        # * change aggregation but keep the name with new aggregation (mean -> max)
        # * add new aggregations (sum2, collect2)
        window = count_hopping_window_definition_factory(count=3, step=2).agg(
            count=agg.Count(),
            sum=agg.Sum(),
            mean=agg.Max(),
            max=agg.Max(),
            collect=agg.Collect(),
            sum2=agg.Sum(),
            collect2=agg.Collect(),
        )
        assert window.name == "hopping_count_window"  # still the same window and store
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, value=1, key=key, transaction=tx, timestamp_ms=16
            )
            assert not expired
            assert (
                updated
                == [
                    (
                        key,
                        {
                            "start": 12,
                            "end": 16,
                            "count": 2,
                            "sum": 4,
                            "sum2": 1,  # sum2 only aggregates the values after the update
                            "mean": 1,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 3,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                ]
            )

            updated, expired = process(
                window, value=4, key=key, transaction=tx, timestamp_ms=22
            )
            assert (
                expired
                == [
                    (
                        key,
                        {
                            "start": 12,
                            "end": 22,
                            "count": 3,
                            "sum": 8,
                            "sum2": 5,  # sum2 only aggregates the values after the update
                            "mean": 4,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 4,
                            "collect": [3, 1, 4],
                            "collect2": [3, 1, 4],
                        },
                    ),
                ]
            )
            assert (
                updated
                == [
                    (
                        key,
                        {
                            "start": 12,
                            "end": 22,
                            "count": 3,
                            "sum": 8,
                            "sum2": 5,  # sum2 only aggregates the values after the update
                            "mean": 4,  # mean was replace by max. The aggregation restarts with the new values.
                            "max": 4,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                    (
                        key,
                        {
                            "start": 22,
                            "end": 22,
                            "count": 1,
                            "sum": 4,
                            "sum2": 4,
                            "mean": 4,
                            "max": 4,
                            "collect": [],
                            "collect2": [],
                        },
                    ),
                ]
            )

    def test_count(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.count()
        assert window.name == "hopping_count_window_count"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert expired == []

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4
            assert updated[1][1]["value"] == 2
            assert len(expired) == 1
            assert expired[0][1]["value"] == 4

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 1
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=0, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4
            assert updated[1][1]["value"] == 2
            assert len(expired) == 1
            assert expired[0][1]["value"] == 4

    def test_sum(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.sum()
        assert window.name == "hopping_count_window_sum"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 3  # 1 + 2
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 6  # 1 + 2 + 3
            assert updated[1][1]["value"] == 3
            assert expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 10  # 1 + 2 + 3 + 4
            assert updated[1][1]["value"] == 7  # 3 + 4
            assert len(expired) == 1
            assert expired[0][1]["value"] == 10

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 12  # 3 + 4 + 5
            assert updated[1][1]["value"] == 5
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 18  # 3 + 4 + 5 + 6
            assert updated[1][1]["value"] == 11  # 5 + 6
            assert len(expired) == 1
            assert expired[0][1]["value"] == 18

    def test_mean(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.mean()
        assert window.name == "hopping_count_window_mean"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1.5  # (1 + 2) / 2
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 2  # (1 + 2 + 3) / 3
            assert updated[1][1]["value"] == 3
            assert expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 2.5  # (1 + 2 + 3 + 4) / 4
            assert updated[1][1]["value"] == 3.5  # 3 + 4
            assert len(expired) == 1
            assert expired[0][1]["value"] == 2.5

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4  # (3 + 4 + 5) / 3
            assert updated[1][1]["value"] == 5
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert (
                updated[0][1]["value"] == 4.5
            )  # (3 # sum2 only aggregates the values after the update + 6) / 2
            assert len(expired) == 1
            assert expired[0][1]["value"] == 4.5

    def test_reduce(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        assert window.name == "hopping_count_window_reduce"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == [1]
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == [1, 2]
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == [1, 2, 3]
            assert updated[1][1]["value"] == [3]
            assert expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == [1, 2, 3, 4]
            assert updated[1][1]["value"] == [3, 4]
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3, 4]

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == [3, 4, 5]
            assert updated[1][1]["value"] == [5]
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == [3, 4, 5, 6]
            assert updated[1][1]["value"] == [5, 6]
            assert len(expired) == 1
            assert expired[0][1]["value"] == [3, 4, 5, 6]

    def test_max(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.max()
        assert window.name == "hopping_count_window_max"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4
            assert updated[1][1]["value"] == 4
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 4
            assert updated[1][1]["value"] == 4
            assert len(expired) == 1
            assert expired[0][1]["value"] == 4

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 5
            assert updated[1][1]["value"] == 5
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 6
            assert updated[1][1]["value"] == 6
            assert len(expired) == 1
            assert expired[0][1]["value"] == 6

    def test_min(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.min()
        assert window.name == "hopping_count_window_min"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 4
            assert expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0][1]["value"] == 2
            assert expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 2
            assert updated[1][1]["value"] == 3
            assert expired == []

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 2
            assert updated[1][1]["value"] == 3
            assert len(expired) == 1
            assert expired[0][1]["value"] == 2

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 6
            assert len(expired) == 0

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 5
            assert len(expired) == 1
            assert expired[0][1]["value"] == 3

    def test_collect(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.collect()
        assert window.name == "hopping_count_window_collect"

        window.final()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3, 4]

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [3, 4, 5, 6]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix="")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == [5, 6]

    def test_unaligned_steps(
        self, count_hopping_window_definition_factory, state_manager
    ):
        window_def = count_hopping_window_definition_factory(count=5, step=2)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="", value=1, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=2, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=3, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=4, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=5, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [1, 2, 3, 4, 5]

            updated, expired = process(
                window, key="", value=6, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=7, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [3, 4, 5, 6, 7]

            updated, expired = process(
                window, key="", value=8, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=9, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [5, 6, 7, 8, 9]

            updated, expired = process(
                window, key="", value=10, transaction=tx, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = process(
                window, key="", value=11, transaction=tx, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0][1]["value"] == [7, 8, 9, 10, 11]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix="")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == [9, 10, 11]

    def test_multiple_keys_sum(
        self, count_hopping_window_definition_factory, state_manager
    ):
        window_def = count_hopping_window_definition_factory(count=3, step=1)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="key1", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 1
            assert updated[0][1]["value"] == 1
            updated, expired = process(
                window, key="key2", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 1
            assert updated[0][1]["value"] == 5

            updated, expired = process(
                window, key="key1", value=2, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 2
            assert updated[0][1]["value"] == 3
            assert updated[1][1]["value"] == 2

            updated, expired = process(
                window, key="key2", value=4, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 2
            assert updated[0][1]["value"] == 9
            assert updated[1][1]["value"] == 4

            updated, expired = process(
                window, key="key1", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == 6
            assert len(updated) == 3
            assert updated[0][1]["value"] == 6
            assert updated[1][1]["value"] == 5
            assert updated[2][1]["value"] == 3

            updated, expired = process(
                window, key="key1", value=4, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == 9
            assert len(updated) == 3
            assert updated[0][1]["value"] == 9
            assert updated[1][1]["value"] == 7
            assert updated[2][1]["value"] == 4

            updated, expired = process(
                window, key="key2", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == 12
            assert len(updated) == 3
            assert updated[0][1]["value"] == 12
            assert updated[1][1]["value"] == 7
            assert updated[2][1]["value"] == 3

            updated, expired = process(
                window, key="key2", value=2, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == 9
            assert len(updated) == 3
            assert updated[0][1]["value"] == 9
            assert updated[1][1]["value"] == 5
            assert updated[2][1]["value"] == 2

            updated, expired = process(
                window, key="key1", value=5, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == 12
            assert len(updated) == 3
            assert updated[0][1]["value"] == 12
            assert updated[1][1]["value"] == 9
            assert updated[2][1]["value"] == 5

            updated, expired = process(
                window, key="key2", value=1, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == 6
            assert len(updated) == 3
            assert updated[0][1]["value"] == 6
            assert updated[1][1]["value"] == 3
            assert updated[2][1]["value"] == 1

    def test_multiple_keys_collect(
        self, count_hopping_window_definition_factory, state_manager
    ):
        window_def = count_hopping_window_definition_factory(count=3, step=1)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(stream_id="test", store_name=window.name)
        store.assign_partition(0)

        with store.start_partition_transaction(0) as tx:
            updated, expired = process(
                window, key="key1", value=1, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 0
            updated, expired = process(
                window, key="key2", value=5, transaction=tx, timestamp_ms=100
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=2, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 0
            updated, expired = process(
                window, key="key2", value=4, transaction=tx, timestamp_ms=110
            )
            assert len(expired) == 0
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == [1, 2, 3]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key1", value=4, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == [2, 3, 4]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=3, transaction=tx, timestamp_ms=120
            )
            assert expired[0][1]["value"] == [5, 4, 3]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=2, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == [4, 3, 2]
            assert len(updated) == 0
            updated, expired = process(
                window, key="key1", value=5, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == [3, 4, 5]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=1, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == [3, 2, 1]
            assert len(updated) == 0

            updated, expired = process(
                window, key="key2", value=0, transaction=tx, timestamp_ms=130
            )
            assert expired[0][1]["value"] == [2, 1, 0]
            assert len(updated) == 0
            updated, expired = process(
                window, key="key1", value=6, transaction=tx, timestamp_ms=140
            )
            assert expired[0][1]["value"] == [4, 5, 6]
            assert len(updated) == 0
