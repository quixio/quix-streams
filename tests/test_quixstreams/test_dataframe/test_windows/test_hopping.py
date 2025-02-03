import pytest

from quixstreams.dataframe.windows import (
    HoppingCountWindowDefinition,
    HoppingTimeWindowDefinition,
)


@pytest.fixture()
def hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        duration_ms: int, step_ms: int, grace_ms: int = 0
    ) -> HoppingTimeWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
        window_def = HoppingTimeWindowDefinition(
            duration_ms=duration_ms, step_ms=step_ms, grace_ms=grace_ms, dataframe=sdf
        )
        return window_def

    return factory


class TestHoppingWindow:
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

    def test_hoppingwindow_count(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.count()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            state = tx.as_state(prefix=key)
            window.process_window(value=2, key=key, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, key=key, state=state, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0]["value"] == 2
        assert updated[0]["start"] == 95
        assert updated[0]["end"] == 105

        assert updated[1]["value"] == 2
        assert updated[1]["start"] == 100
        assert updated[1]["end"] == 110
        assert not expired

    def test_hoppingwindow_sum(self, hopping_window_definition_factory, state_manager):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            state = tx.as_state(prefix=key)
            window.process_window(value=2, key=key, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, key=key, state=state, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0]["value"] == 3
        assert updated[0]["start"] == 95
        assert updated[0]["end"] == 105

        assert updated[1]["value"] == 3
        assert updated[1]["start"] == 100
        assert updated[1]["end"] == 110
        assert not expired

    def test_hoppingwindow_mean(self, hopping_window_definition_factory, state_manager):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.mean()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            state = tx.as_state(prefix=key)
            window.process_window(value=2, key=key, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, key=key, state=state, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0]["value"] == 1.5
        assert updated[0]["start"] == 95
        assert updated[0]["end"] == 105

        assert updated[1]["value"] == 1.5
        assert updated[1]["start"] == 100
        assert updated[1]["end"] == 110
        assert not expired

    def test_hoppingwindow_reduce(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            state = tx.as_state(prefix=key)
            updated, expired = window.process_window(
                value=1, key=key, state=state, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0]["value"] == [1]
        assert updated[0]["start"] == 95
        assert updated[0]["end"] == 105

        assert updated[1]["value"] == [1]
        assert updated[1]["start"] == 100
        assert updated[1]["end"] == 110
        assert not expired

    def test_hoppingwindow_max(self, hopping_window_definition_factory, state_manager):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.max()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            state = tx.as_state(prefix=key)
            updated, expired = window.process_window(
                value=1, key=key, state=state, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0]["value"] == 1
        assert updated[0]["start"] == 95
        assert updated[0]["end"] == 105

        assert updated[1]["value"] == 1
        assert updated[1]["start"] == 100
        assert updated[1]["end"] == 110
        assert not expired

    def test_hoppingwindow_min(self, hopping_window_definition_factory, state_manager):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.min()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            state = tx.as_state(prefix=key)
            updated, expired = window.process_window(
                value=1, key=key, state=state, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0]["value"] == 1
        assert updated[0]["start"] == 95
        assert updated[0]["end"] == 105

        assert updated[1]["value"] == 1
        assert updated[1]["start"] == 100
        assert updated[1]["end"] == 110
        assert not expired

    def test_hoppingwindow_collect(
        self, hopping_window_definition_factory, state_manager
    ):
        window_def = hopping_window_definition_factory(duration_ms=10, step_ms=5)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            key = b"key"
            state = tx.as_state(prefix=key)
            window.process_window(value=1, key=key, state=state, timestamp_ms=100)
            window.process_window(value=2, key=key, state=state, timestamp_ms=100)
            window.process_window(value=3, key=key, state=state, timestamp_ms=101)
            updated, expired = window.process_window(
                value=4, key=key, state=state, timestamp_ms=110
            )

        assert not updated
        assert expired == [
            {"start": 95, "end": 105, "value": [1, 2, 3]},
            {"start": 100, "end": 110, "value": [1, 2, 3]},
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

    def test_hopping_window_process_window_expired(
        self,
        hopping_window_definition_factory,
        state_manager,
    ):
        window_def = hopping_window_definition_factory(
            duration_ms=10, grace_ms=0, step_ms=5
        )
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        key = b"key"
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=key)
            # Add item to the windows [95, 105) and [100, 110)
            updated, expired = window.process_window(
                value=1, key=key, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 1
            assert updated[0]["start"] == 95
            assert updated[0]["end"] == 105
            assert updated[1]["value"] == 1
            assert updated[1]["start"] == 100
            assert updated[1]["end"] == 110

            assert not expired

            # Now add item to the windows [105, 115) and [110, 120)
            # The windows [95, 105) and [100, 110) are now expired
            # and should be returned
            _, expired = window.process_window(
                value=2, key=key, state=state, timestamp_ms=110
            )
            assert len(expired) == 2
            assert expired[0]["value"] == 1
            assert expired[0]["start"] == 95
            assert expired[0]["end"] == 105
            assert expired[1]["value"] == 1
            assert expired[1]["start"] == 100
            assert expired[1]["end"] == 110


@pytest.fixture()
def count_hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(count: int, step: int) -> HoppingCountWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
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

    def test_count(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.count()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                key="", value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                key="", value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 2
            assert expired == []

            updated, expired = window.process_window(
                key="", value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 3
            assert updated[1]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                key="", value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4
            assert updated[1]["value"] == 2
            assert len(expired) == 1
            assert expired[0]["value"] == 4

            updated, expired = window.process_window(
                key="", value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 3
            assert updated[1]["value"] == 1
            assert len(expired) == 0

            updated, expired = window.process_window(
                key="", value=0, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4
            assert updated[1]["value"] == 2
            assert len(expired) == 1
            assert expired[0]["value"] == 4

    def test_sum(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                key="", value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                key="", value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 3  # 1 + 2
            assert expired == []

            updated, expired = window.process_window(
                key="", value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 6  # 1 + 2 + 3
            assert updated[1]["value"] == 3
            assert expired == []

            updated, expired = window.process_window(
                key="", value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 10  # 1 + 2 + 3 + 4
            assert updated[1]["value"] == 7  # 3 + 4
            assert len(expired) == 1
            assert expired[0]["value"] == 10

            updated, expired = window.process_window(
                key="", value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 12  # 3 + 4 + 5
            assert updated[1]["value"] == 5
            assert len(expired) == 0

            updated, expired = window.process_window(
                key="", value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 18  # 3 + 4 + 5 + 6
            assert updated[1]["value"] == 11  # 5 + 6
            assert len(expired) == 1
            assert expired[0]["value"] == 18

    def test_mean(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.mean()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                key="", value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                key="", value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1.5  # (1 + 2) / 2
            assert expired == []

            updated, expired = window.process_window(
                key="", value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 2  # (1 + 2 + 3) / 3
            assert updated[1]["value"] == 3
            assert expired == []

            updated, expired = window.process_window(
                key="", value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 2.5  # (1 + 2 + 3 + 4) / 4
            assert updated[1]["value"] == 3.5  # 3 + 4
            assert len(expired) == 1
            assert expired[0]["value"] == 2.5

            updated, expired = window.process_window(
                key="", value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4  # (3 + 4 + 5) / 3
            assert updated[1]["value"] == 5
            assert len(expired) == 0

            updated, expired = window.process_window(
                key="", value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4.5  # (3 + 4 + 5 + 6) / 4
            assert updated[1]["value"] == 5.5  # (5 + 6) / 2
            assert len(expired) == 1
            assert expired[0]["value"] == 4.5

    def test_reduce(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                key="", value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == [1]
            assert expired == []

            updated, expired = window.process_window(
                key="", value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == [1, 2]
            assert expired == []

            updated, expired = window.process_window(
                key="", value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == [1, 2, 3]
            assert updated[1]["value"] == [3]
            assert expired == []

            updated, expired = window.process_window(
                key="", value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == [1, 2, 3, 4]
            assert updated[1]["value"] == [3, 4]
            assert len(expired) == 1
            assert expired[0]["value"] == [1, 2, 3, 4]

            updated, expired = window.process_window(
                key="", value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == [3, 4, 5]
            assert updated[1]["value"] == [5]
            assert len(expired) == 0

            updated, expired = window.process_window(
                key="", value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == [3, 4, 5, 6]
            assert updated[1]["value"] == [5, 6]
            assert len(expired) == 1
            assert expired[0]["value"] == [3, 4, 5, 6]

    def test_max(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.max()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                key="", value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert expired == []

            updated, expired = window.process_window(
                key="", value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 2
            assert expired == []

            updated, expired = window.process_window(
                key="", value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4
            assert updated[1]["value"] == 4
            assert expired == []

            updated, expired = window.process_window(
                key="", value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 4
            assert updated[1]["value"] == 4
            assert len(expired) == 1
            assert expired[0]["value"] == 4

            updated, expired = window.process_window(
                key="", value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 5
            assert updated[1]["value"] == 5
            assert len(expired) == 0

            updated, expired = window.process_window(
                key="", value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 6
            assert updated[1]["value"] == 6
            assert len(expired) == 1
            assert expired[0]["value"] == 6

    def test_min(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.min()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                key="", value=4, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 4
            assert expired == []

            updated, expired = window.process_window(
                key="", value=2, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 2
            assert expired == []

            updated, expired = window.process_window(
                key="", value=3, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 2
            assert updated[1]["value"] == 3
            assert expired == []

            updated, expired = window.process_window(
                key="", value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 2
            assert updated[1]["value"] == 3
            assert len(expired) == 1
            assert expired[0]["value"] == 2

            updated, expired = window.process_window(
                key="", value=6, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 3
            assert updated[1]["value"] == 6
            assert len(expired) == 0

            updated, expired = window.process_window(
                key="", value=5, state=state, timestamp_ms=100
            )
            assert len(updated) == 2
            assert updated[0]["value"] == 3
            assert updated[1]["value"] == 5
            assert len(expired) == 1
            assert expired[0]["value"] == 3

    def test_collect(self, count_hopping_window_definition_factory, state_manager):
        window_def = count_hopping_window_definition_factory(count=4, step=2)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                key="", value=1, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=2, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=3, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=4, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [1, 2, 3, 4]

            updated, expired = window.process_window(
                key="", value=5, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=6, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [3, 4, 5, 6]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == [5, 6]

    def test_unaligned_steps(
        self, count_hopping_window_definition_factory, state_manager
    ):
        window_def = count_hopping_window_definition_factory(count=5, step=2)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                key="", value=1, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=2, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=3, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=4, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=5, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [1, 2, 3, 4, 5]

            updated, expired = window.process_window(
                key="", value=6, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=7, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [3, 4, 5, 6, 7]

            updated, expired = window.process_window(
                key="", value=8, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=9, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [5, 6, 7, 8, 9]

            updated, expired = window.process_window(
                key="", value=10, state=state, timestamp_ms=100
            )
            assert updated == expired == []

            updated, expired = window.process_window(
                key="", value=11, state=state, timestamp_ms=100
            )
            assert updated == []
            assert len(expired) == 1
            assert expired[0]["value"] == [7, 8, 9, 10, 11]

        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            remaining_items = state.get_from_collection(start=0, end=1000)
            assert remaining_items == [9, 10, 11]
