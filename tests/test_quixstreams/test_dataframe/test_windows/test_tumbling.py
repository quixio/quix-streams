import pytest

from quixstreams.dataframe.windows import (
    CountTumblingWindowDefinition,
    FixedTimeTumblingWindowDefinition,
)


@pytest.fixture()
def tumbling_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        duration_ms: int, grace_ms: int = 0
    ) -> FixedTimeTumblingWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
        window_def = FixedTimeTumblingWindowDefinition(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=sdf
        )
        return window_def

    return factory


class TestFixedTimeTumblingWindow:
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
        twd = FixedTimeTumblingWindowDefinition(
            duration_ms=duration,
            grace_ms=grace,
            dataframe=dataframe_factory(),
            name=provided_name,
        )
        name = twd._get_name(func_name)
        assert name == expected_name

    def test_tumblingwindow_count(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.count()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=0, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=0, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 2
        assert not expired

    def test_tumblingwindow_sum(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 3
        assert not expired

    def test_tumblingwindow_mean(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.mean()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 1.5
        assert not expired

    def test_tumblingwindow_reduce(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == [2, 1]
        assert not expired

    def test_tumblingwindow_max(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.max()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 2
        assert not expired

    def test_tumblingwindow_min(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.min()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 1
        assert not expired

    def test_tumblingwindow_collect(
        self, tumbling_window_definition_factory, state_manager
    ):
        window_def = tumbling_window_definition_factory(duration_ms=10, grace_ms=5)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=1, state=state, timestamp_ms=100)
            window.process_window(value=2, state=state, timestamp_ms=100)
            window.process_window(value=3, state=state, timestamp_ms=101)
            updated, expired = window.process_window(
                value=4, state=state, timestamp_ms=200
            )
        assert not updated
        assert expired == [{"start": 100, "end": 110, "value": [1, 2, 3]}]

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
            FixedTimeTumblingWindowDefinition(
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
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            # Add item to the window [100, 110)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert updated[0]["start"] == 100
            assert updated[0]["end"] == 110
            assert not expired

            # Now add item to the window [110, 120)
            # The window [100, 110) is now expired and should be returned
            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=110
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 2
            assert updated[0]["start"] == 110
            assert updated[0]["end"] == 120

            assert len(expired) == 1
            assert expired[0]["value"] == 1
            assert expired[0]["start"] == 100
            assert expired[0]["end"] == 110


@pytest.fixture()
def count_tumbling_window_definition_factory(state_manager, dataframe_factory):
    def factory(count: int) -> CountTumblingWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
        window_def = CountTumblingWindowDefinition(dataframe=sdf, count=count)
        return window_def

    return factory


class TestCountTumblingWindow:
    @pytest.mark.parametrize(
        "count, name",
        [
            (-10, "test"),
            (0, "test"),
            (1, "test"),
        ],
    )
    def test_init_invalid(self, count, name, dataframe_factory):
        with pytest.raises(ValueError):
            CountTumblingWindowDefinition(
                count=count,
                name=name,
                dataframe=dataframe_factory(),
            )

    def test_count(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.count()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=0, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=0, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 2
        assert not expired

    def test_sum(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 3
        assert not expired

    def test_mean(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.mean()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 1.5
        assert not expired

    def test_reduce(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.reduce(
            reducer=lambda agg, current: agg + [current],
            initializer=lambda value: [value],
        )
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == [2, 1]
        assert not expired

    def test_max(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.max()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 2
        assert not expired

    def test_min(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=10)
        window = window_def.min()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 1
        assert updated[0]["value"] == 1
        assert not expired

    def test_window_expired(
        self,
        count_tumbling_window_definition_factory,
        state_manager,
    ):
        window_def = count_tumbling_window_definition_factory(count=2)
        window = window_def.sum()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            # Add first item to the window
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 1
            assert updated[0]["start"] == 100
            assert updated[0]["end"] == 100
            assert not expired

            # Now add second item to the window
            # The window is now expired and should be returned
            updated, expired = window.process_window(
                value=2, state=state, timestamp_ms=110
            )
            assert len(updated) == 1
            assert updated[0]["value"] == 3
            assert updated[0]["start"] == 100
            assert updated[0]["end"] == 110

            assert len(expired) == 1
            assert expired[0]["value"] == 3
            assert expired[0]["start"] == 100
            assert expired[0]["end"] == 110

    def test_collect(self, count_tumbling_window_definition_factory, state_manager):
        window_def = count_tumbling_window_definition_factory(count=3)
        window = window_def.collect()
        window.register_store()
        store = state_manager.get_store(topic="test", store_name=window.name)
        store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            window.process_window(value=1, state=state, timestamp_ms=100)
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=3, state=state, timestamp_ms=101
            )
        assert not updated
        assert expired == [{"start": 100, "end": 101, "value": [1, 2, 3]}]
