import pytest

from quixstreams.dataframe.windows import HoppingWindowDefinition


@pytest.fixture()
def hopping_window_definition_factory(state_manager, dataframe_factory):
    def factory(
        duration_ms: int, step_ms: int, grace_ms: int = 0
    ) -> HoppingWindowDefinition:
        sdf = dataframe_factory(state_manager=state_manager)
        window_def = HoppingWindowDefinition(
            duration_ms=duration_ms, step_ms=step_ms, grace_ms=grace_ms, dataframe=sdf
        )
        return window_def

    return factory


class TestHoppingWindow:
    @pytest.mark.parametrize(
        "duration, grace, step, provided_name, func_name, expected_name",
        [
            (10, 5, 3, "custom_window", "sum", "custom_window"),
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
        twd = HoppingWindowDefinition(
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
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
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
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
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
            state = tx.as_state(prefix=b"key")
            window.process_window(value=2, state=state, timestamp_ms=100)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
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
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
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
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
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
            state = tx.as_state(prefix=b"key")
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
            )
        assert len(updated) == 2
        assert updated[0]["value"] == 1
        assert updated[0]["start"] == 95
        assert updated[0]["end"] == 105

        assert updated[1]["value"] == 1
        assert updated[1]["start"] == 100
        assert updated[1]["end"] == 110
        assert not expired

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
            HoppingWindowDefinition(
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
        with store.start_partition_transaction(0) as tx:
            state = tx.as_state(prefix=b"key")
            # Add item to the windows [95, 105) and [100, 110)
            updated, expired = window.process_window(
                value=1, state=state, timestamp_ms=100
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
            _, expired = window.process_window(value=2, state=state, timestamp_ms=110)
            assert len(expired) == 2
            assert expired[0]["value"] == 1
            assert expired[0]["start"] == 95
            assert expired[0]["end"] == 105
            assert expired[1]["value"] == 1
            assert expired[1]["start"] == 100
            assert expired[1]["end"] == 110
