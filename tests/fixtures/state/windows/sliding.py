from contextlib import contextmanager
from unittest import mock

import pytest

from quixstreams.dataframe.windows import SlidingWindowDefinition


@pytest.fixture
def mock_message_context():
    with mock.patch("quixstreams.dataframe.windows.time_based.message_context"):
        yield


@pytest.fixture
def sliding_window_definition_factory(
    state_manager, dataframe_factory, topic_manager_topic_factory
):
    def factory(duration_ms: int, grace_ms: int) -> SlidingWindowDefinition:
        topic = topic_manager_topic_factory("topic")
        sdf = dataframe_factory(topic=topic, state_manager=state_manager)
        return SlidingWindowDefinition(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=sdf
        )

    return factory


@pytest.fixture
def window_factory(sliding_window_definition_factory):
    def factory(duration_ms: int, grace_ms: int):
        window_definition = sliding_window_definition_factory(
            duration_ms=duration_ms, grace_ms=grace_ms
        )
        window = window_definition.reduce(
            reducer=lambda agg, value: agg + [value],
            initializer=lambda value: [value],
        )
        window.register_store()
        return window

    return factory


@pytest.fixture
def state_factory(state_manager):
    store = None

    @contextmanager
    def factory(window):
        nonlocal store
        if store is None:
            store = state_manager.get_store(topic="topic", store_name=window.name)
            store.assign_partition(0)
        with store.start_partition_transaction(0) as tx:
            yield tx.as_state(prefix=b"key")

    return factory
