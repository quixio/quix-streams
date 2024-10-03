from collections import namedtuple
from contextlib import contextmanager
from timeit import timeit
from uuid import uuid4

import pytest

from quixstreams.dataframe.windows import SlidingWindowDefinition

Message = namedtuple("Message", ["timestamp", "value", "updated", "expired", "deleted"])

A, B, C, D, E, F, G, H, I = "A", "B", "C", "D", "E", "F", "G", "H", "I"

BASIC_CASE_MESSAGES = [
    Message(
        timestamp=11,
        value=A,
        updated=[
            {"start": 1, "end": 11, "value": [A]},  # left A
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=19,
        value=B,
        updated=[
            {"start": 9, "end": 19, "value": [A, B]},  # left B
            {"start": 12, "end": 22, "value": [B]},  # right A
        ],
        expired=[
            {"start": 1, "end": 11, "value": [A]},  # left A
        ],
        deleted=[],
    ),
    Message(
        timestamp=16,
        value=C,
        updated=[
            {"start": 6, "end": 16, "value": [A, C]},  # left C
            {"start": 9, "end": 19, "value": [A, B, C]},  # left B
            {"start": 12, "end": 22, "value": [B, C]},  # right A
            {"start": 17, "end": 27, "value": [B]},  # right C
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=27,
        value=D,
        updated=[
            {"start": 17, "end": 27, "value": [B, D]},  # right C / left D
            {"start": 20, "end": 30, "value": [D]},  # right B
        ],
        expired=[
            {"start": 6, "end": 16, "value": [A, C]},  # left C
            {"start": 9, "end": 19, "value": [A, B, C]},  # left B
        ],
        deleted=[
            {"start": 1, "end": 11, "value": [A]},  # left A
        ],
    ),
    Message(
        timestamp=22,
        value=E,
        updated=[
            {"start": 12, "end": 22, "value": [B, C, E]},  # right A / left E
            {"start": 17, "end": 27, "value": [B, D, E]},  # right C / left D
            {"start": 20, "end": 30, "value": [D, E]},  # right B
            {"start": 23, "end": 33, "value": [D]},  # right E
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=25,
        value=F,
        updated=[
            {"start": 15, "end": 25, "value": [B, C, E, F]},  # left F
            {"start": 17, "end": 27, "value": [B, D, E, F]},  # right C / left D
            {"start": 20, "end": 30, "value": [D, E, F]},  # right B
            {"start": 23, "end": 33, "value": [D, F]},  # right E
            {"start": 26, "end": 36, "value": [D]},  # right F
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=27,
        value=G,
        updated=[
            {"start": 17, "end": 27, "value": [B, D, E, F, G]},  # right C / left D, G
            {"start": 20, "end": 30, "value": [D, E, F, G]},  # right B
            {"start": 23, "end": 33, "value": [D, F, G]},  # right E
            {"start": 26, "end": 36, "value": [D, G]},  # right F
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=38,
        value=H,
        updated=[
            {"start": 28, "end": 38, "value": [H]},  # left H
        ],
        expired=[
            {"start": 12, "end": 22, "value": [B, C, E]},  # right A / left E
            {"start": 15, "end": 25, "value": [B, C, E, F]},  # left F
            {"start": 17, "end": 27, "value": [B, D, E, F, G]},  # right C / left D
            {"start": 20, "end": 30, "value": [D, E, F, G]},  # right B
        ],
        deleted=[
            {"start": 6, "end": 16, "value": [A, C]},  # left C
            {"start": 9, "end": 19, "value": [A, B, C]},  # left B
        ],
    ),
    Message(
        timestamp=54,
        value=I,
        updated=[
            {"start": 44, "end": 54, "value": [I]},  # left I
        ],
        expired=[
            {"start": 23, "end": 33, "value": [D, F, G]},  # right E
            {"start": 26, "end": 36, "value": [D, G]},  # right F
            {"start": 28, "end": 38, "value": [H]},  # left H
        ],
        deleted=[
            {"start": 12, "end": 22, "value": [B, C, E]},  # right A / left E
            {"start": 15, "end": 25, "value": [B, C, E, F]},  # left F
            {"start": 17, "end": 27, "value": [B, D, E, F, G]},  # right C / left D
            {"start": 20, "end": 30, "value": [D, E, F, G]},  # right B
            {"start": 23, "end": 33, "value": [D, F, G]},  # right E
            {"start": 26, "end": 36, "value": [D, G]},  # right F
        ],
    ),
]

RIGHT_WINDOW_EXISTS_MESSAGES = [
    Message(
        timestamp=11,
        value=A,
        updated=[
            {"start": 1, "end": 11, "value": [A]},  # left A
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=13,
        value=B,
        updated=[
            {"start": 3, "end": 13, "value": [A, B]},  # left B
            {"start": 12, "end": 22, "value": [B]},  # right A
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=11,
        value=C,
        updated=[
            {"start": 1, "end": 11, "value": [A, C]},  # left A
            {"start": 3, "end": 13, "value": [A, B, C]},  # left B
        ],
        expired=[],
        deleted=[],
    ),
]


@pytest.fixture
def sliding_window_definition_factory(
    state_manager, dataframe_factory, topic_manager_topic_factory
):
    def factory(
        topic: str, duration_ms: int, grace_ms: int = 0
    ) -> SlidingWindowDefinition:
        topic = topic_manager_topic_factory(topic)
        sdf = dataframe_factory(topic=topic, state_manager=state_manager)
        return SlidingWindowDefinition(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=sdf
        )

    return factory


@pytest.fixture
def window_factory(sliding_window_definition_factory):
    def factory(topic: str, duration_ms: int, grace_ms: int):
        window_definition = sliding_window_definition_factory(
            topic=topic, duration_ms=duration_ms, grace_ms=grace_ms
        )
        window = window_definition.reduce(
            reducer=lambda agg, value: agg + [value],
            initializer=lambda value: [value],
        )
        window.register_store()
        return window

    return factory


@pytest.fixture
def store_factory(state_manager):
    def factory(window):
        topic = window._dataframe.topic.name
        store = state_manager.get_store(topic=topic, store_name=window.name)
        store.assign_partition(0)
        return store

    return factory


@pytest.fixture
def state_factory():
    @contextmanager
    def factory(store, key):
        with store.start_partition_transaction(0) as tx:
            yield tx.as_state(prefix=key)

    return factory


@pytest.mark.parametrize(
    "duration_ms, grace_ms, messages",
    [
        pytest.param(10, 5, BASIC_CASE_MESSAGES, id="basic-case"),
        pytest.param(10, 15, RIGHT_WINDOW_EXISTS_MESSAGES, id="right-window-exists"),
    ],
)
def test_sliding_window(
    window_factory, store_factory, state_factory, duration_ms, grace_ms, messages
):
    window = window_factory(topic="topic", duration_ms=10, grace_ms=5)
    store = store_factory(window)
    for message in messages:
        with state_factory(store, b"key") as state:
            updated, expired = window.process_window(
                value=message.value, timestamp_ms=message.timestamp, state=state
            )

        assert list(updated) == message.updated
        assert expired == message.expired

        with state_factory(store, b"key") as state:
            for deleted in message.deleted:
                assert not state.get_window(
                    start_ms=deleted["start"], end_ms=deleted["end"]
                )


@pytest.mark.timeit
def test_sliding_window_timeit(window_factory, store_factory, state_factory):
    number = 1000
    i = 0

    windows, stores = [], []
    for _ in range(number):
        window = window_factory(topic=uuid4().hex, duration_ms=10, grace_ms=5)
        windows.append(window)
        stores.append(store_factory(window))

    def func():
        nonlocal windows, stores, i
        window, store, i = windows[i], stores[i], i + 1

        for message in BASIC_CASE_MESSAGES:
            with state_factory(store, uuid4().hex.encode()) as state:
                window.process_window(
                    value=message.value, timestamp_ms=message.timestamp, state=state
                )

    result = timeit(
        func,
        number=number,
        globals={"windows": windows, "stores": stores, "i": i},
    )
    print("Timeit:", result)
