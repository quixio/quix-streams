from collections import namedtuple
from contextlib import contextmanager

import pytest

from quixstreams.dataframe.windows import SlidingWindowDefinition

Message = namedtuple("Message", ["timestamp", "value", "updated", "expired", "deleted"])

A, B, C, D, E, F, G, H, I = "A", "B", "C", "D", "E", "F", "G", "H", "I"

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# ______________________________________________________________________
# A 11            A
#       |---------|
#       1        11
# ______________________________________________________________________
# B 19             |---------|
#                  12       22
#                         B
#               |---------|
#               9        19
# ______________________________________________________________________
# C 16                 C
#            |---------||---------|
#            6       16  17      27
# ______________________________________________________________________
# D 27                     |---------|
#                          20       30
#                                 D
# ______________________________________________________________________
# E 22                       E
#                             |---------|
#                             23       33
# ______________________________________________________________________
# F 25                          F
#                     |---------||---------|
#                     15      25  26      36
# ______________________________________________________________________
# G 27                            G
# ______________________________________________________________________
# H 38                                       H
#                                  |---------|
#                                  28       38
# ______________________________________________________________________
# I 54                                                        I
#                                                   |---------|
#                                                   44       54
BASIC_CASE = [
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
            # {"start": 12, "end": 22, "value": [B]},  # right A
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
            # {"start": 12, "end": 22, "value": [B, C]},  # right A
            # {"start": 17, "end": 27, "value": [B]},  # right C
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=27,
        value=D,
        updated=[
            {"start": 17, "end": 27, "value": [B, D]},  # right C / left D
            # {"start": 20, "end": 30, "value": [D]},  # right B
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
            # {"start": 20, "end": 30, "value": [D, E]},  # right B
            # {"start": 23, "end": 33, "value": [D]},  # right E
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
            # {"start": 20, "end": 30, "value": [D, E, F]},  # right B
            # {"start": 23, "end": 33, "value": [D, F]},  # right E
            # {"start": 26, "end": 36, "value": [D]},  # right F
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=27,
        value=G,
        updated=[
            {"start": 17, "end": 27, "value": [B, D, E, F, G]},  # right C / left D, G
            # {"start": 20, "end": 30, "value": [D, E, F, G]},  # right B
            # {"start": 23, "end": 33, "value": [D, F, G]},  # right E
            # {"start": 26, "end": 36, "value": [D, G]},  # right F
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
            # {"start": 20, "end": 30, "value": [D, E, F, G]},  # right B
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
            # {"start": 23, "end": 33, "value": [D, F, G]},  # right E
            # {"start": 26, "end": 36, "value": [D, G]},  # right F
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

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# ______________________________________________________________________
# A 11            A
#       |---------|
#       1        11
# ______________________________________________________________________
# B 13             |---------|
#                  12       22
#                   B
#         |---------|
#         3        13
# ______________________________________________________________________
# C 11            C
RIGHT_WINDOW_EXISTS = [
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
            # {"start": 12, "end": 22, "value": [B]},  # right A
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

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# ______________________________________________________________________
# A 11            A
#       |---------|
#       1        11
# ______________________________________________________________________
# B 23                        B
#                   |---------|
#                   13       23
# ______________________________________________________________________
# C 19             |---------|
#                  12       22
#                         C
#               |---------||---------|
#               9       19  20      30
# ______________________________________________________________________
# D 29                         |---------|
#                              24       34
#                                   D
#                         |---------|
#                         19       29
# ______________________________________________________________________
# E 24                         E
#                    |---------||---------|
#                    14      24  25      35
DELETION_WATERMARK = [
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
        timestamp=23,
        value=B,
        updated=[
            {"start": 13, "end": 23, "value": [B]},  # left B
        ],
        expired=[
            {"start": 1, "end": 11, "value": [A]},  # left A
        ],
        deleted=[],
    ),
    Message(
        timestamp=19,
        value=C,
        updated=[
            {"start": 9, "end": 19, "value": [A, C]},  # left C
            # {"start": 12, "end": 22, "value": [C]},  # right A
            {"start": 13, "end": 23, "value": [B, C]},  # left B
            # {"start": 20, "end": 30, "value": [B]},  # right C
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=29,
        value=D,
        updated=[
            {"start": 19, "end": 29, "value": [B, C, D]},  # left D
            # {"start": 20, "end": 30, "value": [B, D]},  # right C
            # {"start": 24, "end": 34, "value": [D]},  # right B
        ],
        expired=[
            {"start": 9, "end": 19, "value": [A, C]},  # left C
            # {"start": 12, "end": 22, "value": [C]},  # right A
            {"start": 13, "end": 23, "value": [B, C]},  # left B
        ],
        deleted=[
            {"start": 1, "end": 11, "value": [A]},  # left A
            {"start": 9, "end": 19, "value": [A, C]},  # left C
            {"start": 12, "end": 22, "value": [C]},  # right A
        ],
    ),
    Message(
        timestamp=24,
        value=E,
        updated=[
            {"start": 14, "end": 24, "value": [B, C, E]},  # left E
            {"start": 19, "end": 29, "value": [B, C, D, E]},  # left D
            # {"start": 20, "end": 30, "value": [B, D, E]},  # right C
            # {"start": 24, "end": 34, "value": [D, E]},  # right B
            # {"start": 25, "end": 35, "value": [D]},  # right E
        ],
        expired=[],
        deleted=[],
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# ______________________________________________________________________
# A 11            A
#       |---------|
#       1        11
# ______________________________________________________________________
# B 21             |---------|
#                  12       22
#                           B
#                 |---------|
#                 11       21
AGG_FROM_MIN_ELIGIBLE_WINDOW = [
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
        timestamp=21,
        value=B,
        updated=[
            {"start": 11, "end": 21, "value": [A, B]},  # left B
            # {"start": 12, "end": 22, "value": [B]},  # right A
        ],
        expired=[
            {"start": 1, "end": 11, "value": [A]},  # left A
        ],
        deleted=[],
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# ______________________________________________________________________
# A 11            A
#       |---------|
#       1        11
# ______________________________________________________________________
# B 22                       B
#                  |---------|
#                  12       22
AGG_NOT_FOUND = [
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
        timestamp=22,
        value=B,
        updated=[
            {"start": 12, "end": 22, "value": [B]},  # left B
        ],
        expired=[
            {"start": 1, "end": 11, "value": [A]},  # left A
        ],
        deleted=[],
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# ______________________________________________________________________
# A 3     A
#      |--|
#      0  3
# ______________________________________________________________________
# B 5      |---------|
#          4        14
#           B
#      |----|
#      0    5
# ______________________________________________________________________
# C 4       |---------|
#           5        15
#          C
#      |---|
#      0   4
PREVENT_NEGATIVE_START_TIME = [
    Message(
        timestamp=3,
        value=A,
        updated=[
            {"start": 0, "end": 3, "value": [A]},  # left A
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=5,
        value=B,
        updated=[
            {"start": 0, "end": 5, "value": [A, B]},  # left B
            # {"start": 4, "end": 14, "value": [B]},  # right A
        ],
        expired=[],
        deleted=[],
    ),
    Message(
        timestamp=4,
        value=C,
        updated=[
            {"start": 0, "end": 4, "value": [A, C]},  # left C
            {"start": 0, "end": 5, "value": [A, B, C]},  # left B
            # {"start": 4, "end": 14, "value": [B, C]},  # right A
            # {"start": 5, "end": 15, "value": [B]},  # right C
        ],
        expired=[],
        deleted=[],
    ),
]


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


@pytest.mark.parametrize(
    "duration_ms, grace_ms, messages",
    [
        pytest.param(10, 5, BASIC_CASE, id="basic-case"),
        pytest.param(10, 5, RIGHT_WINDOW_EXISTS, id="right-window-exists"),
        pytest.param(10, 5, DELETION_WATERMARK, id="deletion-watermark"),
        pytest.param(
            10, 5, AGG_FROM_MIN_ELIGIBLE_WINDOW, id="agg-from-min-eligible-window"
        ),
        pytest.param(10, 5, AGG_NOT_FOUND, id="agg-not-found"),
        pytest.param(
            10, 5, PREVENT_NEGATIVE_START_TIME, id="prevent-negative-start-time"
        ),
    ],
)
def test_sliding_window(window_factory, state_factory, duration_ms, grace_ms, messages):
    window = window_factory(duration_ms=duration_ms, grace_ms=grace_ms)
    for message in messages:
        with state_factory(window) as state:
            updated, expired = window.process_window(
                value=message.value, timestamp_ms=message.timestamp, state=state
            )

        assert list(updated) == message.updated
        assert list(expired) == message.expired

        with state_factory(window) as state:
            for deleted in message.deleted:
                assert not state.get_window(
                    start_ms=deleted["start"], end_ms=deleted["end"]
                )
