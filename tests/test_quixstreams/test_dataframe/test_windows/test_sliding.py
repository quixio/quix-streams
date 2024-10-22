from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest

from quixstreams.dataframe.windows import SlidingWindowDefinition

A, B, C, D, E, F, G, H, I = "A", "B", "C", "D", "E", "F", "G", "H", "I"


@dataclass
class Message:
    """
    Represents an incoming message with its timestamp and value. It also tracks
    the expected state of sliding windows after the message is processed.
    """

    timestamp: int
    value: str

    # Windows that will be emitted via .current()
    updated: list[dict[str, Any]] = field(default_factory=list)

    # Windows that will be emitted via .final()
    expired: list[dict[str, Any]] = field(default_factory=list)

    # Windows that should no longer be in state.
    deleted: list[dict[str, Any]] = field(default_factory=list)


#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
# ______________________________________________________________________
# A 23                        A
#                   |---------|
#                   13       23
# ______________________________________________________________________
# B 12             B
#        |---------|
#        2        12
# ______________________________________________________________________
# Message B arrives and finds that the right window (13, 23) already exists.
# It will neither be created nor updated. Message B will only create a left
# window for itself.
RIGHT_WINDOW_EXISTS = [
    Message(
        timestamp=23,
        value=A,
        updated=[{"start": 13, "end": 23, "value": [A]}],  # left A
    ),
    Message(
        timestamp=12,
        value=B,
        updated=[{"start": 2, "end": 12, "value": [B]}],  # left B
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
# ______________________________________________________________________
# A 23                        A
#                   |---------|
#                   13       23
# ______________________________________________________________________
# B 20                     B
#                |---------||---------|
#                10      20  21      31
# ______________________________________________________________________
# Late message B arrives:
# * The right window (21, 31) must be created because it is not empty.
# * The left window of A (13, 23) must be updated with the new message.
# * The left window for B (10, 20) must be created.
RIGHT_WINDOW_CREATED = [
    Message(
        timestamp=23,
        value=A,
        updated=[{"start": 13, "end": 23, "value": [A]}],  # left A
    ),
    Message(
        timestamp=20,
        value=B,
        updated=[
            {"start": 10, "end": 20, "value": [B]},  # left B
            {"start": 13, "end": 23, "value": [A, B]},  # left A
        ],
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
# ______________________________________________________________________
# A 23                        A
#                   |---------|
#                   13       23
# ______________________________________________________________________
# B 20                     B
#                |---------||---------|
#                10      20  21      31
# ______________________________________________________________________
# B 27                         |---------|
#                              24       34
#                                 C
#                       |---------|
#                       17       27
# ______________________________________________________________________
# Message C arrives after late message B. The right window (21, 31)
# is updated but not emitted.
RIGHT_WINDOW_UPDATED = [
    Message(
        timestamp=23,
        value=A,
        updated=[{"start": 13, "end": 23, "value": [A]}],  # left A
    ),
    Message(
        timestamp=20,
        value=B,
        updated=[
            {"start": 10, "end": 20, "value": [B]},  # left B
            {"start": 13, "end": 23, "value": [A, B]},  # left A
        ],
    ),
    Message(
        timestamp=27,
        value=C,
        updated=[{"start": 17, "end": 27, "value": [A, B, C]}],  # left C
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
# ______________________________________________________________________
# A 17                  A
#             |---------|
#             7        17
# ______________________________________________________________________
# B 17                  B
# ______________________________________________________________________
# The left window (7, 17) already exists, and it will be updated.
# No right window will be created.
LEFT_WINDOW_EXISTS = [
    Message(
        timestamp=17,
        value=A,
        updated=[{"start": 7, "end": 17, "value": [A]}],  # left A
    ),
    Message(
        timestamp=17,
        value=B,
        updated=[{"start": 7, "end": 17, "value": [A, B]}],  # left A, B
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
# ______________________________________________________________________
# A 23                        A
#                   |---------|
#                   13       23
# ______________________________________________________________________
# B 20
#                          B
#                |---------||---------|
#                10      20  21      31
# ______________________________________________________________________
# C 31                         |---------|
#                              24       34
#                                     C
# ______________________________________________________________________
# D 31                                D
# Message C arrives after late message B:
# * A new right window for message A (24, 34) must be created.
# * The right window (21, 31) becomes the left window of C and gets emitted.
# When message D arrives, it finds no windows to create.
RIGHT_WINDOW_BECOMES_LEFT_WINDOW = [
    Message(
        timestamp=23,
        value=A,
        updated=[{"start": 13, "end": 23, "value": [A]}],  # left A
    ),
    Message(
        timestamp=20,
        value=B,
        updated=[
            {"start": 10, "end": 20, "value": [B]},  # left B
            {"start": 13, "end": 23, "value": [A, B]},  # left A
        ],
    ),
    Message(
        timestamp=31,
        value=C,
        updated=[{"start": 21, "end": 31, "value": [A, C]}],  # left C
    ),
    Message(
        timestamp=31,
        value=D,
        updated=[{"start": 21, "end": 31, "value": [A, C, D]}],  # left C, D
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
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
# Right windows do not need to be created in advance. The window (12, 22)
# will be created only after message B arrives. Additionally:
# * The left window (9, 19) will aggregate with window (1, 11).
# * The right window (12, 22) will not be emitted.
RIGHT_WINDOW_FOR_PREVIOUS_MESSAGE_CREATED = [
    Message(
        timestamp=11,
        value=A,
        updated=[{"start": 1, "end": 11, "value": [A]}],  # left A
    ),
    Message(
        timestamp=19,
        value=B,
        updated=[{"start": 9, "end": 19, "value": [A, B]}],  # left B
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 0
# ______________________________________________________________________
# A 16                 A
#            |---------|
#            6        16
# ______________________________________________________________________
# B 24                  |---------|
#                       17       27
#                              B
#                    |---------|
#                    14       24
#                   ^ 13  expiration watermark = 24 - 10 - 0 - 1
#           ^ 5  deletion watermark = min(5, 13)
# ______________________________________________________________________
# C 25                          |---------|
#                               25       35
#                               C
#                     |---------|
#                     15       25
#                    ^ 14  expiration watermark = 25 - 10 - 0 - 1
#                   ^ 13  deletion watermark = min(13, 14)
# ______________________________________________________________________
# For message C, the aggregation from window (14, 24) was used. Since we will
# not revisit lower windows, the deletion watermark can be set dynamically higher
# than the default expiration watermark minus the window duration. In this case,
# with a grace period of 0, the deletion watermark is set to the start of window
# (14, 24) minus one.
DELETION_WATERMARK_SET_BELOW_LAST_ITERATED_WINDOW = [
    Message(
        timestamp=16,
        value=A,
        updated=[{"start": 6, "end": 16, "value": [A]}],  # left A
    ),
    Message(
        timestamp=24,
        value=B,
        updated=[{"start": 14, "end": 24, "value": [A, B]}],  # left B
        expired=[{"start": 6, "end": 16, "value": [A]}],  # left A
    ),
    Message(
        timestamp=25,
        value=C,
        updated=[{"start": 15, "end": 25, "value": [A, B, C]}],  # left C
        expired=[{"start": 14, "end": 24, "value": [A, B]}],  # left B
        deleted=[{"start": 6, "end": 16, "value": [A]}],  # left A
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 0
# ______________________________________________________________________
# A 16                 A
#            |---------|
#            6        16
# ______________________________________________________________________
# B 24                  |---------|
#                       17       27
#                              B
#                    |---------|
#                    14       24
#                 ^ 11  expiration watermark = 24 - 10 - 2 - 1
#           ^ 5  deletion watermark = min(5, 11)
# ______________________________________________________________________
# C 25                          |---------|
#                               25       35
#                               C
#                     |---------|
#                     15       25
#                  ^ 12  expiration watermark = 25 - 10 - 2 - 1
#                  ^ 12  deletion watermark = min(13, 12)
# ______________________________________________________________________
# For message C, the aggregation from window (14, 24) was used. Since we will
# not revisit lower windows, the deletion watermark is dynamically set higher.
# In this case, with a grace period of 2, the deletion watermark is set to the
# expiration watermark, so window (14, 24) will not be deleted.
DELETION_WATERMARK_SET_TO_EXPIRATION_WATERMARK = [
    Message(
        timestamp=16,
        value=A,
        updated=[{"start": 6, "end": 16, "value": [A]}],  # left A
    ),
    Message(
        timestamp=24,
        value=B,
        updated=[{"start": 14, "end": 24, "value": [A, B]}],  # left B
        expired=[{"start": 6, "end": 16, "value": [A]}],  # left A
    ),
    Message(
        timestamp=25,
        value=C,
        updated=[{"start": 15, "end": 25, "value": [A, B, C]}],  # left C
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
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
# ______________________________________________________________________
# Window (1, 11) overlaps with window (11, 21). It is the lowest possible
# window to provide aggregation to combine with message B.
AGGREGATION_FROM_MIN_ELIGIBLE_WINDOW = [
    Message(
        timestamp=11,
        value=A,
        updated=[{"start": 1, "end": 11, "value": [A]}],  # left A
    ),
    Message(
        timestamp=21,
        value=B,
        updated=[{"start": 11, "end": 21, "value": [A, B]}],  # left B
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
# ______________________________________________________________________
# A 11            A
#       |---------|
#       1        11
# ______________________________________________________________________
# B 22                       B
#                  |---------|
#                  12       22
# ______________________________________________________________________
# Window (1, 11) does not overlap with window (12, 22). Message B will
# be the only message in window (12, 22).
AGGREGATION_NOT_FOUND = [
    Message(
        timestamp=11,
        value=A,
        updated=[{"start": 1, "end": 11, "value": [A]}],  # left A
    ),
    Message(
        timestamp=22,
        value=B,
        updated=[{"start": 12, "end": 22, "value": [B]}],  # left B
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
# ______________________________________________________________________
# A 3     A
#      |--|
#      0  3
# ______________________________________________________________________
# If the event times start from 0, prevent window start times from
# going into negative values.
PREVENT_NEGATIVE_START_TIME = [
    Message(
        timestamp=3,
        value=A,
        updated=[
            {"start": 0, "end": 3, "value": [A]},  # left A
        ],
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 15
# ______________________________________________________________________
# A 29                              A
#                         |---------|
#                         19       29
# ______________________________________________________________________
# B 26                           B
#                      |---------||---------|
#                      16      26  27      37
# ______________________________________________________________________
# C 41                                          C
#                                     |---------|
#                                     31       41
# ______________________________________________________________________
# When message C arrives, it finds the overlapping window (27, 37),
# but the maximum timestamp in that window is from message A, which is 29.
# Since 29 falls outside of C's left window (31, 41), C will be the only
# message in its own window.
DEFAULT_AGGREGATION_USED = [
    Message(
        timestamp=29,
        value=A,
        updated=[{"start": 19, "end": 29, "value": [A]}],  # left A
    ),
    Message(
        timestamp=26,
        value=B,
        updated=[
            {"start": 16, "end": 26, "value": [B]},  # left B
            {"start": 19, "end": 29, "value": [A, B]},  # left A
        ],
    ),
    Message(
        timestamp=41,
        value=C,
        updated=[{"start": 31, "end": 41, "value": [C]}],  # left C
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 0
# ______________________________________________________________________
# A 22                       A
#                  |---------|
#                  12        22
# ______________________________________________________________________
# B 23                        |---------|
#                             23       33
#                             B
#                   |---------|
#                   13       23
#                  ^ 12  expiration watermark = 23 - 10 - 0 - 1
# ______________________________________________________________________
# C 22                       C
#                  ^ 12  expiration watermark = 23 - 10 - 0 - 1
# ______________________________________________________________________
# When message B arrives, window (12, 22) no longer accepts messages,
# it may be closed. Message C will not update window (12, 22).
EXPIRATION_WITHOUT_GRACE = [
    Message(
        timestamp=22,
        value=A,
        updated=[{"start": 12, "end": 22, "value": [A]}],  # left A
    ),
    Message(
        timestamp=23,
        value=B,
        updated=[{"start": 13, "end": 23, "value": [A, B]}],  # left B
        expired=[{"start": 12, "end": 22, "value": [A]}],  # left A
    ),
    Message(
        timestamp=22,
        value=C,
        updated=[{"start": 13, "end": 23, "value": [A, B, C]}],  # left B
    ),
]

#      0        10        20        30        40        50        60
# -----|---------|---------|---------|---------|---------|---------|--->
# Duration: 10
# Grace: 3
# ______________________________________________________________________
# A 22                       A
#                  |---------|
#                  12       22
# ______________________________________________________________________
# B 23                        |---------|
#                             23       33
#                             B
#                   |---------|
#                   13       23
#               ^ 9  expiration watermark = 23 - 10 - 3 - 1
# ______________________________________________________________________
# C 17                  C
#                        |---------|
#                        18       28
#               ^ 9  expiration watermark = 23 - 10 - 3 - 1
# ______________________________________________________________________
# D 26                         |---------|
#                              24       34
#                                D
#                      |---------|
#                      16       26
#                  ^ 12  expiration watermark = 26 - 10 - 3 - 1
# ______________________________________________________________________
# Window (12, 22) is expired by a message D arriving at 26.
# Note: left window for message C (7, 17) will not be created
# because its start time is behind the expiration watermark
EXPIRATION_WITH_GRACE = [
    Message(
        timestamp=22,
        value=A,
        updated=[{"start": 12, "end": 22, "value": [A]}],  # left A
    ),
    Message(
        timestamp=23,
        value=B,
        updated=[{"start": 13, "end": 23, "value": [A, B]}],  # left B
    ),
    Message(
        timestamp=17,
        value=C,
        updated=[
            {"start": 12, "end": 22, "value": [A, C]},  # left A
            {"start": 13, "end": 23, "value": [A, B, C]},  # left B
        ],
    ),
    Message(
        timestamp=26,
        value=D,
        updated=[{"start": 16, "end": 26, "value": [A, B, C, D]}],  # left D
        expired=[{"start": 12, "end": 22, "value": [A, C]}],  # left A
        deleted=[{"start": 12, "end": 22, "value": [A, C]}],  # left A
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
        pytest.param(10, 15, RIGHT_WINDOW_EXISTS, id="right-window-exists"),
        pytest.param(10, 15, RIGHT_WINDOW_CREATED, id="right-window-created"),
        pytest.param(10, 15, RIGHT_WINDOW_UPDATED, id="right-window-updated"),
        pytest.param(10, 15, LEFT_WINDOW_EXISTS, id="left-window-exists"),
        pytest.param(
            10,
            15,
            RIGHT_WINDOW_BECOMES_LEFT_WINDOW,
            id="right-window-becomes-left-window",
        ),
        pytest.param(
            10,
            15,
            RIGHT_WINDOW_FOR_PREVIOUS_MESSAGE_CREATED,
            id="right-window-for-revious-message-created",
        ),
        pytest.param(
            10,
            0,
            DELETION_WATERMARK_SET_BELOW_LAST_ITERATED_WINDOW,
            id="deletion-watermark-set-below-last-iterated-window",
        ),
        pytest.param(
            10,
            2,
            DELETION_WATERMARK_SET_TO_EXPIRATION_WATERMARK,
            id="deletion-watermark-set-expiration-watermark",
        ),
        pytest.param(
            10,
            15,
            AGGREGATION_FROM_MIN_ELIGIBLE_WINDOW,
            id="aggregation-from-min-eligible-window",
        ),
        pytest.param(10, 15, AGGREGATION_NOT_FOUND, id="aggregation-not-found"),
        pytest.param(
            10, 15, PREVENT_NEGATIVE_START_TIME, id="prevent-negative-start-time"
        ),
        pytest.param(10, 15, DEFAULT_AGGREGATION_USED, id="default-aggregation-used"),
        pytest.param(10, 0, EXPIRATION_WITHOUT_GRACE, id="expiration-without-grace"),
        pytest.param(10, 3, EXPIRATION_WITH_GRACE, id="expiration-with-grace"),
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
