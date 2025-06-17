from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, Literal, Optional, Union

import pytest

from quixstreams.dataframe.joins.base import OnOverlap


@dataclass
class Message:
    timestamp: int
    value: Any = field(default_factory=dict)
    key: bytes = b"key"
    topic: Optional[Literal["left", "right"]] = None
    expected: list[Any] = field(default_factory=list)


@dataclass
class Case:
    how: Literal["inner", "left"] = "inner"
    on_merge: Union[OnOverlap, Callable[[Any, Any], Any]] = "raise"
    grace_ms: Union[int, timedelta] = timedelta(days=7)
    backward_ms: Union[int, timedelta] = 0
    forward_ms: Union[int, timedelta] = 0
    messages: list[Message] = field(default_factory=list)


class TestStreamingDataFrameJoinInterval:
    @pytest.mark.parametrize(
        "case",
        [
            # Backward interval: Right message with the timestamp 7
            # makes it into the interval 7-10
            Case(
                backward_ms=3,
                forward_ms=0,
                messages=[
                    Message(
                        topic="right",
                        timestamp=7,
                        value={"right": 1},
                    ),
                    Message(
                        topic="left",
                        timestamp=10,
                        value={"left": 2},
                        expected=[Message(timestamp=10, value={"left": 2, "right": 1})],
                    ),
                ],
            ),
            # Backward interval: Right message with the timestamp 6
            # does not make it into the interval 7-10
            Case(
                backward_ms=3,
                forward_ms=0,
                messages=[
                    Message(topic="right", timestamp=6),
                    Message(topic="left", timestamp=10, expected=[]),
                ],
            ),
            # Forward interval: Left message with the timestamp 7
            # makes it into the interval 7-10
            Case(
                backward_ms=0,
                forward_ms=3,
                messages=[
                    Message(topic="left", timestamp=7, value={"left": 1}),
                    Message(
                        topic="right",
                        timestamp=10,
                        value={"right": 2},
                        expected=[Message(timestamp=10, value={"left": 1, "right": 2})],
                    ),
                ],
            ),
            # Forward interval: Left message with the timestamp 6
            # does not make it into the interval 7-10
            Case(
                backward_ms=0,
                forward_ms=3,
                messages=[
                    Message(topic="left", timestamp=6),
                    Message(topic="right", timestamp=10, expected=[]),
                ],
            ),
            # Left join with forward interval.
            # Left is expected in the result without any right match.
            Case(
                how="left",
                backward_ms=0,
                forward_ms=3,
                messages=[
                    Message(
                        topic="left",
                        timestamp=7,
                        value={"left": 1},
                        expected=[Message(timestamp=7, value={"left": 1})],
                    ),
                    Message(
                        topic="right",
                        timestamp=10,
                        value={"right": 2},
                        expected=[Message(timestamp=10, value={"left": 1, "right": 2})],
                    ),
                ],
            ),
            # Backward and forward interval.
            # Multiple right messages with the same timestamp.
            Case(
                backward_ms=3,
                forward_ms=3,
                messages=[
                    Message(topic="right", timestamp=6, value={"right": 6}),
                    Message(topic="right", timestamp=7, value={"right": 7}),
                    Message(topic="right", timestamp=8, value={"right": 81}),
                    Message(topic="right", timestamp=8, value={"right": 82}),
                    Message(topic="right", timestamp=10, value={"right": 10}),
                    Message(
                        topic="left",
                        timestamp=10,
                        value={"left": 10},
                        expected=[
                            Message(timestamp=10, value={"left": 10, "right": 7}),
                            Message(timestamp=10, value={"left": 10, "right": 81}),
                            Message(timestamp=10, value={"left": 10, "right": 82}),
                            Message(timestamp=10, value={"left": 10, "right": 10}),
                        ],
                    ),
                    Message(
                        topic="right",
                        timestamp=11,
                        value={"right": 111},
                        expected=[
                            Message(timestamp=11, value={"left": 10, "right": 111}),
                        ],
                    ),
                    Message(
                        topic="right",
                        timestamp=11,
                        value={"right": 112},
                        expected=[
                            Message(timestamp=11, value={"left": 10, "right": 112}),
                        ],
                    ),
                ],
            ),
            # Test interval = 0, which is equivalent to exact timestamp match.
            Case(
                backward_ms=0,
                forward_ms=0,
                messages=[
                    Message(topic="right", timestamp=7, value={"right": 1}),
                    Message(topic="left", timestamp=6, value={"left": 2}),
                    Message(topic="left", timestamp=8, value={"left": 3}),
                    Message(
                        topic="left",
                        timestamp=7,
                        value={"left": 4},
                        expected=[Message(timestamp=7, value={"left": 4, "right": 1})],
                    ),
                    Message(
                        topic="right",
                        timestamp=7,
                        value={"right": 5},
                        expected=[Message(timestamp=7, value={"left": 4, "right": 5})],
                    ),
                    Message(
                        topic="left",
                        timestamp=7,
                        value={"left": 6},
                        expected=[
                            Message(timestamp=7, value={"left": 6, "right": 1}),
                            Message(timestamp=7, value={"left": 6, "right": 5}),
                        ],
                    ),
                ],
            ),
            # Test with an on_merge keep-left
            Case(
                on_merge="keep-left",
                messages=[
                    Message(topic="left", timestamp=7, value={"a": 1, "b": 2}),
                    Message(
                        topic="right",
                        timestamp=7,
                        value={"a": 1, "b": 3},
                        expected=[Message(timestamp=7, value={"a": 1, "b": 2})],
                    ),
                ],
            ),
            # Test with an on_merge keep-right
            Case(
                on_merge="keep-right",
                messages=[
                    Message(topic="left", timestamp=7, value={"a": 1, "b": 2}),
                    Message(
                        topic="right",
                        timestamp=7,
                        value={"a": 1, "b": 3},
                        expected=[Message(timestamp=7, value={"a": 1, "b": 3})],
                    ),
                ],
            ),
            # Test with a custom on_merge function.
            Case(
                on_merge=lambda left, right: left + right,
                messages=[
                    Message(topic="right", timestamp=7, value=1),
                    Message(
                        topic="left",
                        timestamp=7,
                        value=2,
                        expected=[Message(timestamp=7, value=3)],
                    ),
                ],
            ),
            # Right join - left message is not published, right message
            # is published without a match and a joined left+right is published
            Case(
                how="right",
                messages=[
                    Message(topic="left", timestamp=6, value={"left": 1}),
                    Message(
                        topic="right",
                        timestamp=7,
                        value={"right": 2},
                        expected=[Message(timestamp=7, value={"right": 2})],
                    ),
                    Message(
                        topic="left",
                        timestamp=7,
                        value={"left": 3},
                        expected=[Message(timestamp=7, value={"left": 3, "right": 2})],
                    ),
                ],
            ),
            # Outer join - left message is published without a match, right message
            # is published without a match and a joined left+right is published
            Case(
                how="outer",
                messages=[
                    Message(
                        topic="left",
                        timestamp=6,
                        value={"left": 1},
                        expected=[Message(timestamp=6, value={"left": 1})],
                    ),
                    Message(
                        topic="right",
                        timestamp=7,
                        value={"right": 2},
                        expected=[Message(timestamp=7, value={"right": 2})],
                    ),
                    Message(
                        topic="left",
                        timestamp=7,
                        value={"left": 3},
                        expected=[Message(timestamp=7, value={"left": 3, "right": 2})],
                    ),
                ],
            ),
        ],
    )
    def test_join_interval(
        self,
        topic_manager_topic_factory,
        create_sdf,
        assign_partition,
        publish,
        case,
    ):
        left_topic = topic_manager_topic_factory("left")
        right_topic = topic_manager_topic_factory("right")
        topics = {"left": left_topic, "right": right_topic}
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)

        joined_sdf = left_sdf.join_interval(
            right_sdf,
            how=case.how,
            on_merge=case.on_merge,
            grace_ms=case.grace_ms,
            backward_ms=case.backward_ms,
            forward_ms=case.forward_ms,
        )
        assign_partition(left_sdf)
        assign_partition(right_sdf)

        for message in case.messages:
            result = publish(
                joined_sdf,
                topic=topics[message.topic],
                value=message.value,
                key=message.key,
                timestamp=message.timestamp,
            )

            assert len(result) == len(message.expected)

            for (result_value, result_key, result_timestamp, _), expected in zip(
                result, message.expected
            ):
                assert result_timestamp == expected.timestamp
                assert result_key == expected.key
                assert result_value == expected.value

    def test_backward_ms_greater_than_grace_ms(
        self,
        topic_manager_topic_factory,
        create_sdf,
    ):
        left_topic = topic_manager_topic_factory("left")
        right_topic = topic_manager_topic_factory("right")
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)

        with pytest.raises(
            ValueError,
            match="The backward_ms must not be greater than the grace_ms to avoid losing data.",
        ):
            left_sdf.join_interval(right_sdf, grace_ms=1, backward_ms=2)
