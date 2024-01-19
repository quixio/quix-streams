import pytest

from quixstreams.models import MessageTimestamp, TimestampType


@pytest.mark.parametrize(
    "timestamp_type, milliseconds, expected",
    [
        (0, 0, MessageTimestamp(type=TimestampType(0), milliseconds=0)),
        (1, 123, MessageTimestamp(type=TimestampType(1), milliseconds=123)),
        (2, 123, MessageTimestamp(type=TimestampType(2), milliseconds=123)),
    ],
)
def test_create_timestamp_success(
    timestamp_type: int, milliseconds: int, expected: MessageTimestamp
):
    ts = MessageTimestamp.create(
        timestamp_type=timestamp_type, milliseconds=milliseconds
    )
    assert ts.type == expected.type
    assert ts.milliseconds == expected.milliseconds


def test_create_timestamp_unknown_type_error():
    with pytest.raises(ValueError, match="123 is not a valid TimestampType"):
        MessageTimestamp.create(timestamp_type=123, milliseconds=123)
