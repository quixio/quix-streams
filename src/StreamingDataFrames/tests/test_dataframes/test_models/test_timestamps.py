import pytest

from streamingdataframes.models import MessageTimestamp, TimestampType


@pytest.mark.parametrize(
    "timestamp_type, milliseconds, expected",
    [
        (0, 123, MessageTimestamp(type=TimestampType(0), milliseconds=None)),
        (1, 123, MessageTimestamp(type=TimestampType(1), milliseconds=123)),
        (2, 123, MessageTimestamp(type=TimestampType(2), milliseconds=123)),
    ],
)
def test_create_timestamp_success(
    timestamp_type: int, milliseconds: int, expected: MessageTimestamp
):
    assert (
        MessageTimestamp.create(
            timestamp_type=timestamp_type, milliseconds=milliseconds
        )
        == expected
    )


def test_create_timestamp_unknown_type_error():
    with pytest.raises(ValueError, match="123 is not a valid TimestampType"):
        MessageTimestamp.create(timestamp_type=123, milliseconds=123)
