from quixstreams.state.metadata import SEPARATOR_LENGTH
from quixstreams.state.serialization import (
    decode_integer_pair,
    int_to_bytes,
)

__all__ = ("parse_window_key",)

_TIMESTAMP_BYTE_LENGTH = len(int_to_bytes(0))
_TIMESTAMPS_SEGMENT_LEN = _TIMESTAMP_BYTE_LENGTH * 2 + SEPARATOR_LENGTH


def parse_window_key(key: bytes) -> tuple[bytes, int, int]:
    """
    Parse the window key from Rocksdb into (message_key, start, end) structure.

    Expected window key format:
    <message_key>|<start>|<end>

    :param key: a key from Rocksdb
    :return: a tuple with message key, start timestamp, end timestamp
    """

    message_key, timestamps_bytes = (
        key[: -_TIMESTAMPS_SEGMENT_LEN - 1],
        key[-_TIMESTAMPS_SEGMENT_LEN:],
    )

    start_ms, end_ms = decode_integer_pair(timestamps_bytes)
    return message_key, start_ms, end_ms
