from typing import Tuple

from ..metadata import PREFIX_SEPARATOR
from ..serialization import int_from_int64_bytes, int_to_int64_bytes

__all__ = ("parse_window_key", "encode_window_key", "encode_window_prefix")

_TIMESTAMP_BYTE_LENGTH = len(int_to_int64_bytes(0))
_PREFIX_SEPARATOR_LENGTH = len(PREFIX_SEPARATOR)


def parse_window_key(key: bytes) -> Tuple[bytes, int, int]:
    """
    Parse the window key from Rocksdb into (message_key, start, end) structure.

    Expected window key format:
    <message_key>|<start>|<end>

    :param key: a key from Rocksdb
    :return: a tuple with message key, start timestamp, end timestamp
    """

    timestamps_segment_len = _TIMESTAMP_BYTE_LENGTH * 2 + _PREFIX_SEPARATOR_LENGTH
    message_key, timestamps_bytes = (
        key[: -timestamps_segment_len - 1],
        key[-timestamps_segment_len:],
    )
    start_bytes, end_bytes = (
        timestamps_bytes[:_TIMESTAMP_BYTE_LENGTH],
        timestamps_bytes[_TIMESTAMP_BYTE_LENGTH + 1 :],
    )

    start_ms, end_ms = int_from_int64_bytes(start_bytes), int_from_int64_bytes(
        end_bytes
    )
    return message_key, start_ms, end_ms


def encode_window_key(start_ms: int, end_ms: int) -> bytes:
    """
    Encode window start and end timestamps into bytes of the following format:
    ```<start>|<end>```

    Encoding window keys this way make them sortable in RocksDB within the same prefix.

    :param start_ms: window start in milliseconds
    :param end_ms: window end in milliseconds
    :return: window timestamps as bytes
    """
    return int_to_int64_bytes(start_ms) + PREFIX_SEPARATOR + int_to_int64_bytes(end_ms)


def encode_window_prefix(prefix: bytes, start_ms: int) -> bytes:
    """
    Encode window prefix and start time to iterate over keys in RocksDB
    Format:
    ```<prefix>|<start>```

    :param prefix: transaction prefix
    :param start_ms: window start time in milliseconds
    :return: bytes
    """
    return prefix + PREFIX_SEPARATOR + int_to_int64_bytes(start_ms)
