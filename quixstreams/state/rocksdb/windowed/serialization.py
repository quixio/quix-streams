import struct

from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.serialization import (
    int_to_int64_bytes,
)

__all__ = ("parse_window_key", "encode_integer_pair", "append_integer")

_TIMESTAMP_BYTE_LENGTH = len(int_to_int64_bytes(0))
_SEPARATOR_LENGTH = len(SEPARATOR)
_TIMESTAMPS_SEGMENT_LEN = _TIMESTAMP_BYTE_LENGTH * 2 + _SEPARATOR_LENGTH

_window_pack_format = ">q" + "c" * _SEPARATOR_LENGTH + "q"
_window_packer = struct.Struct(_window_pack_format)
_window_pack = _window_packer.pack
_window_unpack = _window_packer.unpack


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

    start_ms, _, end_ms = _window_unpack(timestamps_bytes)
    return message_key, start_ms, end_ms


def encode_integer_pair(integer_1: int, integer_2: int) -> bytes:
    """
    Encode a pair of integers into bytes of the following format:
    ```<integer_1>|<integer_2>```

    Encoding integers this way make them sortable in RocksDB within the same prefix.

    :param integer_1: first integer
    :param integer_2: second integer
    :return: integers as bytes
    """
    return _window_pack(integer_1, SEPARATOR, integer_2)


def append_integer(base_bytes: bytes, integer: int) -> bytes:
    """
    Append integer to the base bytes
    Format:
    ```<base_bytes>|<integer>```

    :param base_bytes: base bytes
    :param integer: integer to append
    :return: bytes
    """
    return base_bytes + SEPARATOR + int_to_int64_bytes(integer)
