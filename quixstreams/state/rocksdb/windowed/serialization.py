import struct
from typing import Tuple

from quixstreams.state.metadata import PREFIX_SEPARATOR
from quixstreams.state.serialization import (
    int_to_int64_bytes,
)

__all__ = ("parse_window_key", "encode_window_key", "append_integer")

_TIMESTAMP_BYTE_LENGTH = len(int_to_int64_bytes(0))
_PREFIX_SEPARATOR_LENGTH = len(PREFIX_SEPARATOR)
_TIMESTAMPS_SEGMENT_LEN = _TIMESTAMP_BYTE_LENGTH * 2 + _PREFIX_SEPARATOR_LENGTH

_window_pack_format = ">q" + "c" * _PREFIX_SEPARATOR_LENGTH + "q"
_window_packer = struct.Struct(_window_pack_format)
_window_pack = _window_packer.pack
_window_unpack = _window_packer.unpack


def parse_window_key(key: bytes) -> Tuple[bytes, int, int]:
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


def encode_window_key(start_ms: int, end_ms: int) -> bytes:
    """
    Encode window start and end timestamps into bytes of the following format:
    ```<start>|<end>```

    Encoding window keys this way make them sortable in RocksDB within the same prefix.

    :param start_ms: window start in milliseconds
    :param end_ms: window end in milliseconds
    :return: window timestamps as bytes
    """
    return _window_pack(start_ms, PREFIX_SEPARATOR, end_ms)


def append_integer(base_bytes: bytes, integer: int) -> bytes:
    """
    Append integer to the base bytes
    Format:
    ```<base_bytes>|<integer>```

    :param base_bytes: base bytes
    :param integer: integer to append
    :return: bytes
    """
    return base_bytes + PREFIX_SEPARATOR + int_to_int64_bytes(integer)
