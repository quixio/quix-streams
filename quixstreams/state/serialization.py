import struct
from typing import Any, Callable

from .exceptions import StateSerializationError
from .metadata import SEPARATOR, SEPARATOR_LENGTH

__all__ = (
    "DumpsFunc",
    "LoadsFunc",
    "serialize",
    "deserialize",
    "int_to_bytes",
    "int_from_bytes",
    "encode_integer_pair",
    "decode_integer_pair",
)

_int_packer = struct.Struct(">Q")
_int_pack = _int_packer.pack
_int_unpack = _int_packer.unpack

_int_pair_pack_format = ">Q" + "c" * SEPARATOR_LENGTH + "Q"
_int_pair_packer = struct.Struct(_int_pair_pack_format)
_int_pair_pack = _int_pair_packer.pack
_int_pair_unpack = _int_pair_packer.unpack

DumpsFunc = Callable[[Any], bytes]
LoadsFunc = Callable[[bytes], Any]


def serialize(value: Any, dumps: DumpsFunc) -> bytes:
    try:
        return dumps(value)
    except Exception as exc:
        raise StateSerializationError(f'Failed to serialize value: "{value}"') from exc


def deserialize(value: bytes, loads: LoadsFunc) -> Any:
    try:
        return loads(value)
    except Exception as exc:
        raise StateSerializationError(
            f'Failed to deserialize value: "{value!r}"'
        ) from exc


def int_to_bytes(value: int) -> bytes:
    return _int_pack(value)


def int_from_bytes(value: bytes) -> int:
    return _int_unpack(value)[0]


def encode_integer_pair(integer_1: int, integer_2: int) -> bytes:
    """
    Encode a pair of integers into bytes of the following format:
    ```<integer_1>|<integer_2>```

    Encoding integers this way make them sortable in RocksDB within the same prefix.

    :param integer_1: first integer
    :param integer_2: second integer
    :return: integers as bytes
    """
    return _int_pair_pack(integer_1, SEPARATOR, integer_2)


def decode_integer_pair(value: bytes) -> tuple[int, int]:
    """
    Decode a pair of integers from bytes of the following format:
    ```<integer_1>|<integer_2>```

    :param value: bytes
    :return: tuple of integers
    """
    integer_1, _, integer_2 = _int_pair_unpack(value)
    return integer_1, integer_2
