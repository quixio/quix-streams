import struct


def float_to_bytes(value: float) -> bytes:
    return struct.pack(">d", value)


def int_to_bytes(value: int) -> bytes:
    return struct.pack(">i", value)
