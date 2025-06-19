import pytest

from quixstreams.state.metadata import SEPARATOR
from quixstreams.state.serialization import (
    append_integer,
    decode_integer_pair,
    encode_integer_pair,
)


@pytest.mark.parametrize(
    "start, end",
    [
        (0, 0),
        (1, 18446744073709551615),
    ],
)
def test_encode_integer_pair(start, end):
    # This test also covers decode_integer_pair function
    key = encode_integer_pair(start, end)
    assert isinstance(key, bytes)

    decoded_start, decoded_end = decode_integer_pair(key)
    assert decoded_start == start
    assert decoded_end == end


@pytest.mark.parametrize("base_bytes", [b"", b"base_bytes"])
def test_append_integer(
    base_bytes: bytes,
):
    start, end = 0, 10
    window_key = base_bytes + SEPARATOR + encode_integer_pair(start, end)
    window_base_bytes = append_integer(base_bytes=base_bytes, integer=start)
    assert window_key.startswith(window_base_bytes)
