import pytest

from quixstreams.state.serialization import (
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
    key = encode_integer_pair(start, end)
    assert isinstance(key, bytes)

    decoded_start, decoded_end = decode_integer_pair(key)
    assert decoded_start == start
    assert decoded_end == end
