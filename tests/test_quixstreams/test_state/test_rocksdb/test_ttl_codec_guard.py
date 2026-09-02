"""
#12 (review batch 3): the TTL codec's stamp encoders are unsigned ``>Q`` packers,
so a negative expiry reaches ``struct.pack`` and raises a raw ``struct.error``
(crash-loop on every recurrence of the offending record). The encoders must reject
a negative / out-of-range expiry with a *descriptive* ``ValueError`` before the
pack, so any caller gets a diagnosable, catchable error. ``SENTINEL_NEVER``
(``2**64-1``) stays valid.
"""

import struct

import pytest

from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
)

_MAX_UINT64 = 2**64 - 1


class TestNegativeExpiryRejected:
    @pytest.mark.parametrize("bad", [-1, -1000, -(2**40), -(2**63)])
    def test_encode_ttl_value_rejects_negative(self, bad):
        with pytest.raises(ValueError):
            encode_ttl_value(bad, b"payload")

    @pytest.mark.parametrize("bad", [-1, -1000, -(2**40)])
    def test_encode_index_key_rejects_negative(self, bad):
        with pytest.raises(ValueError):
            encode_index_key(bad, b"pfx|key")

    def test_encode_ttl_value_rejects_overflow(self):
        with pytest.raises(ValueError):
            encode_ttl_value(2**64, b"payload")

    def test_encode_index_key_rejects_overflow(self):
        with pytest.raises(ValueError):
            encode_index_key(2**64 + 5, b"pfx|key")

    def test_error_is_valueerror_not_structerror(self):
        # The whole point of #12: a descriptive ValueError, never a raw
        # struct.error that a caller cannot distinguish/handle.
        try:
            encode_ttl_value(-1, b"x")
        except ValueError:
            pass
        except struct.error:  # pragma: no cover - the bug we are fixing
            pytest.fail("encode_ttl_value raised struct.error instead of ValueError")


class TestValidStampsStillEncode:
    def test_sentinel_never_round_trips(self):
        blob = encode_ttl_value(SENTINEL_NEVER, b"payload")
        stamp, payload = decode_ttl_value(blob)
        assert stamp == SENTINEL_NEVER
        assert payload == b"payload"

    def test_max_uint64_is_valid(self):
        # SENTINEL_NEVER == 2**64 - 1 is the boundary; it must be accepted.
        assert SENTINEL_NEVER == _MAX_UINT64
        encode_ttl_value(_MAX_UINT64, b"v")  # must not raise

    def test_normal_positive_expiry_round_trips(self):
        expiry = 1_780_000_000_000
        blob = encode_ttl_value(expiry, b'"value"')
        stamp, payload = decode_ttl_value(blob)
        assert stamp == expiry
        assert payload == b'"value"'

    def test_zero_is_accepted_by_encoder(self):
        # 0 is a valid uint64; the encoder rejects only < 0 / > 2**64-1. (The
        # read-side _safe_decode_stamp separately treats 0 as never-expires.)
        blob = encode_ttl_value(0, b"v")
        stamp, _ = decode_ttl_value(blob)
        assert stamp == 0

    def test_index_key_positive_round_trips(self):
        expiry = 1_780_000_000_000
        blob = encode_index_key(expiry, b"pfx|key")
        stamp, user_key = decode_index_key(blob)
        assert stamp == expiry
        assert user_key == b"pfx|key"
