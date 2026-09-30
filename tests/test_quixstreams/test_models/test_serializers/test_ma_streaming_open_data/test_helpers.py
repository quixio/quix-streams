"""Unit tests for the small module-level helpers in ``deserializer.py``.

Validates the docstring contracts of ``_coerce_to_bytes`` and
``_to_tagged_union`` (deserializer.py, module docstring + lines 56-101)
which underpin the tagged-union filter shape documented in
docs/advanced/serialization.md ("Output shape — asymmetric").
"""

import base64

import pytest

from quixstreams.models.serializers.ma_streaming_open_data.deserializer import (
    _coerce_to_bytes,
    _to_tagged_union,
)


class TestCoerceToBytes:
    def test_none_returns_none(self):
        """Validates deserializer.py:90-91: None in -> None out."""
        assert _coerce_to_bytes(None) is None

    def test_bytes_passthrough(self):
        """Validates deserializer.py:92-93: bytes are returned unchanged."""
        assert _coerce_to_bytes(b"\x01\x02") == b"\x01\x02"

    def test_empty_string_returns_empty_bytes(self):
        """Validates deserializer.py:95-96: "" decodes to b""."""
        assert _coerce_to_bytes("") == b""

    def test_valid_base64_string_is_decoded(self):
        """Validates deserializer.py:97-98: base64 str -> bytes."""
        raw = b"hello world"
        encoded = base64.b64encode(raw).decode()
        assert _coerce_to_bytes(encoded) == raw

    def test_invalid_base64_string_returns_none(self):
        """Validates the fail-soft contract: bad base64 must not raise."""
        assert _coerce_to_bytes("not-valid-base64!!") is None

    def test_non_str_non_bytes_returns_none(self):
        """Validates deserializer.py:101: anything else -> None."""
        assert _coerce_to_bytes(12345) is None

    def test_bytearray_is_not_specially_coerced(self):
        """``bytearray`` is not ``bytes`` nor ``str``, so it falls through
        to the final ``return None`` (deserializer.py:101) despite the
        class docstring saying content bytes can arrive in "either form"
        (bytes or base64 str) — bytearray/memoryview are not among the
        two forms the docstring claims, and the code does not special
        case them either. See Tester report for the "surprises" note.
        """
        assert _coerce_to_bytes(bytearray(b"abc")) is None

    def test_memoryview_is_not_specially_coerced(self):
        """Same as above but for ``memoryview``."""
        assert _coerce_to_bytes(memoryview(b"abc")) is None


class TestToTaggedUnion:
    def test_wraps_type_and_content(self):
        """Validates deserializer.py:56-80: root key is Packet.type,
        value is the decoded content dict only (envelope dropped).
        """
        packet_dict = {
            "type": "NewSession",
            "session_key": "abc",
            "is_essential": True,
            "content": {"data_source": "ecu1"},
        }
        assert _to_tagged_union(packet_dict) == {
            "NewSession": {"data_source": "ecu1"},
        }

    def test_non_dict_content_falls_back_to_envelope(self):
        """When ``content`` isn't a dict (e.g. decode_content=False, still
        a base64 str), the envelope minus ``type`` becomes the inner
        value (deserializer.py:72-77).
        """
        packet_dict = {
            "type": "NewSession",
            "session_key": "abc",
            "content": "base64==",
        }
        result = _to_tagged_union(packet_dict)
        assert result == {
            "NewSession": {"session_key": "abc", "content": "base64=="},
        }

    def test_missing_content_falls_back_to_envelope(self):
        packet_dict = {"type": "NewSession", "session_key": "abc"}
        result = _to_tagged_union(packet_dict)
        assert result == {"NewSession": {"session_key": "abc"}}

    @pytest.mark.parametrize("bad_type", [None, "", 123])
    def test_missing_or_non_string_type_falls_back_to_empty_key(self, bad_type):
        """Validates deserializer.py:78-79: falsy/non-str type -> {"": inner}."""
        packet_dict = {"type": bad_type, "content": {"a": 1}}
        assert _to_tagged_union(packet_dict) == {"": {"a": 1}}
