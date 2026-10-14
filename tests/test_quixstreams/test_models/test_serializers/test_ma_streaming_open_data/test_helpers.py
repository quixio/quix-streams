"""Unit tests for the module-level helpers in ``deserializer.py``."""

from quixstreams.models.serializers.ma_streaming_open_data.deserializer import (
    _to_tagged_union,
)


class TestToTaggedUnion:
    def test_root_key_is_type_and_value_is_rest_of_envelope(self):
        """The record's only key is the packet type and its value is the envelope without type."""
        packet_dict = {
            "type": "NewSession",
            "session_key": "abc",
            "is_essential": True,
            "content": {"data_source": "ecu1"},
        }
        assert _to_tagged_union(packet_dict) == {
            "NewSession": {
                "session_key": "abc",
                "is_essential": True,
                "content": {"data_source": "ecu1"},
            },
        }
