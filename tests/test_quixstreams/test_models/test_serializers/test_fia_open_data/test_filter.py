"""Unit tests for ``resolve_filter`` validation and for the filter checks
``FIAOpenDataDeserializer`` runs at construction."""

import pytest

from quixstreams.models.serializers.fia_open_data.deserializer import (
    FIAOpenDataDeserializer,
)
from quixstreams.models.serializers.fia_open_data.filter import (
    ResolvedFilter,
    resolve_filter,
)

KNOWN_TYPES = frozenset({"PeriodicData", "RowData", "NewSession"})


class TestResolveFilter:
    def test_none_is_a_no_op(self):
        """filter=None disables both axes."""
        resolved = resolve_filter(
            None,
            known_packet_types=KNOWN_TYPES,
            decode_content=True,
            build_table_active=True,
        )
        assert resolved == ResolvedFilter(types=None, signals=None)
        assert resolved.is_active is False

    def test_empty_mapping_is_a_no_op_alias_for_none(self):
        """An empty mapping is accepted as a no-op alias for filter=None."""
        resolved = resolve_filter(
            {},
            known_packet_types=KNOWN_TYPES,
            decode_content=True,
            build_table_active=True,
        )
        assert resolved.is_active is False

    def test_non_mapping_raises_type_error(self):
        """A non-mapping filter raises TypeError."""
        with pytest.raises(TypeError):
            resolve_filter(
                ["PeriodicData"],
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_unknown_key_raises_value_error(self):
        """An unknown key such as "signal" raises ValueError."""
        with pytest.raises(ValueError, match="unknown keys"):
            resolve_filter(
                {"signal": ["vCar"]},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_bare_string_for_types_raises_type_error(self):
        """A bare string for "types" raises TypeError."""
        with pytest.raises(TypeError):
            resolve_filter(
                {"types": "PeriodicData"},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_empty_types_list_raises_value_error(self):
        """An empty "types" list raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            resolve_filter(
                {"types": []},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_non_string_entries_raise_type_error(self):
        with pytest.raises(TypeError):
            resolve_filter(
                {"types": [123]},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_unknown_packet_type_raises_value_error(self):
        """An unknown packet type in "types" raises ValueError."""
        with pytest.raises(ValueError, match="unknown packet types"):
            resolve_filter(
                {"types": ["NotARealType"]},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_signals_with_decode_content_false_raises_value_error(self):
        """Setting "signals" with decode_content=False raises ValueError."""
        with pytest.raises(ValueError, match="decode_content=False"):
            resolve_filter(
                {"signals": ["vCar"]},
                known_packet_types=KNOWN_TYPES,
                decode_content=False,
                build_table_active=True,
            )

    def test_signals_with_build_table_inactive_raises_value_error(self):
        """Setting "signals" with build_table disabled raises ValueError."""
        with pytest.raises(ValueError, match="build_table is disabled"):
            resolve_filter(
                {"signals": ["vCar"]},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=False,
            )

    def test_unknown_signal_names_are_accepted(self):
        """Unknown signal names are accepted."""
        resolved = resolve_filter(
            {"signals": ["totally-unknown-signal"]},
            known_packet_types=KNOWN_TYPES,
            decode_content=True,
            build_table_active=True,
        )
        assert resolved.signals == frozenset({"totally-unknown-signal"})

    def test_both_axes_combined(self):
        resolved = resolve_filter(
            {"types": ["PeriodicData", "NewSession"], "signals": ["vCar"]},
            known_packet_types=KNOWN_TYPES,
            decode_content=True,
            build_table_active=True,
        )
        assert resolved.types == frozenset({"PeriodicData", "NewSession"})
        assert resolved.signals == frozenset({"vCar"})
        assert resolved.is_active is True


class TestDeserializerFilterConstruction:
    """``FIAOpenDataDeserializer`` applies ``resolve_filter`` at construction."""

    def test_unknown_type_rejected_at_construction(self):
        with pytest.raises(ValueError, match="unknown packet types"):
            FIAOpenDataDeserializer(filter={"types": ["NotARealType"]})

    def test_non_mapping_filter_rejected(self):
        with pytest.raises(TypeError):
            FIAOpenDataDeserializer(filter=["PeriodicData"])

    def test_signals_with_decode_content_false_rejected(self):
        with pytest.raises(ValueError):
            FIAOpenDataDeserializer(decode_content=False, filter={"signals": ["vCar"]})

    def test_signals_with_build_table_none_rejected(self):
        with pytest.raises(ValueError):
            FIAOpenDataDeserializer(build_table=None, filter={"signals": ["vCar"]})

    def test_extra_content_types_extend_known_type_registry(self):
        """filter["types"] accepts types registered through extra_content_types."""
        from quixstreams.models.serializers.fia_open_data import (
            open_data_pb2 as pb,
        )

        deserializer = FIAOpenDataDeserializer(
            extra_content_types={"CustomType": pb.MetadataPacket},
            filter={"types": ["CustomType"]},
        )
        assert deserializer is not None
