"""Unit tests for filter.py's ``resolve_filter`` validation, plus
constructor-level wiring tests on ``MAStreamingDeserializer``.

Validates the "Validation errors raised at construction" contract in
deserializer.py's ``__init__`` docstring (lines 267-284) and
docs/advanced/serialization.md §Filtering / §Configuring the filter
from environment variables (empty-list anti-footgun, `{}` no-op alias).
"""

import pytest

from quixstreams.models.serializers.ma_streaming_open_data.deserializer import (
    MAStreamingDeserializer,
)
from quixstreams.models.serializers.ma_streaming_open_data.filter import (
    ResolvedFilter,
    resolve_filter,
)

KNOWN_TYPES = frozenset({"PeriodicData", "RowData", "NewSession"})


class TestResolveFilter:
    def test_none_is_a_no_op(self):
        """Validates deserializer.py: filter=None disables both axes."""
        resolved = resolve_filter(
            None,
            known_packet_types=KNOWN_TYPES,
            decode_content=True,
            build_table_active=True,
        )
        assert resolved == ResolvedFilter(types=None, signals=None)
        assert resolved.is_active is False

    def test_empty_mapping_is_a_no_op_alias_for_none(self):
        """Validates serialization.md:232: `{}` is accepted as a no-op
        alias for filter=None.
        """
        resolved = resolve_filter(
            {},
            known_packet_types=KNOWN_TYPES,
            decode_content=True,
            build_table_active=True,
        )
        assert resolved.is_active is False

    def test_non_mapping_raises_type_error(self):
        """Validates filter.py:102-106."""
        with pytest.raises(TypeError):
            resolve_filter(
                ["PeriodicData"],
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_unknown_key_raises_value_error(self):
        """Validates filter.py:108-113: typo keys (e.g. "signal") rejected."""
        with pytest.raises(ValueError, match="unknown keys"):
            resolve_filter(
                {"signal": ["vCar"]},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_bare_string_for_types_raises_type_error(self):
        """A bare string is iterable-of-chars, not a sequence of type
        names — filter.py:53-57 rejects it explicitly.
        """
        with pytest.raises(TypeError):
            resolve_filter(
                {"types": "PeriodicData"},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_empty_types_list_raises_value_error(self):
        """Validates filter.py:59-63: empty list is an anti-footgun error,
        not silently treated as "no constraint".
        """
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
        """Validates filter.py:118-124: typo'd packet type names rejected
        at construction, not silently dropped at runtime.
        """
        with pytest.raises(ValueError, match="unknown packet types"):
            resolve_filter(
                {"types": ["NotARealType"]},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=True,
            )

    def test_signals_with_decode_content_false_raises_value_error(self):
        """Validates deserializer.py docstring: `signals` set with
        `decode_content=False` raises ValueError.
        """
        with pytest.raises(ValueError, match="decode_content=False"):
            resolve_filter(
                {"signals": ["vCar"]},
                known_packet_types=KNOWN_TYPES,
                decode_content=False,
                build_table_active=True,
            )

    def test_signals_with_build_table_inactive_raises_value_error(self):
        """Validates deserializer.py docstring: `signals` set with
        `build_table=None` raises ValueError.
        """
        with pytest.raises(ValueError, match="build_table is disabled"):
            resolve_filter(
                {"signals": ["vCar"]},
                known_packet_types=KNOWN_TYPES,
                decode_content=True,
                build_table_active=False,
            )

    def test_unknown_signal_names_are_accepted(self):
        """Signal names are runtime data — unlike `types`, unknown
        `signals` are not validated against a registry (filter.py:96-97).
        """
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
    """Confirms the deserializer wires ``filter`` through to
    ``resolve_filter`` correctly (deserializer.py:353-362).
    """

    def test_unknown_type_rejected_at_construction(self):
        with pytest.raises(ValueError, match="unknown packet types"):
            MAStreamingDeserializer(filter={"types": ["NotARealType"]})

    def test_non_mapping_filter_rejected(self):
        with pytest.raises(TypeError):
            MAStreamingDeserializer(filter=["PeriodicData"])

    def test_signals_with_decode_content_false_rejected(self):
        with pytest.raises(ValueError):
            MAStreamingDeserializer(decode_content=False, filter={"signals": ["vCar"]})

    def test_signals_with_build_table_none_rejected(self):
        with pytest.raises(ValueError):
            MAStreamingDeserializer(build_table=None, filter={"signals": ["vCar"]})

    def test_extra_content_types_extend_known_type_registry(self):
        """``filter["types"]`` validation must accept types registered
        via ``extra_content_types`` too (deserializer.py: filter resolved
        after ``_content_types`` is built).
        """
        from quixstreams.models.serializers.ma_streaming_open_data import (
            open_data_pb2 as pb,
        )

        deserializer = MAStreamingDeserializer(
            extra_content_types={"CustomType": pb.MetadataPacket},
            filter={"types": ["CustomType"]},
        )
        assert deserializer is not None
