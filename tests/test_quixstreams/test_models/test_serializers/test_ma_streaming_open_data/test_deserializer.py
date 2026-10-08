"""Tests for ``MAStreamingDeserializer`` (deserializer.py).

The spec for this feature is the class docstring (deserializer.py:104-152)
plus docs/advanced/serialization.md ("MA Streaming Open Data" section).
Every test below cites the specific contract it validates.
"""

import pytest
from google.protobuf.json_format import MessageToDict

from quixstreams.models.serializers.ma_streaming_open_data import open_data_pb2 as pb
from quixstreams.models.serializers.ma_streaming_open_data.content_types import (
    CONTENT_TYPES,
)
from quixstreams.models.serializers.ma_streaming_open_data.deserializer import (
    MAStreamingDeserializer,
)

from ..constants import DUMMY_CONTEXT
from .builders import (
    data_format_definition_packet,
    double_column,
    packet_bytes,
    periodic_data_packet,
    wrap,
)

VALID = pb.DataStatus.DATA_STATUS_VALID
MISSING = pb.DataStatus.DATA_STATUS_MISSING


class TestEnvelopeAndInnerDecode:
    @pytest.mark.parametrize("packet_type,payload_cls", list(CONTENT_TYPES.items()))
    def test_every_content_type_round_trips(self, packet_type, payload_cls):
        """Validates deserializer.py Stage 1 + Stage 2 dispatch (module
        docstring, lines 1-17): every ``Packet.type`` in the registry
        must decode to the matching message class's canonical dict.
        """
        inner = payload_cls()
        value = wrap(packet_type, inner)
        deserializer = MAStreamingDeserializer(build_table=None)
        result = deserializer(value, DUMMY_CONTEXT)
        expected_content = MessageToDict(
            inner,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        assert result["type"] == packet_type
        assert result["content"] == expected_content

    def test_decode_content_false_leaves_content_as_base64(self):
        """Validates __init__ docstring: decode_content=False leaves
        ``content`` as the Stage-1 base64 string.
        """
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner)
        deserializer = MAStreamingDeserializer(decode_content=False, build_table=None)
        result = deserializer(value, DUMMY_CONTEXT)
        assert isinstance(result["content"], str)

    def test_unknown_packet_type_leaves_content_untouched(self):
        """Validates deserializer.py:550-559: unknown Packet.type is
        fail-soft — content is left as the base64 string, no raise.
        """
        value = packet_bytes("TotallyUnknownType", content=b"whatever")
        deserializer = MAStreamingDeserializer(build_table=None)
        result = deserializer(value, DUMMY_CONTEXT)
        assert result["type"] == "TotallyUnknownType"
        assert isinstance(result["content"], str)

    def test_inner_decode_error_is_fail_soft(self):
        """Validates deserializer.py:569-576: a garbled inner payload
        must not raise — content is left as the base64 string.
        """
        value = packet_bytes("NewSession", content=b"\x08")
        deserializer = MAStreamingDeserializer(build_table=None)
        result = deserializer(value, DUMMY_CONTEXT)
        assert isinstance(result["content"], str)

    def test_fast_path_bad_envelope_bytes_is_fail_soft(self):
        """Validates deserializer.py:419-425: a malformed outer Packet
        on the fast path returns [] rather than raising.
        """
        deserializer = MAStreamingDeserializer()
        assert deserializer(b"\x08", DUMMY_CONTEXT) == []

    def test_extra_content_types_are_merged_and_decoded(self):
        """Validates __init__ docstring: extra_content_types lets
        callers register OEM extensions without forking the library.
        """
        inner = pb.MetadataPacket()
        value = wrap("CustomType", inner)
        deserializer = MAStreamingDeserializer(
            build_table=None,
            extra_content_types={"CustomType": pb.MetadataPacket},
        )
        result = deserializer(value, DUMMY_CONTEXT)
        assert result["type"] == "CustomType"
        assert result["content"] == {"metadata": {}}

    def test_builtin_content_types_take_precedence_on_collision(self):
        """Validates __init__ docstring: "Built-in entries take
        precedence on key collision".
        """
        deserializer = MAStreamingDeserializer(
            build_table=None,
            extra_content_types={"NewSession": pb.MetadataPacket},
        )
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner)
        result = deserializer(value, DUMMY_CONTEXT)
        # Decoded using the built-in NewSessionPacket schema, not MetadataPacket.
        assert result["content"] == {
            "data_source": "ecu1",
            "topic_partition_offsets": {},
        }


class TestDefaultProjection:
    def test_periodic_data_default_projection_is_flat_rows(self):
        """Validates serialization.md:148: with no arguments the
        deserializer returns flat rows (packet_type/timestamp/name/
        value/validity) for sample-bearing packets.
        """
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=1000,
            interval=10,
            columns=[double_column([(312.4, VALID), (313.1, VALID)])],
        )
        value = wrap("PeriodicData", pkt, session_key="s1")
        deserializer = MAStreamingDeserializer()
        rows = deserializer(value, DUMMY_CONTEXT)
        assert rows == [
            {
                "packet_type": "PeriodicData",
                "timestamp": 1000,
                "name": "vCar",
                "value": 312.4,
                "validity": "DATA_STATUS_VALID",
            },
            {
                "packet_type": "PeriodicData",
                "timestamp": 1010,
                "name": "vCar",
                "value": 313.1,
                "validity": "DATA_STATUS_VALID",
            },
        ]

    def test_non_engineering_packet_default_returns_empty_list(self):
        """Validates serialization.md:148: "...and an empty list for
        everything else."
        """
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner)
        deserializer = MAStreamingDeserializer()
        assert deserializer(value, DUMMY_CONTEXT) == []


class TestTaggedUnionPath:
    def test_non_engineering_survives_as_tagged_union_when_filter_active(self):
        """Validates deserializer.py __init__ docstring
        "Shape contract (tagged-union)": non-sample-bearing packets
        that survive the type filter emit one tagged-union record.
        """
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner, session_key="s1", is_essential=True)
        deserializer = MAStreamingDeserializer(filter={"types": ["NewSession"]})
        result = deserializer(value, DUMMY_CONTEXT)
        assert result == [
            {"NewSession": {"data_source": "ecu1", "topic_partition_offsets": {}}}
        ]

    def test_envelope_fields_are_dropped_from_tagged_union_inner_value(self):
        """Validates deserializer.py:60-64: session_key/is_essential/id
        are dropped from the inner tagged-union value.
        """
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner, session_key="s1", is_essential=True)
        deserializer = MAStreamingDeserializer(filter={"types": ["NewSession"]})
        ((_, inner_value),) = deserializer(value, DUMMY_CONTEXT)[0].items()
        assert "session_key" not in inner_value
        assert "is_essential" not in inner_value
        assert "id" not in inner_value
        assert "type" not in inner_value


class TestFilterSemantics:
    def test_types_filter_drops_non_matching_packet(self):
        """Validates __init__ docstring: filter["types"] is a pure
        allowlist — anything else is dropped ([]).
        """
        inner = pb.EventPacket(timestamp=5)
        value = wrap("Event", inner)
        deserializer = MAStreamingDeserializer(filter={"types": ["PeriodicData"]})
        assert deserializer(value, DUMMY_CONTEXT) == []

    def test_signals_filter_keeps_only_allowlisted_signal_rows(self):
        """Validates __init__ docstring: filter["signals"] keeps only
        rows whose signal name is in the allowlist. Engineering rows
        stay flat even under an active filter (matches
        serialization.md's asymmetric shape, see also Bug report re:
        the class docstring's conflicting tagged-union example).
        """
        pkt = periodic_data_packet(
            names=["vCar", "nEngine"],
            start_time=0,
            interval=1,
            columns=[
                double_column([(1.0, VALID)]),
                double_column([(2.0, VALID)]),
            ],
        )
        value = wrap("PeriodicData", pkt)
        deserializer = MAStreamingDeserializer(filter={"signals": ["nEngine"]})
        result = deserializer(value, DUMMY_CONTEXT)
        assert result == [
            {
                "packet_type": "PeriodicData",
                "timestamp": 0,
                "name": "nEngine",
                "value": 2.0,
                "validity": "DATA_STATUS_VALID",
            }
        ]

    def test_signals_filter_matching_nothing_returns_empty_list(self):
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=0,
            interval=1,
            columns=[double_column([(1.0, VALID)])],
        )
        value = wrap("PeriodicData", pkt)
        deserializer = MAStreamingDeserializer(filter={"signals": ["unknown"]})
        assert deserializer(value, DUMMY_CONTEXT) == []

    def test_types_and_signals_combined(self):
        """Validates __init__ docstring combining-filters example."""
        engineering = wrap(
            "PeriodicData",
            periodic_data_packet(
                names=["vCar", "nEngine"],
                start_time=0,
                interval=1,
                columns=[
                    double_column([(1.0, VALID)]),
                    double_column([(2.0, VALID)]),
                ],
            ),
        )
        dropped_type = wrap("Event", pb.EventPacket(timestamp=1))
        deserializer = MAStreamingDeserializer(
            filter={"types": ["PeriodicData", "NewSession"], "signals": ["nEngine"]}
        )
        assert deserializer(dropped_type, DUMMY_CONTEXT) == []
        result = deserializer(engineering, DUMMY_CONTEXT)
        assert result == [
            {
                "packet_type": "PeriodicData",
                "timestamp": 0,
                "name": "nEngine",
                "value": 2.0,
                "validity": "DATA_STATUS_VALID",
            }
        ]

    def test_filter_none_and_build_table_none_preserves_raw_dict_shape(self):
        """Validates __init__ docstring: "When filter=None today's
        no-wrap shapes are preserved (raw dict, not list-wrapped)."
        """
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner, session_key="s1")
        deserializer = MAStreamingDeserializer(build_table=None)
        result = deserializer(value, DUMMY_CONTEXT)
        assert isinstance(result, dict)
        assert result["type"] == "NewSession"
        assert result["session_key"] == "s1"
        assert result["content"] == {
            "data_source": "ecu1",
            "topic_partition_offsets": {},
        }

    def test_filter_active_with_build_table_none_still_tags_via_slow_path(self):
        """Validates deserializer.py:373-374 + 386-404: with
        build_table=None the fast path never engages, so any surviving
        packet type (including sample-bearing ones) is wrapped as a
        single tagged-union record holding the un-projected content —
        not fanned into per-sample rows.
        """
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=0,
            interval=1,
            columns=[double_column([(1.0, VALID)])],
        )
        value = wrap("PeriodicData", pkt)
        deserializer = MAStreamingDeserializer(
            build_table=None, filter={"types": ["PeriodicData"]}
        )
        result = deserializer(value, DUMMY_CONTEXT)
        assert len(result) == 1
        ((root_key, inner_value),) = result[0].items()
        assert root_key == "PeriodicData"
        assert "columns" in inner_value  # un-projected raw content, not rows


class TestBuildTableProjection:
    def test_list_form_projects_only_requested_columns(self):
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=0,
            interval=1,
            columns=[double_column([(1.0, VALID)])],
        )
        value = wrap("PeriodicData", pkt)
        deserializer = MAStreamingDeserializer(
            build_table=["parameter_identifier", "value"]
        )
        rows = deserializer(value, DUMMY_CONTEXT)
        assert rows == [{"parameter_identifier": "vCar", "value": 1.0}]

    def test_dict_form_renames_output_columns(self):
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=100,
            interval=1,
            columns=[double_column([(1.0, VALID)])],
        )
        value = wrap("PeriodicData", pkt)
        deserializer = MAStreamingDeserializer(
            build_table={
                "sig": ["parameter_identifier"],
                "ts": ["start_time", "timestamp"],
            }
        )
        rows = deserializer(value, DUMMY_CONTEXT)
        assert rows == [{"sig": "vCar", "ts": 100}]

    def test_unsupported_column_name_raises(self):
        """Validates __init__ docstring: list form validates against
        SUPPORTED_TABLE_COLUMNS at construction.
        """
        with pytest.raises(ValueError, match="unsupported column"):
            MAStreamingDeserializer(build_table=["bogus_column"])

    def test_dict_form_empty_sources_raises(self):
        with pytest.raises(ValueError, match="at least one source field"):
            MAStreamingDeserializer(build_table={"x": []})

    def test_dict_form_unrecognized_source_raises(self):
        with pytest.raises(ValueError, match="no recognized field"):
            MAStreamingDeserializer(build_table={"x": ["bogus_field"]})


class TestFastSlowPathConsistency:
    def test_fast_path_rows_match_manual_projection_of_slow_path_dict(self):
        """Cross-checks the fast path's direct-protobuf-attribute
        computation (deserializer.py:406-515) against the slow path's
        MessageToDict-based decode (deserializer.py:536-584): both must
        describe the same wire data.
        """
        pkt = periodic_data_packet(
            names=["vCar", "nEngine"],
            start_time=1000,
            interval=10,
            columns=[
                double_column([(1.0, VALID), (2.0, MISSING)]),
                double_column([(11.0, VALID)]),
            ],
        )
        value = wrap("PeriodicData", pkt, session_key="s1")

        fast = MAStreamingDeserializer()
        rows_fast = fast(value, DUMMY_CONTEXT)

        slow = MAStreamingDeserializer(build_table=None)
        raw = slow(value, DUMMY_CONTEXT)
        content = raw["content"]
        names = content["data_format"]["parameter_identifiers"]["parameter_identifiers"]
        start_time = int(content["start_time"])
        interval = content["interval"]
        rows_from_slow = []
        for col_idx, column in enumerate(content["columns"]):
            name = names[col_idx]
            for i, sample in enumerate(column["double_samples"]["samples"]):
                rows_from_slow.append(
                    {
                        "packet_type": raw["type"],
                        "timestamp": start_time + i * interval,
                        "name": name,
                        "value": sample["value"],
                        "validity": sample["status"],
                    }
                )

        assert rows_fast == rows_from_slow


class TestDataFormatDefinitionPacket:
    """Added in commit 5edc9716; the oneof ``format`` resolves to either
    ``parameter_identifiers`` or ``event_identifier``.
    """

    def test_parameter_identifiers_branch(self):
        inner = data_format_definition_packet(
            identifier=5,
            type_=pb.DataFormatType.DATA_FORMAT_TYPE_PARAMETER,
            parameter_identifiers=["vCar", "nEngine"],
        )
        value = wrap("DataFormatDefinition", inner)
        deserializer = MAStreamingDeserializer(build_table=None)
        result = deserializer(value, DUMMY_CONTEXT)
        assert result["content"] == {
            "identifier": "5",
            "type": "DATA_FORMAT_TYPE_PARAMETER",
            "parameter_identifiers": {"parameter_identifiers": ["vCar", "nEngine"]},
        }
        assert "event_identifier" not in result["content"]

    def test_event_identifier_branch(self):
        inner = data_format_definition_packet(
            identifier=6,
            type_=pb.DataFormatType.DATA_FORMAT_TYPE_EVENT,
            event_identifier="PIT_ENTRY",
        )
        value = wrap("DataFormatDefinition", inner)
        deserializer = MAStreamingDeserializer(build_table=None)
        result = deserializer(value, DUMMY_CONTEXT)
        assert result["content"] == {
            "identifier": "6",
            "type": "DATA_FORMAT_TYPE_EVENT",
            "event_identifier": "PIT_ENTRY",
        }
        assert "parameter_identifiers" not in result["content"]

    def test_via_fast_path_non_engineering_branch_under_filter(self):
        """DataFormatDefinition is non-sample-bearing, so with the
        default (active) build_table it is routed through
        ``_slow_path_single_packet`` from inside the fast path
        (deserializer.py:432-441).
        """
        inner = data_format_definition_packet(
            identifier=1,
            type_=pb.DataFormatType.DATA_FORMAT_TYPE_PARAMETER,
            parameter_identifiers=["vCar"],
        )
        value = wrap("DataFormatDefinition", inner)
        deserializer = MAStreamingDeserializer(
            filter={"types": ["DataFormatDefinition"]}
        )
        result = deserializer(value, DUMMY_CONTEXT)
        assert result == [
            {
                "DataFormatDefinition": {
                    "identifier": "1",
                    "type": "DATA_FORMAT_TYPE_PARAMETER",
                    "parameter_identifiers": {"parameter_identifiers": ["vCar"]},
                }
            }
        ]
