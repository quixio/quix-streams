"""Tests for ``MAStreamingDeserializer``: packet decoding, row projection,
filtering, parameter name resolution and the output contract."""

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
    StubConsumer,
    data_format_definition_packet,
    double_column,
    double_row,
    packet_bytes,
    periodic_data_packet,
    row_data_packet,
    synchro_data_packet,
    with_format_identifier,
    wrap,
)

VALID = pb.DataStatus.DATA_STATUS_VALID
MISSING = pb.DataStatus.DATA_STATUS_MISSING
PARAMETER = pb.DataFormatType.DATA_FORMAT_TYPE_PARAMETER


def _definition(identifier: int, names: list[str]) -> bytes:
    """A ``DataFormatDefinition`` packet mapping ``identifier`` to ``names``."""
    return wrap(
        "DataFormatDefinition",
        data_format_definition_packet(
            identifier, PARAMETER, parameter_identifiers=names
        ),
        is_essential=True,
    )


def _identifier_periodic(identifier: int, n_columns: int) -> bytes:
    """An identifier-form ``PeriodicData`` packet with one sample per column."""
    columns = [double_column([(float(i), VALID)]) for i in range(n_columns)]
    pkt = periodic_data_packet([], start_time=0, interval=1, columns=columns)
    return wrap("PeriodicData", with_format_identifier(pkt, identifier))


class TestEnvelopeAndInnerDecode:
    @pytest.mark.parametrize("packet_type,payload_cls", list(CONTENT_TYPES.items()))
    def test_every_content_type_round_trips(self, packet_type, payload_cls):
        """Every registered packet type decodes to its message class's dict under its type key."""
        inner = payload_cls()
        value = wrap(packet_type, inner)
        deserializer = MAStreamingDeserializer(build_table=None)
        result = deserializer(value, DUMMY_CONTEXT)
        expected_content = MessageToDict(
            inner,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        ((root, envelope),) = result[0].items()
        assert root == packet_type
        assert envelope["content"] == expected_content

    def test_decode_content_false_leaves_content_as_base64(self):
        """With decode_content=False the envelope's content stays a base64 string."""
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner)
        deserializer = MAStreamingDeserializer(decode_content=False, build_table=None)
        ((_, envelope),) = deserializer(value, DUMMY_CONTEXT)[0].items()
        assert isinstance(envelope["content"], str)

    def test_unknown_packet_type_leaves_content_untouched(self):
        """An unregistered packet type keeps its content as a base64 string."""
        value = packet_bytes("TotallyUnknownType", content=b"whatever")
        deserializer = MAStreamingDeserializer(build_table=None)
        ((root, envelope),) = deserializer(value, DUMMY_CONTEXT)[0].items()
        assert root == "TotallyUnknownType"
        assert isinstance(envelope["content"], str)

    def test_extra_content_types_are_merged_and_decoded(self):
        """A type registered through extra_content_types is decoded with its class."""
        inner = pb.MetadataPacket()
        value = wrap("CustomType", inner)
        deserializer = MAStreamingDeserializer(
            build_table=None,
            extra_content_types={"CustomType": pb.MetadataPacket},
        )
        ((root, envelope),) = deserializer(value, DUMMY_CONTEXT)[0].items()
        assert root == "CustomType"
        assert envelope["content"] == {"metadata": {}}

    def test_builtin_content_types_take_precedence_on_collision(self):
        """A built-in type is decoded with the built-in class even if extra_content_types maps it too."""
        deserializer = MAStreamingDeserializer(
            build_table=None,
            extra_content_types={"NewSession": pb.MetadataPacket},
        )
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner)
        ((_, envelope),) = deserializer(value, DUMMY_CONTEXT)[0].items()
        assert envelope["content"] == {
            "data_source": "ecu1",
            "topic_partition_offsets": {},
        }


class TestDefaultProjection:
    def test_periodic_data_default_projection_is_flat_rows(self):
        """With no arguments a PeriodicData packet gives one flat row per sample."""
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
        """With no arguments a non-sample packet gives an empty list."""
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner)
        deserializer = MAStreamingDeserializer()
        assert deserializer(value, DUMMY_CONTEXT) == []


class TestTaggedUnionPath:
    def test_non_engineering_survives_as_tagged_union_when_filter_active(self):
        """A non-sample packet in filter["types"] gives one tagged-union record holding its envelope."""
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner, session_key="s1", is_essential=True)
        deserializer = MAStreamingDeserializer(filter={"types": ["NewSession"]})
        result = deserializer(value, DUMMY_CONTEXT)
        assert result == [
            {
                "NewSession": {
                    "session_key": "s1",
                    "is_essential": True,
                    "content": {"data_source": "ecu1", "topic_partition_offsets": {}},
                    "id": "0",
                }
            }
        ]


class TestFilterSemantics:
    def test_types_filter_drops_non_matching_packet(self):
        """A packet whose type is not in filter["types"] gives an empty list."""
        inner = pb.EventPacket(timestamp=5)
        value = wrap("Event", inner)
        deserializer = MAStreamingDeserializer(filter={"types": ["PeriodicData"]})
        assert deserializer(value, DUMMY_CONTEXT) == []

    def test_signals_filter_keeps_only_allowlisted_signal_rows(self):
        """filter["signals"] keeps only the flat rows of the listed signals."""
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
        """Type and signal filters apply together."""
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

    def test_filter_active_with_build_table_none_still_tags_via_slow_path(self):
        """With build_table=None a sample packet gives one tagged-union record with unprojected content."""
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
        assert "columns" in inner_value["content"]


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
        """A list-form build_table with an unsupported column raises ValueError at construction."""
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
        """Fast-path rows equal a manual projection of the slow-path content."""
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
        ((_, envelope),) = slow(value, DUMMY_CONTEXT)[0].items()
        content = envelope["content"]
        names = content["data_format"]["parameter_identifiers"]["parameter_identifiers"]
        start_time = int(content["start_time"])
        interval = content["interval"]
        rows_from_slow = []
        for col_idx, column in enumerate(content["columns"]):
            name = names[col_idx]
            for i, sample in enumerate(column["double_samples"]["samples"]):
                rows_from_slow.append(
                    {
                        "packet_type": "PeriodicData",
                        "timestamp": start_time + i * interval,
                        "name": name,
                        "value": sample["value"],
                        "validity": sample["status"],
                    }
                )

        assert rows_fast == rows_from_slow


class TestDataFormatDefinitionPacket:
    """``DataFormatDefinition`` decodes either branch of its ``format`` oneof."""

    def test_parameter_identifiers_branch(self):
        inner = data_format_definition_packet(
            identifier=5,
            type_=pb.DataFormatType.DATA_FORMAT_TYPE_PARAMETER,
            parameter_identifiers=["vCar", "nEngine"],
        )
        value = wrap("DataFormatDefinition", inner)
        deserializer = MAStreamingDeserializer(build_table=None)
        ((_, envelope),) = deserializer(value, DUMMY_CONTEXT)[0].items()
        assert envelope["content"] == {
            "identifier": "5",
            "type": "DATA_FORMAT_TYPE_PARAMETER",
            "parameter_identifiers": {"parameter_identifiers": ["vCar", "nEngine"]},
        }
        assert "event_identifier" not in envelope["content"]

    def test_event_identifier_branch(self):
        inner = data_format_definition_packet(
            identifier=6,
            type_=pb.DataFormatType.DATA_FORMAT_TYPE_EVENT,
            event_identifier="PIT_ENTRY",
        )
        value = wrap("DataFormatDefinition", inner)
        deserializer = MAStreamingDeserializer(build_table=None)
        ((_, envelope),) = deserializer(value, DUMMY_CONTEXT)[0].items()
        assert envelope["content"] == {
            "identifier": "6",
            "type": "DATA_FORMAT_TYPE_EVENT",
            "event_identifier": "PIT_ENTRY",
        }
        assert "parameter_identifiers" not in envelope["content"]

    def test_via_fast_path_non_engineering_branch_under_filter(self):
        """With the default build_table and the type in filter["types"], a definition gives one tagged-union record."""
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
                    "session_key": "",
                    "is_essential": False,
                    "content": {
                        "identifier": "1",
                        "type": "DATA_FORMAT_TYPE_PARAMETER",
                        "parameter_identifiers": {"parameter_identifiers": ["vCar"]},
                    },
                    "id": "0",
                }
            }
        ]


class TestParameterNameResolution:
    @pytest.mark.parametrize(
        "packet_type,inner",
        [
            pytest.param(
                "PeriodicData",
                with_format_identifier(
                    periodic_data_packet(
                        [],
                        start_time=0,
                        interval=1,
                        columns=[
                            double_column([(1.0, VALID)]),
                            double_column([(2.0, VALID)]),
                        ],
                    ),
                    7,
                ),
                id="PeriodicData",
            ),
            pytest.param(
                "SynchroData",
                with_format_identifier(
                    synchro_data_packet(
                        [],
                        start_time=0,
                        intervals=[],
                        columns=[
                            double_column([(1.0, VALID)]),
                            double_column([(2.0, VALID)]),
                        ],
                    ),
                    7,
                ),
                id="SynchroData",
            ),
            pytest.param(
                "RowData",
                with_format_identifier(
                    row_data_packet(
                        [],
                        timestamps=[0],
                        rows=[double_row([(1.0, VALID), (2.0, VALID)])],
                    ),
                    7,
                ),
                id="RowData",
            ),
        ],
    )
    def test_identifier_form_resolves_names_from_definition(self, packet_type, inner):
        """Rows from an identifier-form packet carry the names of a definition seen earlier on the data topic; previously col0 and col1."""
        deserializer = MAStreamingDeserializer()
        assert deserializer(_definition(7, ["vCar", "nEngine"]), DUMMY_CONTEXT) == []
        rows = deserializer(wrap(packet_type, inner), DUMMY_CONTEXT)
        assert [row["name"] for row in rows] == ["vCar", "nEngine"]

    def test_unknown_identifier_raises_key_error(self):
        """An identifier-form packet with no known definition raises KeyError; previously it gave rows named col0 and col1."""
        deserializer = MAStreamingDeserializer()
        with pytest.raises(KeyError):
            deserializer(_identifier_periodic(7, 2), DUMMY_CONTEXT)

    def test_unknown_identifier_seeds_from_definitions_topic(self):
        """An unknown identifier is resolved from the definitions topic, and a later miss reads only the new messages; previously definitions_topic was not accepted."""
        stub = StubConsumer("essentials.Car1", [_definition(7, ["vCar", "nEngine"])])
        deserializer = MAStreamingDeserializer(
            definitions_topic="essentials.Car1", consumer=stub
        )
        rows = deserializer(_identifier_periodic(7, 2), DUMMY_CONTEXT)
        assert [row["name"] for row in rows] == ["vCar", "nEngine"]

        stub.messages.append(_definition(8, ["nGear"]))
        rows = deserializer(_identifier_periodic(8, 1), DUMMY_CONTEXT)
        assert [row["name"] for row in rows] == ["nGear"]
        assert stub.assigned[-1][0].offset == 1


class TestOutputContract:
    def test_split_values_and_slow_path_returns_list(self):
        """split_values is True and the slow path returns a one-item list; previously split_values was False and the slow path returned a dict."""
        deserializer = MAStreamingDeserializer(build_table=None)
        assert deserializer.split_values is True
        value = wrap("NewSession", pb.NewSessionPacket(data_source="ecu1"))
        result = deserializer(value, DUMMY_CONTEXT)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_tagged_union_keeps_envelope_fields(self):
        """A tagged-union record keeps session_key, is_essential and id next to content; previously only the decoded content was kept."""
        inner = pb.NewSessionPacket(data_source="ecu1")
        value = wrap("NewSession", inner, session_key="s1", is_essential=True)
        deserializer = MAStreamingDeserializer(filter={"types": ["NewSession"]})
        assert deserializer(value, DUMMY_CONTEXT) == [
            {
                "NewSession": {
                    "session_key": "s1",
                    "is_essential": True,
                    "content": {"data_source": "ecu1", "topic_partition_offsets": {}},
                    "id": "0",
                }
            }
        ]

    def test_use_integers_for_enums_applies_to_projected_status(self):
        """With use_integers_for_enums=True the validity column holds the DataStatus integer; previously it held the enum name."""
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=0,
            interval=1,
            columns=[double_column([(1.0, VALID)])],
        )
        deserializer = MAStreamingDeserializer(use_integers_for_enums=True)
        rows = deserializer(wrap("PeriodicData", pkt), DUMMY_CONTEXT)
        assert rows[0]["validity"] == 1
