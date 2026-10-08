"""Helpers to build MA Streaming Open Data v1 protobuf fixtures in-process.

No binary blobs are committed — every fixture is constructed from
``open_data_pb2`` message classes and serialized on the fly, per the
Tester brief's constraint against committing ``.bin`` fixtures.
"""

from __future__ import annotations

from typing import Iterable, Optional, Sequence

from quixstreams.models.serializers.ma_streaming_open_data import (
    open_data_pb2 as pb,
)


def packet_bytes(
    packet_type: str,
    content: bytes = b"",
    *,
    session_key: str = "",
    is_essential: bool = False,
    packet_id: int = 0,
) -> bytes:
    """Serialize a ``Packet`` envelope carrying raw ``content`` bytes."""
    packet = pb.Packet(
        type=packet_type,
        session_key=session_key,
        is_essential=is_essential,
        content=content,
        id=packet_id,
    )
    return packet.SerializeToString()


def wrap(packet_type: str, inner, **envelope_kwargs) -> bytes:
    """Serialize ``inner`` and wrap it in a ``Packet`` of ``packet_type``."""
    return packet_bytes(packet_type, inner.SerializeToString(), **envelope_kwargs)


def double_column(samples: Sequence[tuple]) -> pb.SampleColumn:
    """Build a ``SampleColumn`` populated with ``(value, status)`` doubles."""
    column = pb.SampleColumn()
    for value, status in samples:
        column.double_samples.samples.add(value=value, status=status)
    return column


def double_row(samples: Sequence[tuple]) -> pb.SampleRow:
    """Build a ``SampleRow`` populated with ``(value, status)`` doubles."""
    row = pb.SampleRow()
    for value, status in samples:
        row.double_samples.samples.add(value=value, status=status)
    return row


def empty_column() -> pb.SampleColumn:
    """A column whose ``list`` oneof is unset (no samples at all)."""
    return pb.SampleColumn()


def empty_row() -> pb.SampleRow:
    """A row whose ``list`` oneof is unset (no samples at all)."""
    return pb.SampleRow()


def periodic_data_packet(
    names: Sequence[str],
    start_time: int,
    interval: int,
    columns: Sequence[pb.SampleColumn],
) -> pb.PeriodicDataPacket:
    pkt = pb.PeriodicDataPacket(start_time=start_time, interval=interval)
    pkt.data_format.parameter_identifiers.parameter_identifiers.extend(names)
    pkt.columns.extend(columns)
    return pkt


def synchro_data_packet(
    names: Sequence[str],
    start_time: int,
    intervals: Sequence[int],
    columns: Sequence[pb.SampleColumn],
) -> pb.SynchroDataPacket:
    pkt = pb.SynchroDataPacket(start_time=start_time)
    pkt.data_format.parameter_identifiers.parameter_identifiers.extend(names)
    pkt.intervals.extend(intervals)
    pkt.columns.extend(columns)
    return pkt


def row_data_packet(
    names: Sequence[str],
    timestamps: Sequence[int],
    rows: Sequence[pb.SampleRow],
) -> pb.RowDataPacket:
    pkt = pb.RowDataPacket()
    pkt.data_format.parameter_identifiers.parameter_identifiers.extend(names)
    pkt.timestamps.extend(timestamps)
    pkt.rows.extend(rows)
    return pkt


def data_format_definition_packet(
    identifier: int,
    type_: "pb.DataFormatType.ValueType",
    *,
    parameter_identifiers: Optional[Iterable[str]] = None,
    event_identifier: Optional[str] = None,
) -> pb.DataFormatDefinitionPacket:
    pkt = pb.DataFormatDefinitionPacket(identifier=identifier, type=type_)
    if parameter_identifiers is not None:
        pkt.parameter_identifiers.parameter_identifiers.extend(parameter_identifiers)
    if event_identifier is not None:
        pkt.event_identifier = event_identifier
    return pkt
