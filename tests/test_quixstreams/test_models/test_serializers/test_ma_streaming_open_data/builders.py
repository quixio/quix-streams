"""Helpers to build MA Streaming Open Data v1 protobuf fixtures in-process.

Every fixture is constructed from ``open_data_pb2`` message classes and
serialized on the fly. ``StubConsumer`` stands in for
``quixstreams.kafka.Consumer`` when a deserializer reads a definitions topic.
"""

from __future__ import annotations

from types import SimpleNamespace
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


def with_format_identifier(pkt, identifier: int):
    """Switch ``pkt.data_format`` to the ``data_format_identifier`` form."""
    pkt.data_format.data_format_identifier = identifier
    return pkt


class StubMessage:
    """A successful Kafka message on partition 0."""

    def __init__(self, topic: str, offset: int, value: bytes):
        self._topic = topic
        self._offset = offset
        self._value = value

    def error(self):
        return None

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._value


class StubConsumer:
    """A one-partition consumer whose log is the public ``messages`` list.

    ``poll`` returns ``None`` once the log is read, as a real poll does on
    timeout. ``trailing_gap`` puts the high watermark that many offsets past
    the last message, as a transaction marker or a compaction gap does.
    """

    def __init__(self, topic: str, messages: list[bytes], trailing_gap: int = 0):
        self.topic = topic
        self.messages = messages
        self.trailing_gap = trailing_gap
        self.assigned: list[list] = []
        self.polls = 0
        self._cursor = 0

    def list_topics(self, topic=None, timeout=None):
        return SimpleNamespace(
            topics={self.topic: SimpleNamespace(partitions={0: None})}
        )

    def get_watermark_offsets(self, partition, timeout=None, cached=False):
        return 0, len(self.messages) + self.trailing_gap

    def assign(self, partitions):
        self.assigned.append(partitions)
        if partitions:
            # OFFSET_BEGINNING is a negative logical offset
            self._cursor = max(partitions[0].offset, 0)

    def poll(self, timeout=None):
        self.polls += 1
        if self._cursor >= len(self.messages):
            return None
        message = StubMessage(self.topic, self._cursor, self.messages[self._cursor])
        self._cursor += 1
        return message

    def close(self):
        pass
