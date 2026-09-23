"""Mapping from ``Packet.type`` string to the matching MA payload class.

The outer envelope ``ma.streaming.open_data.v1.Packet`` carries the actual
payload as ``content`` bytes. The deserializer picks the right protobuf
message class to parse those bytes based on the ``type`` discriminator
string.

Names here mirror what is observed on the wire (e.g. ``"RawCANData"``,
``"StreamStarted"``) — they are the message-name suffix without the
trailing ``Packet`` token. ``SystemStatus`` is the lone exception: the
matching class is named ``SystemStatusMessage``, not ``SystemStatusPacket``.
"""

from __future__ import annotations

from google.protobuf.message import Message

from . import open_data_pb2 as pb

# Maps the value of Packet.type to the protobuf class whose schema
# describes Packet.content for that type.
CONTENT_TYPES: dict[str, type[Message]] = {
    "NewSession": pb.NewSessionPacket,
    "EndOfSession": pb.EndOfSessionPacket,
    "SessionInfo": pb.SessionInfoPacket,
    "StreamStarted": pb.StreamStartedPacket,
    "StreamStopped": pb.StreamStoppedPacket,
    "Configuration": pb.ConfigurationPacket,
    "PeriodicData": pb.PeriodicDataPacket,
    "RowData": pb.RowDataPacket,
    "SynchroData": pb.SynchroDataPacket,
    "Event": pb.EventPacket,
    "Marker": pb.MarkerPacket,
    "Error": pb.ErrorPacket,
    "Metadata": pb.MetadataPacket,
    "RawCANData": pb.RawCANDataPacket,
    "AxisData": pb.AxisDataPacket,
    "MapData": pb.MapDataPacket,
    "DataFormatConfiguration": pb.DataFormatConfigurationPacket,
    "DataFormatDefinition": pb.DataFormatDefinitionPacket,
    "CoverageCursorInfo": pb.CoverageCursorInfoPacket,
    "SystemStatus": pb.SystemStatusMessage,
}
