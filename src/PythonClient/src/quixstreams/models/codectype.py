from enum import Enum


class CodecType(Enum):
    """
    Codecs available for serialization and deserialization of streams.
    """

    Json = 0
    CompactJsonForBetterPerformance = 1
    Protobuf = 2
