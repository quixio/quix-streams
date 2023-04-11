from enum import Enum


class CodecType(Enum):
    """
    Codecs available for serialization.
    """

    Json = 0
    HumanReadableSemiJsonWithBetterPerformance = 1
    Protobuf = 2
