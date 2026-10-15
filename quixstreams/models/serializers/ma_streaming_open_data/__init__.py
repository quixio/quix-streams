"""Deprecated alias of :mod:`quixstreams.models.serializers.fia_open_data`."""

import warnings

from ..fia_open_data import (
    FIAOpenDataDeserializer,
    __protocol_version__,
    open_data_pb2,
)

warnings.warn(
    "quixstreams.models.serializers.ma_streaming_open_data is deprecated and "
    "will be removed in a future major release; use FIAOpenDataDeserializer "
    "from quixstreams.models.serializers.fia_open_data instead.",
    DeprecationWarning,
    stacklevel=2,
)

MAStreamingDeserializer = FIAOpenDataDeserializer

__all__ = ("MAStreamingDeserializer", "__protocol_version__", "open_data_pb2")
