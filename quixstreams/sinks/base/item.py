from typing import Any, Optional

from quixstreams.models import HeadersTuples

__all__ = ("SinkItem",)


class SinkItem:
    __slots__ = (
        "value",
        "key",
        "timestamp",
        "offset",
        "headers",
    )

    def __init__(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        offset: Optional[int],
    ):
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.offset = offset
        self.headers = headers  # TODO: Headers are potentially mutable
