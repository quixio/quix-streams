from typing import Tuple, List, Any

from quixstreams.models import HeaderValue

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
        headers: List[Tuple[str, HeaderValue]],
        offset: int,
    ):
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.offset = offset
        self.headers = headers  # TODO: Headers are potentially mutable
