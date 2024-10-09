import gzip
from typing import List, Iterator

import orjson

from quixstreams.sinks.base.item import SinkItem
from .base import FileFormatter

__all__ = ("JSONFormatter",)


class JSONFormatter(FileFormatter):
    """
    Bypass formatter to serialize
    """

    def __init__(
        self,
        separator: bytes = b"\n",
        gzip: bool = False,
    ):
        self._gzip = gzip
        self._separator = separator

    @property
    def file_extension(self) -> str:
        extension = ".json"
        if self._gzip:
            extension += ".gz"
        return extension

    def write_batch_values(self, filepath: str, items: List[SinkItem]) -> None:
        if self._gzip:
            f = gzip.open(filepath, "wb")
        else:
            f = open(filepath, "wb")

        try:
            f.writelines(self._iterate_items(items))
        finally:
            f.close()

    def _iterate_items(self, items: List[SinkItem]) -> Iterator[bytes]:
        for item in items:
            yield orjson.dumps(
                {
                    "key": item.key,
                    "value": item.value,
                    "timestamp": item.timestamp,
                }
            )
            yield self._separator
