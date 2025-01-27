from types import ModuleType
from typing import Any, Optional

pd: Optional[ModuleType] = None
try:
    import pandas as pd
except ImportError:
    pass

pl: Optional[ModuleType] = None
try:
    import polars as pl
except ImportError:
    pass

from quixstreams.models import HeadersTuples

from ..base import BaseSink


class LocalSink(BaseSink):
    """
    A sink that accumulates data into local memory.

    The accumulated data can be accessed as:
    - A Python list using the `list` property
    - A Pandas DataFrame using the `pandas` property (requires Pandas)
    - A Polars DataFrame using the `polars` property (requires Polars)

    Metadata Behavior:
        When initialized with metadata=True, each record will be
        enriched with the following metadata fields as a prefix
        to the value dictionary:
        - _key: The message key
        - _timestamp: Message timestamp
        - _headers: String representation of message headers
        - _topic: Source topic name
        - _partition: Source partition number
        - _offset: Message offset in partition

        When metadata=False (default), only the message value is stored.
    """

    def __init__(self, metadata: bool = False):
        super().__init__()
        self.list: list[Any] = []
        self._metadata = metadata
        self._pandas = None
        self._polars = None

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        if not isinstance(value, dict):
            value = {"value": value}

        if self._metadata:
            value = {
                "_key": key,
                "_timestamp": timestamp,
                "_headers": str(headers),
                "_topic": topic,
                "_partition": partition,
                "_offset": offset,
                **value,
            }

        self.list.append(value)

    def flush(self, *args, **kwargs):
        return

    @property
    def pandas(self):
        if self._pandas is None:
            if pd is None:
                raise ModuleNotFoundError(
                    "Pandas is not installed. "
                    "Run `pip install quixstreams[pandas]` to install it."
                )
            self._pandas = pd.DataFrame(self.list)
        return self._pandas

    @property
    def polars(self):
        if self._polars is None:
            if pl is None:
                raise ModuleNotFoundError(
                    "Polars is not installed. "
                    "Run `pip install quixstreams[polars]` to install it."
                )
            self._polars = pl.DataFrame(self.list)
        return self._polars
