from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from typing_extensions import runtime_checkable

from quixstreams.state.serialization import DumpsFunc, LoadsFunc
from quixstreams.utils.json import dumps as json_dumps
from quixstreams.utils.json import loads as json_loads


@runtime_checkable
class SlateDBOptionsType(Protocol):
    dumps: DumpsFunc
    loads: LoadsFunc
    open_max_retries: int
    open_retry_backoff: float


@dataclass
class SlateDBOptions:
    dumps: DumpsFunc = json_dumps
    loads: LoadsFunc = json_loads
    open_max_retries: int = 5
    open_retry_backoff: float = 0.2
    # If True, attempt to destroy and recreate a corrupted DB path on open
    on_corrupted_recreate: bool = False
