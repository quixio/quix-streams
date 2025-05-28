from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Cache:
    key: bytes
    cf_name: str
    values: dict[bytes, Any] = field(default_factory=dict)


@dataclass
class CounterCache:
    key: bytes
    cf_name: str
    counter: Optional[int] = None
