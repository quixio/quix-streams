from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TimestampsCache:
    key: bytes
    cf_name: str
    timestamps: dict[bytes, Optional[int]] = field(default_factory=dict)


@dataclass
class CounterCache:
    key: bytes
    cf_name: str
    counter: Optional[int] = None
