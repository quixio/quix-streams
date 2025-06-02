from dataclasses import dataclass, field
from typing import Any


@dataclass
class Cache:
    key: bytes
    cf_name: str
    values: dict[bytes, Any] = field(default_factory=dict)
