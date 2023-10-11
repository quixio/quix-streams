from typing import Callable, Any, Protocol

import rocksdict

DumpsFunc = Callable[[Any], bytes]
LoadsFunc = Callable[[bytes], Any]


class RocksDBOptionsProto(Protocol):
    def to_options(self) -> rocksdict.Options:
        ...
