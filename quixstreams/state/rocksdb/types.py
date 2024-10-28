from typing import Literal, Optional, Protocol

import rocksdict
from typing_extensions import runtime_checkable

from quixstreams.state.serialization import DumpsFunc, LoadsFunc

CompressionType = Literal["none", "snappy", "zlib", "bz2", "lz4", "lz4hc", "zstd"]


@runtime_checkable
class RocksDBOptionsType(Protocol):
    write_buffer_size: int
    target_file_size_base: int
    max_write_buffer_number: int
    block_cache_size: int
    enable_pipelined_write: bool
    compression_type: CompressionType
    wal_dir: Optional[str]
    db_log_dir: Optional[str]
    dumps: DumpsFunc
    loads: LoadsFunc
    open_max_retries: int
    open_retry_backoff: float

    def to_options(self) -> rocksdict.Options: ...
