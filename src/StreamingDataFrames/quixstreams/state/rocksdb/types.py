from typing import Protocol, Optional, Literal

import rocksdict

from quixstreams.state.types import DumpsFunc, LoadsFunc

CompressionType = Literal["none", "snappy", "zlib", "bz2", "lz4", "lz4hc", "zstd"]


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

    def to_options(self) -> rocksdict.Options:
        ...
