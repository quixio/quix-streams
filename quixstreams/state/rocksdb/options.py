import dataclasses
from typing import Mapping, Optional

import rocksdict
from rocksdict import DBCompressionType

from quixstreams.state.serialization import DumpsFunc, LoadsFunc
from quixstreams.utils.json import dumps, loads

from .types import CompressionType, RocksDBOptionsType

__all__ = ("RocksDBOptions",)

COMPRESSION_TYPES: Mapping[CompressionType, DBCompressionType] = {
    "none": DBCompressionType.none(),
    "snappy": DBCompressionType.snappy(),
    "zlib": DBCompressionType.zlib(),
    "bz2": DBCompressionType.bz2(),
    "lz4": DBCompressionType.lz4(),
    "lz4hc": DBCompressionType.lz4hc(),
    "zstd": DBCompressionType.zstd(),
}


@dataclasses.dataclass(frozen=True)
class RocksDBOptions(RocksDBOptionsType):
    """
    RocksDB database options.

    :param dumps: function to dump data to JSON
    :param loads: function to load data from JSON
    :param open_max_retries: number of times to retry opening the database
            if it's locked by another process. To disable retrying, pass 0
    :param open_retry_backoff: number of seconds to wait between each retry.

    Please see `rocksdict.Options` for a complete description of other options.
    """

    write_buffer_size: int = 64 * 1024 * 1024
    target_file_size_base: int = 64 * 1024 * 1024
    max_write_buffer_number: int = 3
    block_cache_size: int = 128 * 1024 * 1024
    bloom_filter_bits_per_key: int = 10
    enable_pipelined_write: bool = False
    compression_type: CompressionType = "lz4"
    wal_dir: Optional[str] = None
    max_total_wal_size: int = 128 * 1024 * 1024
    db_log_dir: Optional[str] = None
    dumps: DumpsFunc = dumps
    loads: LoadsFunc = loads
    open_max_retries: int = 10
    open_retry_backoff: float = 3.0

    def to_options(self) -> rocksdict.Options:
        """
        Convert parameters to `rocksdict.Options`
        :return: instance of `rocksdict.Options`
        """
        opts = rocksdict.Options(raw_mode=True)
        opts.create_if_missing(True)
        opts.set_write_buffer_size(self.write_buffer_size)
        opts.set_target_file_size_base(self.target_file_size_base)
        opts.set_max_write_buffer_number(self.max_write_buffer_number)
        opts.set_enable_pipelined_write(self.enable_pipelined_write)
        if self.wal_dir is not None:
            opts.set_wal_dir(self.wal_dir)
        if self.db_log_dir is not None:
            opts.set_db_log_dir(self.db_log_dir)

        table_factory_options = rocksdict.BlockBasedOptions()
        table_factory_options.set_block_cache(rocksdict.Cache(self.block_cache_size))
        table_factory_options.set_bloom_filter(
            self.bloom_filter_bits_per_key, block_based=True
        )
        opts.set_block_based_table_factory(table_factory_options)
        compression_type = COMPRESSION_TYPES[self.compression_type]
        opts.set_compression_type(compression_type)
        opts.set_max_total_wal_size(size=self.max_total_wal_size)
        return opts
