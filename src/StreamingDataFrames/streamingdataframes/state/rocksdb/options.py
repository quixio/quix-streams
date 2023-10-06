import dataclasses
from typing import Optional, Mapping, Literal

import rocksdict
from rocksdict import DBCompressionType

from .types import RocksDBOptionsProto

__all__ = ("RocksDBOptions",)

CompressionType = Literal["none", "snappy", "zlib", "bz2", "lz4", "lz4hc", "zstd"]

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
class RocksDBOptions(RocksDBOptionsProto):
    """
    Common RocksDB database options.

    Please see `rocksdict.Options` for a complete description of each option.

    To provide extra options that are not presented in this class, feel free
    to override it and specify the additional values.
    """

    write_buffer_size: int = 64 * 1024 * 1024  # 64MB
    target_file_size_base: int = 64 * 1024 * 1024  # 64MB
    max_write_buffer_number: int = 3
    block_cache_size: int = 128 * 1024 * 1024  # 128MB
    enable_pipelined_write: bool = False
    compression_type: CompressionType = "lz4"
    wal_dir: Optional[str] = None
    db_log_dir: Optional[str] = None

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
        opts.set_block_based_table_factory(table_factory_options)

        compression_type = COMPRESSION_TYPES[self.compression_type]
        opts.set_compression_type(compression_type)
        return opts
