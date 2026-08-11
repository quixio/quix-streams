import dataclasses
import threading
from typing import Dict, Mapping, Optional

import rocksdict
from rocksdict import DBCompressionType

from quixstreams.state.serialization import DumpsFunc, LoadsFunc
from quixstreams.utils.json import dumps, loads

from .types import CompressionType, RocksDBOptionsType

__all__ = ("RocksDBOptions",)

# One block cache per configured capacity, shared by every store partition in
# the process.
#
# RocksDB's own guidance is a single block cache shared across databases, and
# without this ``block_cache_size`` is a PER-PARTITION multiplier: a 32-partition
# 3-store application would reserve 96 x the configured size. Keying on capacity
# rather than on the options instance is deliberate — a partition constructed
# without explicit options builds its own ``RocksDBOptions`` (see
# ``RocksDBStorePartition.__init__``), so per-instance memoization would not
# share for the default case, which is most deployments.
#
# A cache is a lazily-filled ceiling, not an allocation, so a small deployment
# never realizes the full capacity.
_BLOCK_CACHES: Dict[int, rocksdict.Cache] = {}
_BLOCK_CACHES_LOCK = threading.Lock()


def _shared_block_cache(capacity: int) -> rocksdict.Cache:
    """
    Get the process-wide block cache for ``capacity``, creating it on first use.

    Locked because partitions are opened concurrently during a rebalance, and two
    threads racing here would otherwise each build a cache of this size.
    """
    with _BLOCK_CACHES_LOCK:
        cache = _BLOCK_CACHES.get(capacity)
        if cache is None:
            cache = rocksdict.Cache(capacity)
            _BLOCK_CACHES[capacity] = cache
        return cache


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
    :param on_corrupted_recreate: when True, the corrupted DB will be destroyed
        if the `use_changelog_topics=True` is also set on the Application.
        If this option is True, but `use_changelog_topics=False`,
        the DB won't be destroyed.
        Note: risk of data loss! Make sure that the changelog topics are up-to-date before disabling it in production.
        Default - `True`.
    :param block_cache_size: size of the RocksDB block cache, in bytes.
        This is an **aggregate ceiling for the whole process**: one cache of this
        size is shared by every store partition, following RocksDB's own guidance
        that a single block cache be shared across databases. It is not a
        per-partition budget, so a 32-partition application does not reserve 32
        times this value.
        A cache is filled lazily, so a small deployment never realizes the full
        capacity; size it against total available memory rather than per store.
        Note that index and bloom-filter blocks are held outside this budget
        (``cache_index_and_filter_blocks`` is not enabled), so real usage is this
        value plus roughly ``bloom_filter_bits_per_key / 8`` bytes per key.
        Default - ``1 GiB``.
    :param max_evictions_per_flush: cap on TTL-driven evictions performed
        during a single ``flush()`` for stores with TTL enabled. Larger values
        increase per-flush latency but let the sweep keep up with higher
        steady-state expiration rates. Only meaningful for TTL-enabled
        stores; ignored otherwise.
        Default - ``10_000``.

    Please see `rocksdict.Options` for a complete description of other options.
    """

    write_buffer_size: int = 64 * 1024 * 1024
    target_file_size_base: int = 64 * 1024 * 1024
    max_write_buffer_number: int = 3
    block_cache_size: int = 1024 * 1024 * 1024
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
    use_fsync: bool = True
    on_corrupted_recreate: bool = True
    max_evictions_per_flush: int = 10_000

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
        opts.set_use_fsync(self.use_fsync)
        if self.wal_dir is not None:
            opts.set_wal_dir(self.wal_dir)
        if self.db_log_dir is not None:
            opts.set_db_log_dir(self.db_log_dir)

        table_factory_options = rocksdict.BlockBasedOptions()
        table_factory_options.set_block_cache(
            _shared_block_cache(self.block_cache_size)
        )
        table_factory_options.set_bloom_filter(
            self.bloom_filter_bits_per_key, block_based=True
        )
        opts.set_block_based_table_factory(table_factory_options)
        compression_type = COMPRESSION_TYPES[self.compression_type]
        opts.set_compression_type(compression_type)
        opts.set_max_total_wal_size(size=self.max_total_wal_size)
        return opts
