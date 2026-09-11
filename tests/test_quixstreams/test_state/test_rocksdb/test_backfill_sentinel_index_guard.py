"""
Regression test for finding L1 (batch4 re-review): backfill writes a
SENTINEL_NEVER index entry.

``RocksDBStorePartition.backfill_legacy_records`` unconditionally puts a
``__ttl_index__`` entry for every re-stamped key (``batch.put(encode_index_key(
expires_at_ms, key), b"", index_handle)`` ~line 2640), unlike other stamping
paths which guard ``if expires_at_ms != SENTINEL_NEVER``. When
``expires_at_ms == SENTINEL_NEVER`` (reachable via the resume /
``_resume_interrupted_live_backfill`` fallback, and via the additive-sum
clamp), this creates a permanent, never-swept ``__ttl_index__`` entry per key
-- a resource leak, since a SENTINEL_NEVER-stamped record never expires and so
should never be indexed for eviction at all.
"""

from datetime import timedelta

from quixstreams.state.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import SENTINEL_NEVER


def _index_entry_count(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    return sum(1 for _ in cf.keys())


def test_backfill_sentinel_expiry_creates_no_index_entry(tmp_path):
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(
            open_max_retries=0,
            open_retry_backoff=3.0,
            legacy_records_ttl=timedelta(days=7),
            legacy_backfill_chunk_size=10,
        ),
        changelog_producer=None,
    )
    default_cf = part.get_or_create_column_family("default")
    default_cf[b"pfx|legacy1"] = b'"L1"'
    default_cf[b"pfx|legacy2"] = b'"L2"'

    restamped = part.backfill_legacy_records(
        expires_at_ms=SENTINEL_NEVER,
        changelog_producer=None,
        processed_offsets=None,
        staged_default_keys=set(),
        chunk_size=10,
    )
    assert restamped == 2

    count = _index_entry_count(part)
    assert count == 0, (
        "L1: a SENTINEL_NEVER (never-expires) backfill expiry must not create "
        f"a permanent, never-swept __ttl_index__ entry, got {count} entries"
    )
    part.close()
