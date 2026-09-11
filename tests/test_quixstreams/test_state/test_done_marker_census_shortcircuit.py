"""
#9 (review batch 3): during changelog replay, stop writing pending-CF census
entries once the migration done-marker is known — either already local at open
(warm restart of a fully-migrated store) or latched the moment the (flag-last)
marker record replays. Classification for stores WITHOUT the marker is
byte-identical to HEAD. Safe because ``complete_recovery`` already discards /
no-ops the census when the marker is seen, so the skipped entries would have been
dropped anyway.
"""

from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import STATE_FORMAT_VERSION
from quixstreams.state.serialization import int_to_bytes

BASE_TS = 1_000_000_000_000
MARKER_VALUE = int_to_bytes(STATE_FORMAT_VERSION)


def _producer_mock():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="cl")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _rocksdb(tmp_path, name="db"):
    return RocksDBStorePartition(
        (tmp_path / name).as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=_producer_mock(),
    )


def _pending_rocksdb(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return {bytes(k) for k in cf.keys()}


def _pending_memory(partition):
    return set(partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {}).keys())


def _replay(partition, key, value, ttl_stamped, cf_name="default", offset=0):
    partition.recover_from_changelog_message(
        key=key, value=value, cf_name=cf_name, offset=offset, ttl_stamped=ttl_stamped
    )


class TestRocksDBCensusShortCircuit:
    def test_warm_open_with_marker_skips_census(self, tmp_path):
        # Flip + produce the done-marker, then reopen: the marker is local at open,
        # so replaying a legacy record censuses NOTHING.
        part = _rocksdb(tmp_path)
        with part.begin() as tx:
            tx.set(
                key="seed",
                value="s",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )
        part._produce_migration_done_marker()
        part.close()

        reopened = _rocksdb(tmp_path)
        assert reopened._has_local_migration_done_marker()
        _replay(reopened, b"pfx|legacy", b"legacy-val", ttl_stamped=False, offset=10)
        assert (
            _pending_rocksdb(reopened) == set()
        ), "a warm open with the local done-marker must skip censusing entirely"
        reopened.close()

    def test_cold_rebuild_latches_on_marker_record(self, tmp_path):
        # Fresh store (no marker at open). A legacy record BEFORE the marker is
        # censused; the (flag-last) marker replay latches; a legacy record AFTER it
        # is NOT censused.
        part = _rocksdb(tmp_path)
        _replay(part, b"pfx|before", b"v", ttl_stamped=False, offset=0)
        _replay(
            part,
            TTL_MIGRATION_DONE_KEY,
            MARKER_VALUE,
            ttl_stamped=False,
            cf_name=TTL_SYSTEM_CF_NAME,
            offset=1,
        )
        _replay(part, b"pfx|after", b"v", ttl_stamped=False, offset=2)

        pending = _pending_rocksdb(part)
        assert b"pfx|before" in pending, "pre-marker legacy record is still censused"
        assert (
            b"pfx|after" not in pending
        ), "a legacy record replayed AFTER the marker must not be censused"
        part.close()

    def test_no_marker_censuses_byte_identically(self, tmp_path):
        # A store WITHOUT any marker censuses every legacy record (unchanged).
        part = _rocksdb(tmp_path)
        for i in range(4):
            _replay(part, f"pfx|l{i}".encode(), b"v", ttl_stamped=False, offset=i)
        assert _pending_rocksdb(part) == {f"pfx|l{i}".encode() for i in range(4)}
        part.close()


class TestMemoryCensusShortCircuit:
    def test_cold_rebuild_latches_on_marker_record(self):
        part = MemoryStorePartition(changelog_producer=_producer_mock())
        _replay(part, b"pfx|before", b"v", ttl_stamped=False, offset=0)
        _replay(
            part,
            TTL_MIGRATION_DONE_KEY,
            MARKER_VALUE,
            ttl_stamped=False,
            cf_name=TTL_SYSTEM_CF_NAME,
            offset=1,
        )
        _replay(part, b"pfx|after", b"v", ttl_stamped=False, offset=2)

        pending = _pending_memory(part)
        assert b"pfx|before" in pending
        assert (
            b"pfx|after" not in pending
        ), "memory: a legacy record after the marker must not be censused"
        part.close()

    def test_no_marker_censuses_byte_identically(self):
        part = MemoryStorePartition(changelog_producer=_producer_mock())
        for i in range(4):
            _replay(part, f"pfx|l{i}".encode(), b"v", ttl_stamped=False, offset=i)
        assert _pending_memory(part) == {f"pfx|l{i}".encode() for i in range(4)}
        part.close()
