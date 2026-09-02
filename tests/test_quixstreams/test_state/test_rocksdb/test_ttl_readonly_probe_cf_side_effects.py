"""
Red-first test for PR #1134 round-2 review Finding #4: read-only TTL
migration-status predicates MATERIALIZE column families as a side effect,
turning a pure query into a store mutation.

``RocksDBStorePartition.has_incomplete_ttl_migration`` -- documented as a
cheap, side-effect-free probe consulted by
``RecoveryPartition.needs_recovery_check`` -- calls
``_has_local_migration_done_marker`` / ``_backfill_pending_has_any`` /
``_live_backfill_ledger_has_any``, every one of which routes through
``get_or_create_column_family(...)``. Probing a TTL-enabled store that has no
incomplete migration (no done-marker, no pending census, no live-backfill
ledger -- e.g. a store that engaged TTL natively and never ran a legacy
migration) therefore CREATES the ``__ttl_system__`` /
``__ttl_backfill_pending__`` / ``__ttl_backfill_stamped__`` column families
purely by asking "is there an incomplete migration?".

This is not just wasted I/O: CF *existence* is itself a classification
signal elsewhere. ``_has_warm_ttl_artifacts`` VETOes the warm-preview-store
detection the moment ``TTL_BACKFILL_PENDING_CF_NAME`` /
``TTL_BACKFILL_STAMPED_CF_NAME`` / ``TTL_SYSTEM_CF_NAME`` are present in
``list_column_families()`` (treating their presence as proof of a
current-build crashed migration). A read-only probe that manufactures those
CFs on a store that never had any migration activity can poison that
classification on a later open.

Spec: no spec section demands lazy CF probing explicitly, but
``dev-planning/state-ttl-v3240-auto-adopt/spec.md`` §5.0's warm/cold
classification is built on CF-existence as a positive signal, which this
finding shows is not read-only-safe.
"""

from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.metadata import (
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_STAMPED_CF_NAME,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition

_MIGRATION_BOOKKEEPING_CFS = {
    TTL_SYSTEM_CF_NAME,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_STAMPED_CF_NAME,
}


def _make_producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _rocksdb_partition(tmp_path, name="db", changelog_producer=None):
    path = (tmp_path / name).as_posix()
    opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(
        path, options=opts, changelog_producer=changelog_producer
    )


class TestReadOnlyProbeMaterializesBookkeepingCFs:
    """Finding #4: ``has_incomplete_ttl_migration`` must not create column
    families as a side effect of a read-only query.

    RED on HEAD: calling ``has_incomplete_ttl_migration`` on a TTL-enabled
    store with no incomplete migration (no done-marker, no pending census, no
    live-backfill ledger) materializes ``__ttl_backfill_pending__`` and
    ``__ttl_backfill_stamped__`` (``__ttl_system__`` is already created by the
    open-time ``_cleanup_completed_backfill_bookkeeping`` call -- see the
    companion test below).
    """

    def test_probe_creates_no_new_column_families(self, tmp_path):
        producer = _make_producer()

        # Engage TTL natively (no legacy migration ever ran): persist the
        # __ttl_enabled__ flag directly and close, so the NEXT open loads a
        # TTL-enabled store with zero migration bookkeeping ever touched.
        seed = _rocksdb_partition(
            tmp_path, name="native_ttl", changelog_producer=producer
        )
        seed._stamp_flip_metadata()
        seed.close()

        partition = _rocksdb_partition(
            tmp_path, name="native_ttl", changelog_producer=producer
        )
        assert (
            partition.uses_ttl_stamps is True
        ), "setup sanity: the store must load already TTL-enabled"

        cfs_before = set(partition.list_column_families())

        result = partition.has_incomplete_ttl_migration()

        cfs_after = set(partition.list_column_families())

        assert result is False, (
            "setup sanity: a store with no done-marker, no pending census "
            "and no live-backfill ledger has no incomplete migration"
        )
        new_cfs = cfs_after - cfs_before
        assert not new_cfs, (
            f"has_incomplete_ttl_migration() is documented as a read-only "
            f"probe but materialized new column family(ies) {new_cfs} as a "
            f"side effect -- CF existence is itself a classification signal "
            f"elsewhere (_has_warm_ttl_artifacts), so a read-only query must "
            f"not create them"
        )

        partition.close()


class TestOpeningPlainStoreCreatesNoBookkeepingCFs:
    """Regression guard (not the red defect itself): simply OPENING a
    never-TTL-touched store must not create any TTL migration-bookkeeping
    CFs. Covers the ``_cleanup_completed_backfill_bookkeeping`` __init__
    path, which is gated on ``self.uses_ttl_stamps`` and so does not fire for
    a genuinely untouched store -- this passes today and must keep passing
    after the Finding #4 fix.
    """

    def test_opening_never_touched_store_creates_no_bookkeeping_cfs(self, tmp_path):
        producer = _make_producer()
        partition = _rocksdb_partition(
            tmp_path, name="never_touched", changelog_producer=producer
        )

        assert partition.uses_ttl_stamps is False

        cfs = set(partition.list_column_families())
        found = cfs & _MIGRATION_BOOKKEEPING_CFS
        assert not found, (
            f"opening a plain, never-TTL-touched store must not create TTL "
            f"migration-bookkeeping column families; found {found}"
        )

        partition.close()
