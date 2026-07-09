"""
Under exactly-once the legacy-TTL migration / backfill records must be produced
through a dedicated NON-transactional producer, so that a per-chunk ``flush()``
means DURABLE before the local RocksDB write.

Under exactly-once the main changelog producer is transactional: ``flush()``
confirms delivery but records only become durable when the checkpoint transaction
commits (they abort otherwise). The migration paths write local state (stamps +
resume ledger / pending-cursor) immediately after producing+flushing each chunk;
if those records rode the transactional producer, a crash before commit would
leave local stamps ahead of an aborted, never-republished changelog record — the
changelog-first invariant violated. Routing migration records through a
non-transactional producer restores ``flush()==durable``.

These tests are mock-based (no broker): they assert (1) ``ChangelogProducer``
routes ``migration=True`` traffic to the migration producer and normal traffic to
the main producer, and (2) the recovery-completion path produces via the migration
producer with the per-chunk flush ordered BEFORE the local write.
"""

from datetime import timedelta
from unittest.mock import MagicMock

from quixstreams.internal_producer import InternalProducer
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value

DAY_MS = 86_400_000


class TestChangelogProducerRouting:
    def test_migration_true_routes_to_migration_producer(self):
        main = MagicMock(spec_set=InternalProducer)
        migration = MagicMock(spec_set=InternalProducer)
        cp = ChangelogProducer(
            changelog_name="cl",
            partition=0,
            producer=main,
            migration_producer=migration,
        )

        cp.produce(key=b"k", value=b"v", headers={"h": b"1"}, migration=True)
        migration.produce.assert_called_once()
        main.produce.assert_not_called()

        # Normal (non-migration) production stays on the main (transactional)
        # producer — the fix must not change normal changelog production.
        cp.produce(key=b"k2", value=b"v2")
        main.produce.assert_called_once()
        migration.produce.assert_called_once()  # unchanged

    def test_migration_flush_routes_to_migration_producer(self):
        main = MagicMock(spec_set=InternalProducer)
        migration = MagicMock(spec_set=InternalProducer)
        migration.flush.return_value = 0
        cp = ChangelogProducer(
            changelog_name="cl",
            partition=0,
            producer=main,
            migration_producer=migration,
        )
        cp.flush(migration=True)
        migration.flush.assert_called_once()
        main.flush.assert_not_called()

    def test_falls_back_to_main_when_no_migration_producer(self):
        # Non-exactly-once: no migration producer configured, so migration
        # records fall back to the main producer (already non-transactional).
        main = MagicMock(spec_set=InternalProducer)
        cp = ChangelogProducer(changelog_name="cl", partition=0, producer=main)
        cp.produce(key=b"k", value=b"v", migration=True)
        main.produce.assert_called_once()
        cp.flush(migration=True)
        main.flush.assert_called_once()


class TestCompletionUsesMigrationProducer:
    def test_completion_produces_via_migration_producer_flush_before_write(
        self, tmp_path
    ):
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        n_legacy = 3

        main = MagicMock(spec_set=InternalProducer)
        migration = MagicMock(spec_set=InternalProducer)
        order: list[str] = []

        def _flush(*_a, **_k):
            order.append("flush")
            return 0

        migration.flush.side_effect = _flush
        changelog = ChangelogProducer(
            changelog_name="cl",
            partition=0,
            producer=main,
            migration_producer=migration,
        )

        part = RocksDBStorePartition(
            (tmp_path / "dst").as_posix(),
            changelog_producer=changelog,
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
        )
        part._now_ms = lambda: now_ms  # noqa: E731

        # MIXED replay: 1 stamped survivor + n_legacy leftovers.
        msgs = [
            (b"pfx|s0", encode_ttl_value(stamp_expiry, b"stamped-0"), True),
        ]
        for i in range(n_legacy):
            msgs.append((f"pfx|l{i}".encode(), f"legacy-{i}".encode(), False))
        offset = 0
        for key, value, ttl_stamped in msgs:
            part.recover_from_changelog_message(
                key=key,
                value=value,
                cf_name="default",
                offset=offset,
                ttl_stamped=ttl_stamped,
            )
            offset += 1

        # Track local-write ordering only for the completion phase.
        original_write = part._write
        order.clear()

        def _tracked_write(batch):
            order.append("write")
            return original_write(batch)

        part._write = _tracked_write
        part.complete_recovery()

        # All migration records (n_legacy leftover stamps + 1 done-marker) went to
        # the NON-transactional migration producer; the main (transactional)
        # producer was not used for them.
        assert migration.produce.call_count == n_legacy + 1
        assert main.produce.call_count == 0

        # Per-chunk flush precedes the local write (changelog-first): the leftover
        # chunk (flush, write) then the done-marker (flush, write).
        assert order == ["flush", "write", "flush", "write"]
        part.close()
