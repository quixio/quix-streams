"""
Regression test for finding M3 (batch4 re-review): the backfill changelog
flush must not mask a per-phase delivery error.

``RocksDBStorePartition._flush_backfill_changelog`` must not return success
merely because the SHARED producer's global ``flush()`` return value hit 0 --
it also has to check the produce phase's own ``produced - acked`` outstanding
count. ``Producer.flush()`` can report a 0 global backlog even though one of
this phase's produced records failed delivery (the delivery callback fired with
``err is not None``, so ``MigrationDeliveryPhase.on_delivery`` never counted an
ack): a failed-but-drained record still leaves the broker's send queue, so
``flush()`` sees an empty queue while the phase's own record was never actually
delivered.

The changelog-first invariant (loudly documented on that same method) is that
the local commit must never proceed while an unacked/failed record of the
current phase exists -- so this must raise ``ChangelogFlushError``, not return
cleanly.
"""

import pytest

from quixstreams.state.base.migration_flush import MigrationDeliveryPhase
from quixstreams.state.exceptions import ChangelogFlushError
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition


def test_flush_raises_when_partition_has_unacked_failed_record(tmp_path):
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=None,
    )

    class _FlushZeroProducer:
        """A stub whose ``flush()`` reports a 0 GLOBAL backlog (e.g. a sibling
        partition's record already drained the shared queue), even though
        THIS phase produced one record that failed delivery."""

        def flush(self, timeout=None, migration=False):
            return 0

    try:
        # Simulate: this phase produced 1 backfill record, and its delivery
        # callback fired with an error, so it was never acked (a failed
        # delivery is only ever *not counted* -- ``on_delivery`` is never
        # invoked with ``err is None`` for it).
        phase = MigrationDeliveryPhase()
        phase.record_produced()
        assert phase.counters() == (1, 0)

        with pytest.raises(ChangelogFlushError):
            part._flush_backfill_changelog(_FlushZeroProducer(), phase)
    finally:
        # Release the RocksDB directory lock even on an assertion failure, so a
        # red run does not block ``tmp_path`` cleanup for the rest of the
        # session (Windows).
        part.close()
