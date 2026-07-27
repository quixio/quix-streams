"""
Regression test for finding M3 (batch4 re-review): the backfill changelog
flush masks a per-partition delivery error.

``RocksDBStorePartition._flush_backfill_changelog`` returns success as soon as
the SHARED producer's global ``flush()`` return value hits 0 (line ~2012-2013:
``if remaining == 0: return``) -- BEFORE checking THIS partition's own
``_backfill_produced - _backfill_acked`` outstanding count. ``Producer.flush()``
can report a 0 global backlog even though one of THIS partition's produced
records failed delivery (the delivery callback fired with ``err is not None``,
which never increments ``_backfill_acked``): a failed-but-drained record still
leaves the broker's send queue, so ``flush()`` sees an empty queue while this
partition's own record was never actually delivered.

The changelog-first invariant (loudly documented on this same method) is that
the local commit must never proceed while an unacked/failed record for this
partition exists -- so this must raise ``ChangelogFlushError``, not return
cleanly.
"""

import pytest

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
        THIS partition produced one record that failed delivery."""

        def flush(self, timeout=None, migration=False):
            return 0

    # Simulate: this partition produced 1 backfill record, and its delivery
    # callback fired with an error (never acked).
    part._backfill_produced = 1
    part._backfill_acked = 0

    with pytest.raises(ChangelogFlushError):
        part._flush_backfill_changelog(_FlushZeroProducer())

    part.close()
