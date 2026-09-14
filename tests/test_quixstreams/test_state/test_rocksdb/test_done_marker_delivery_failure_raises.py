"""
Safety direction of the migration done-marker's changelog-first ordering: a
GENUINE done-marker delivery failure must still raise **through** the marker
phase, BEFORE the local ``__ttl_system__`` write.

Nothing else in the suite pinned this. The two delivery-error tests call the
confirm helpers directly (never through the marker),
``test_done_marker_delivery_error.py`` makes ``produce()``/``flush()`` raise
*before* the counter comparison is ever reached, and
``test_ttl_done_marker_produce_order.py`` /
``test_v3240_adoption_rollback_safety.py`` use ``MagicMock`` producers whose
``flush()`` returns a non-int, which short-circuits to ``INDETERMINATE`` and
skips the counter check entirely. So the mutation "stop distinguishing a failed
delivery from a successful one" (or "stop counting the marker's produce") went
undetected: the phase would read balanced counters, the confirm would return
``CONFIRMED``, and the local marker would be written with the marker record
never actually delivered.

The second assertion in each test is the load-bearing one: the local marker must
be ABSENT, which proves the raise happened through the marker phase and before
``self._write(batch)``.
"""

from typing import Optional

import pytest

from quixstreams.state.exceptions import ChangelogFlushError
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition


class _FailingDeliveryProducer:
    """A ``ChangelogProducer``-shaped double that records each ``produce()``'s
    ``on_delivery`` and, on ``flush()``, fires every recorded callback with a
    delivery ERROR and then returns a real ``0``.

    ``0`` is deliberate: a failed-but-drained record does leave the send queue,
    so a healthy shared producer legitimately reports an empty global queue
    while this partition's own record was never delivered. That is the
    ``DRAINED_UNACKED`` signature, and it must be reachable WITHOUT the stub
    raising anything itself — the raise under test has to come from the
    partition's own confirm, not from the producer.
    """

    changelog_name = "cl"
    partition = 0

    def __init__(self) -> None:
        self._pending: list = []
        self.produced_keys: list[bytes] = []

    def produce(
        self,
        key: bytes,
        value: Optional[bytes] = None,
        headers=None,
        migration: bool = False,
        on_delivery=None,
    ) -> None:
        self.produced_keys.append(key)
        self._pending.append(on_delivery)

    def flush(self, timeout: Optional[float] = None, migration: bool = False) -> int:
        pending, self._pending = self._pending, []
        for callback in pending:
            if callback is not None:
                callback(RuntimeError("simulated delivery failure"), None)
        return 0


def test_failed_done_marker_delivery_raises_before_the_local_write(tmp_path):
    producer = _FailingDeliveryProducer()
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=producer,
    )
    try:
        with pytest.raises(ChangelogFlushError):
            part._produce_migration_done_marker()

        assert producer.produced_keys, "the marker must have been produced"
        # LOAD-BEARING: the raise happened through the marker phase and before
        # ``self._write(batch)``, so changelog-first held — the local store did
        # not record "migration done" ahead of the changelog.
        assert part._has_local_migration_done_marker() is False
    finally:
        # Release the RocksDB directory lock even on an assertion failure.
        part.close()
