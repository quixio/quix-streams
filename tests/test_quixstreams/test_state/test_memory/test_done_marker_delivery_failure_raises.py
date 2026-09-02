"""
Memory twin of
``test_rocksdb/test_done_marker_delivery_failure_raises.py``: a GENUINE
done-marker delivery failure must still raise **through** the marker phase,
BEFORE the marker is recorded in the in-RAM ``__ttl_system__`` dict.

Same gap, same reason (see the RocksDB module docstring): every other memory
done-marker test either raises inside ``produce()``/``flush()`` before the
counter comparison, or uses a double whose ``flush()`` returns a non-int and
short-circuits to ``INDETERMINATE``. The local ``__ttl_migration_done__`` write
is durably meaningless on a non-persistent backend — memory's sole durable
artifact is the changelog — but the ordering must still hold, because the
in-RAM marker is what stops this instance from re-entering completion.
"""

from typing import Optional

import pytest

from quixstreams.state.exceptions import ChangelogFlushError
from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_MIGRATION_DONE_KEY, TTL_SYSTEM_CF_NAME


class _FailingDeliveryProducer:
    """A ``ChangelogProducer``-shaped double that records each ``produce()``'s
    ``on_delivery`` and, on ``flush()``, fires every recorded callback with a
    delivery ERROR and then returns a real ``0`` (a failed-but-drained record
    does leave the send queue — the ``DRAINED_UNACKED`` signature). The stub
    itself raises nothing: the raise under test must come from the partition's
    own confirm."""

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


def test_failed_done_marker_delivery_raises_before_the_local_record():
    producer = _FailingDeliveryProducer()
    partition = MemoryStorePartition(changelog_producer=producer)

    with pytest.raises(ChangelogFlushError):
        partition._produce_migration_done_marker()

    assert producer.produced_keys, "the marker must have been produced"
    # LOAD-BEARING: the raise happened through the marker phase and before the
    # in-RAM ``__ttl_system__`` write, so changelog-first held.
    assert TTL_MIGRATION_DONE_KEY not in partition._state.get(TTL_SYSTEM_CF_NAME, {})
    partition.close()
