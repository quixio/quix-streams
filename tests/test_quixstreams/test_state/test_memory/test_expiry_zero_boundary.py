"""
Bug #1 (Memory twin): expiry==0 write/read boundary corruption.

Memory-backend mirror of ``test_rocksdb/test_expiry_zero_boundary.py``.

``_compute_stamp`` (``memory/partition.py``) guarded ``if expiry < 0: raise`` but
NOT ``== 0``. ``_safe_decode_stamp`` (shared with RocksDB) accepts only
``0 < stamp < _MAX_PLAUSIBLE_STAMP_MS``, so a stamp of exactly 0 fails the
validator and the read path degrades to returning the RAW bytes.

**Trigger:** ``timestamp=-1`` with ``ttl=timedelta(milliseconds=1)`` ->
``expiry = -1 + 1 = 0``.

**Correct fix (review batch 4) -- reject at write, do NOT round-trip:** reject
``expiry <= 0`` at WRITE time with a clean ``ValueError`` rather than widen the
read validator to accept ``stamp == 0`` (which would mis-read genuine legacy
values whose first 8 bytes are \\x00 * 8 as expire-at-epoch stamps and strip/sweep
them). So the original "value round-trips" target is the WRONG target; the correct
target is a rejected write, asserted here.
"""

from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock

import pytest

from quixstreams.state.exceptions import StateSerializationError
from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.recovery import ChangelogProducer

BASE_TS = 1_000_000_000_000


def _make_changelog_producer_mock():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _memory_partition():
    return MemoryStorePartition(
        changelog_producer=_make_changelog_producer_mock(),
    )


def _flip(partition, ts=BASE_TS, ttl=timedelta(days=1)):
    """Flip the in-memory store into TTL mode via a normal ttl= write."""
    with partition.begin() as tx:
        tx.set(key="seed", value="seed", prefix=b"pfx", timestamp=ts, ttl=ttl)
    assert partition.uses_ttl_stamps is True


class TestExpiryZeroBoundaryMemory:
    def test_expiry_zero_write_is_rejected(self):
        """A ttl= write that produces expiry==0 (timestamp=-1, ttl=1ms) must be
        REJECTED at write time with a clean ValueError.

        Rejection -- not round-trip storage -- is the correct target (see the
        RocksDB twin). RED before the fix: _compute_stamp accepted expiry==0 and
        stored a stamp-0 blob, so set() did NOT raise."""
        partition = _memory_partition()
        _flip(partition)

        with pytest.raises(ValueError):
            with partition.begin() as tx:
                tx.set(
                    key="zero-expiry",
                    value="payload",
                    prefix=b"pfx",
                    timestamp=-1,
                    ttl=timedelta(milliseconds=1),
                )

    def test_expiry_zero_does_not_raise_serialization_error(self):
        """The rejection must surface as a clean ValueError at write time, NOT as a
        StateSerializationError later on read-back.

        RED before the fix: set() did not raise at all."""
        partition = _memory_partition()
        _flip(partition)

        with pytest.raises(ValueError) as exc_info:
            with partition.begin() as tx:
                tx.set(
                    key="zero-expiry",
                    value={"nested": True},
                    prefix=b"pfx",
                    timestamp=-1,
                    ttl=timedelta(milliseconds=1),
                )
        assert not isinstance(exc_info.value, StateSerializationError)
