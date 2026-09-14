"""
Bug #1 (RocksDB): expiry==0 write/read boundary corruption.

``_compute_stamp`` guarded ``if expiry < 0: raise`` but NOT ``== 0``.
``_safe_decode_stamp`` accepts only ``0 < stamp < _MAX_PLAUSIBLE_STAMP_MS``, so a
stamp of exactly 0 fails the validator and the read path degrades to returning the
RAW bytes (8 zero-bytes prefix + payload) instead of the payload; deserialization
then raises ``StateSerializationError`` and the record never sweeps.

**Trigger:** a stamped write where ``timestamp + ttl_ms == 0`` -- e.g.
``timestamp=-1`` (Kafka ``NO_TIMESTAMP``) with ``ttl=timedelta(milliseconds=1)``.

**Correct fix (review batch 4) -- reject at write, do NOT round-trip:** the safe
fix rejects ``expiry <= 0`` at WRITE time with a clean ``ValueError`` rather than
widening the read validator to accept ``stamp == 0``. Widening the read side would
reintroduce the false-positive class the ``0 <`` guard prevents: a genuine
un-stamped legacy value whose first 8 bytes are ``\\x00`` * 8 would be mis-read as
an expire-at-epoch stamp and silently stripped/swept -> data loss. So the original
"value round-trips" target below is the WRONG target; the correct target is a
rejected write, asserted here.
"""

from datetime import timedelta

import pytest

from quixstreams.state.exceptions import StateSerializationError
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition

BASE_TS = 1_000_000_000_000


def _rocksdb_partition(tmp_path, name="db"):
    path = (tmp_path / name).as_posix()
    opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(path, options=opts)


def _flip(partition, ts=BASE_TS, ttl=timedelta(days=1)):
    """Flip a fresh store into TTL mode via a normal ttl= write."""
    with partition.begin() as tx:
        tx.set(key="seed", value="seed", prefix=b"pfx", timestamp=ts, ttl=ttl)
    assert partition.uses_ttl_stamps is True


class TestExpiryZeroBoundaryRocksDB:
    def test_expiry_zero_write_is_rejected(self, tmp_path):
        """A ttl= write that produces expiry==0 (timestamp=-1, ttl=1ms) must be
        REJECTED at write time with a clean ValueError.

        Rejection -- not round-trip storage -- is the correct target: a stamp-0
        value reads back as raw bytes (StateSerializationError) and never sweeps,
        and widening the read validator to accept 0 would corrupt genuine legacy
        values whose first 8 bytes are \\x00 * 8. RED before the fix: _compute_stamp
        accepted expiry==0 (0 is not < 0) and stored a stamp-0 blob, so set() did
        NOT raise."""
        partition = _rocksdb_partition(tmp_path)
        _flip(partition)

        # timestamp=-1, ttl=1ms -> expiry = -1 + 1 = 0 -> rejected at write.
        with pytest.raises(ValueError):
            with partition.begin() as tx:
                tx.set(
                    key="zero-expiry",
                    value="payload",
                    prefix=b"pfx",
                    timestamp=-1,
                    ttl=timedelta(milliseconds=1),
                )
        partition.close()

    def test_expiry_zero_does_not_raise_serialization_error(self, tmp_path):
        """The rejection must surface as a clean ValueError at write time, NOT as a
        StateSerializationError later on read-back of a corrupted blob.

        RED before the fix: set() did not raise at all -- the corruption only
        surfaced as StateSerializationError on a subsequent get()."""
        partition = _rocksdb_partition(tmp_path)
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
        # A clean ValueError, not the read-path corruption symptom.
        assert not isinstance(exc_info.value, StateSerializationError)
        partition.close()
