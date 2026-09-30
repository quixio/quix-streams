"""
Regression tests for a ``state.set(..., ttl=...)`` write whose record carries
Kafka's ``NO_TIMESTAMP`` (``-1``).

``_compute_stamp`` derives the expiry as ``timestamp + ttl_ms`` and only
rejects a non-positive *result*, so ``timestamp=-1`` with a small ``ttl`` (or
any ``ttl`` when ``timestamp`` is very close to zero) produces a small
positive expiry near the epoch (e.g. 1 Jan 1970), rather than raising.

Such a write correctly does not itself trigger the unflipped-store TTL flip
(``advance_high_water`` ignores a negative timestamp), but the bogus near-epoch
stamp used to still be recorded in ``_pending_stamps``. If a DIFFERENT write in
the same flush batch flipped the store, ``_restamp_default_cf_cache_for_flip``
applied that stale stamp to the NO_TIMESTAMP record, so it was persisted
already expired and swept on the very next TTL sweep — silent data loss, with
nothing logged.

The fix withholds the pending-stamp bookkeeping the same way the flip-trigger
bookkeeping was already withheld: a write with ``timestamp is None or
timestamp < 0`` clears (rather than sets) its own pending stamp, so it falls
back to the existing ``SENTINEL_NEVER`` (never-expires) default in
``_restamp_default_cf_cache_for_flip`` instead of a bogus near-epoch one.
"""

from datetime import timedelta

from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import TTL_INDEX_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
)

NO_TIMESTAMP = -1  # confluent_kafka.TIMESTAMP_NOT_AVAILABLE


def _decode_default_cf(partition):
    cf = partition.get_or_create_column_family("default")
    return {key: decode_ttl_value(value) for key, value in cf.items()}


def _decode_index_cf(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


class TestTtlNoTimestampFlip:
    # The exact repro from the report: an unflipped store gets a NO_TIMESTAMP
    # ttl= write and a normally-timestamped ttl= write in the SAME flush; the
    # second write flips the store. The NO_TIMESTAMP record must come out
    # never-expires, not stamped with a ~1970 expiry.
    def test_no_timestamp_write_is_never_expires_after_flip_in_same_batch(
        self, store_partition_factory
    ):
        partition = store_partition_factory(name="db", options=RocksDBOptions())
        assert partition.uses_ttl_stamps is False

        with partition.begin() as tx:
            # Un-anchorable write: NO_TIMESTAMP + a small ttl would otherwise
            # compute a small positive (near-1970) expiry.
            tx.set(
                key="k_no_ts",
                value="v_no_ts",
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(seconds=5),
            )
            # A second, normally-timestamped ttl= write in the SAME batch —
            # this is the one that flips the store.
            tx.set(
                key="k_real_ts",
                value="v_real_ts",
                prefix=b"pfx",
                timestamp=1_000_000_000_000,
                ttl=timedelta(days=1),
            )

        assert partition.uses_ttl_stamps is True

        decoded = _decode_default_cf(partition)
        index = _decode_index_cf(partition)

        no_ts_key = next(k for k in decoded if b"k_no_ts" in k)
        expires_at, payload = decoded[no_ts_key]
        assert expires_at == SENTINEL_NEVER, (
            "a NO_TIMESTAMP ttl= write must never be stamped with a bogus "
            "near-epoch expiry when a sibling write flips the store"
        )
        assert payload == b'"v_no_ts"'
        # Sentinel (never-expires) entries are never indexed, so it cannot be
        # picked up by the TTL sweep.
        assert no_ts_key not in index

        real_ts_key = next(k for k in decoded if b"k_real_ts" in k)
        real_expires_at, _ = decoded[real_ts_key]
        assert real_expires_at == 1_000_000_000_000 + 86_400_000
        assert index[real_ts_key] == real_expires_at

        partition.close()

    # Same scenario via set_bytes() (the raw-bytes sibling of set()).
    def test_no_timestamp_set_bytes_is_never_expires_after_flip_in_same_batch(
        self, store_partition_factory
    ):
        partition = store_partition_factory(name="db", options=RocksDBOptions())

        with partition.begin() as tx:
            tx.set_bytes(
                key="k_no_ts",
                value=b'"v_no_ts"',
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(seconds=5),
            )
            tx.set_bytes(
                key="k_real_ts",
                value=b'"v_real_ts"',
                prefix=b"pfx",
                timestamp=1_000_000_000_000,
                ttl=timedelta(days=1),
            )

        assert partition.uses_ttl_stamps is True
        decoded = _decode_default_cf(partition)
        no_ts_key = next(k for k in decoded if b"k_no_ts" in k)
        assert decoded[no_ts_key][0] == SENTINEL_NEVER
        partition.close()

    # A NO_TIMESTAMP ttl= write must also clear (not leave stale) an earlier
    # pending stamp for the SAME key from a real-timestamped write earlier in
    # the same unflipped batch — last-write-wins.
    def test_no_timestamp_write_clears_earlier_pending_stamp_same_key(
        self, store_partition_factory
    ):
        partition = store_partition_factory(name="db", options=RocksDBOptions())

        with partition.begin() as tx:
            tx.set(
                key="k",
                value="v1",
                prefix=b"pfx",
                timestamp=1_000_000_000_000,
                ttl=timedelta(seconds=5),
            )
            # Same key, re-written with NO_TIMESTAMP later in the same batch.
            tx.set(
                key="k",
                value="v2",
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(seconds=5),
            )
            # A sibling write flips the store.
            tx.set(
                key="k_flip",
                value="v_flip",
                prefix=b"pfx",
                timestamp=1_000_000_000_000,
                ttl=timedelta(days=1),
            )

        decoded = _decode_default_cf(partition)
        k = next(key for key in decoded if b'"k"' in key or key.endswith(b"k"))
        expires_at, payload = decoded[k]
        assert expires_at == SENTINEL_NEVER
        assert payload == b'"v2"'
        partition.close()

    # Sanity check pinning _compute_stamp's own behavior: it does NOT raise
    # for timestamp=NO_TIMESTAMP with a small ttl (the non-positive-expiry
    # guard only rejects an expiry <= 0, and -1 + 5000 = 4999 > 0). This is
    # what makes the pending-stamps bookkeeping the only place the bug could
    # be fixed.
    def test_compute_stamp_does_not_reject_no_timestamp_small_ttl(
        self, store_partition_factory
    ):
        partition = store_partition_factory(name="db", options=RocksDBOptions())
        with partition.begin() as tx:
            stamp = tx._compute_stamp(ttl=timedelta(seconds=5), timestamp=NO_TIMESTAMP)
        assert stamp == NO_TIMESTAMP + 5_000
        assert 0 < stamp < 10_000
        partition.close()
