"""
Regression tests for a ``state.set(..., ttl=...)`` write whose record carries
Kafka's ``NO_TIMESTAMP`` (``-1``).

Such a write correctly does not itself trigger the unflipped-store TTL flip
(``advance_high_water`` ignores a negative timestamp), but a bogus near-epoch
stamp used to still be recorded in ``_pending_stamps``. If a DIFFERENT write in
the same flush batch flipped the store, ``_restamp_default_cf_cache_for_flip``
applied that stale stamp to the NO_TIMESTAMP record, so it was persisted
already expired and swept on the very next TTL sweep — silent data loss, with
nothing logged.

``_compute_stamp`` is the single place every ``ttl=`` write path stamps through
(``set`` / ``set_bytes``, flipped and unflipped), so the fix lives there: a write
whose timestamp is negative cannot anchor an expiry and falls back to
``SENTINEL_NEVER`` (never-expires), with a warning, instead of a bogus near-epoch
stamp. Expiries that are non-positive or implausibly large are still rejected with
``ValueError`` as before.
"""

from datetime import timedelta

import pytest

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

    # The reviewer-reported case: the same NO_TIMESTAMP ttl= write on a store
    # that is ALREADY flipped (every TTL store from its second batch onward).
    # set() returns early into _set_default_cf_stamped(), which stamps inline
    # and never touches the _pending_stamps bookkeeping.
    def test_no_timestamp_write_is_never_expires_on_an_already_flipped_store(
        self, store_partition_factory
    ):
        partition = store_partition_factory(name="db", options=RocksDBOptions())
        assert partition.uses_ttl_stamps is False

        # A first, ordinary ttl= write flips the store and sets the frontier.
        with partition.begin() as tx:
            tx.set(
                key="k_real_ts",
                value="v_real_ts",
                prefix=b"pfx",
                timestamp=1_000_000_000_000,
                ttl=timedelta(days=1),
            )
        assert partition.uses_ttl_stamps is True

        # A later batch carries the NO_TIMESTAMP ttl= write.
        with partition.begin() as tx:
            tx.set(
                key="k_no_ts",
                value="v_no_ts",
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(seconds=5),
            )

        decoded = _decode_default_cf(partition)
        index = _decode_index_cf(partition)
        no_ts_key = next(k for k in decoded if b"k_no_ts" in k)
        expires_at, _ = decoded[no_ts_key]

        with partition.begin() as tx:
            # Control: the read path itself works.
            assert tx.get(key="k_real_ts", prefix=b"pfx") == "v_real_ts"
            readback = tx.get(key="k_no_ts", prefix=b"pfx")
        partition.close()

        assert expires_at == SENTINEL_NEVER, (
            "a NO_TIMESTAMP ttl= write on an already-flipped store must not be "
            f"stamped with a near-epoch expiry (got {expires_at})"
        )
        assert no_ts_key not in index
        assert readback == "v_no_ts", "the record is unreadable the moment it lands"

    # Same, via set_bytes() on an already-flipped store.
    def test_no_timestamp_set_bytes_is_never_expires_on_an_already_flipped_store(
        self, store_partition_factory
    ):
        partition = store_partition_factory(name="db", options=RocksDBOptions())
        with partition.begin() as tx:
            tx.set(
                key="k_real_ts",
                value="v_real_ts",
                prefix=b"pfx",
                timestamp=1_000_000_000_000,
                ttl=timedelta(days=1),
            )
        with partition.begin() as tx:
            tx.set_bytes(
                key="k_no_ts",
                value=b'"v_no_ts"',
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(seconds=5),
            )
        decoded = _decode_default_cf(partition)
        no_ts_key = next(k for k in decoded if b"k_no_ts" in k)
        assert decoded[no_ts_key][0] == SENTINEL_NEVER
        assert no_ts_key not in _decode_index_cf(partition)
        partition.close()

    # _compute_stamp's contract for a negative timestamp: never-expires, with a
    # warning, but an expiry that is still non-positive is still rejected.
    def test_compute_stamp_negative_timestamp(self, store_partition_factory, caplog):
        partition = store_partition_factory(name="db", options=RocksDBOptions())
        with partition.begin() as tx:
            with caplog.at_level("WARNING"):
                stamp = tx._compute_stamp(
                    ttl=timedelta(seconds=5), timestamp=NO_TIMESTAMP
                )
            assert stamp == SENTINEL_NEVER
            assert "negative event-time timestamp" in caplog.text
            # -1 + 1ms = 0 -> still a loud rejection, as before.
            with pytest.raises(ValueError, match="non-positive expiry"):
                tx._compute_stamp(ttl=timedelta(milliseconds=1), timestamp=NO_TIMESTAMP)
        partition.close()
