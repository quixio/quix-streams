"""
Backend-agnostic regression tests for ``ttl=`` writes that carry Kafka's
``NO_TIMESTAMP`` (``-1``), run against both the RocksDB and in-memory stores.

A negative event-time cannot anchor an expiry. Before the fix, ``timestamp + ttl``
passed validation as a small positive number near the epoch (``-1`` + 5s = 4999 ms,
i.e. 1 Jan 1970), so the record was stored already expired: it read back as
``None`` and the next TTL sweep deleted it, silently. These tests assert the
observable behaviour (the value stays readable) rather than backend-specific
bytes, for the unflipped (same batch as the flip) and already-flipped paths, and
for ``set`` and ``set_bytes``.
"""

from datetime import timedelta

import pytest

from quixstreams.state.manager import SUPPORTED_STORES

NO_TIMESTAMP = -1  # confluent_kafka.TIMESTAMP_NOT_AVAILABLE
BASE_TS = 1_000_000_000_000


def _get(partition, key, prefix=b"pfx"):
    return partition.begin().get(key=key, prefix=prefix, cf_name="default")


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestNoTimestampTtlWrite:
    def test_unflipped_store_flipped_by_sibling_write_in_same_batch(
        self, store_partition
    ):
        with store_partition.begin() as tx:
            tx.set(
                key="k_no_ts",
                value="v_no_ts",
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(seconds=5),
            )
            # A normally-timestamped ttl= write in the same batch flips the store.
            tx.set(
                key="k_real_ts",
                value="v_real_ts",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )
        assert store_partition.uses_ttl_stamps is True

        assert _get(store_partition, "k_real_ts") == "v_real_ts"
        assert _get(store_partition, "k_no_ts") == "v_no_ts"

    def test_already_flipped_store(self, store_partition):
        with store_partition.begin() as tx:
            tx.set(
                key="seed",
                value="seed",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )
        assert store_partition.uses_ttl_stamps is True

        with store_partition.begin() as tx:
            tx.set(
                key="k_no_ts",
                value="v_no_ts",
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(seconds=5),
            )
        assert _get(store_partition, "k_no_ts") == "v_no_ts"

    def test_already_flipped_store_set_bytes(self, store_partition):
        with store_partition.begin() as tx:
            tx.set(
                key="seed",
                value="seed",
                prefix=b"pfx",
                timestamp=BASE_TS,
                ttl=timedelta(days=1),
            )
        with store_partition.begin() as tx:
            tx.set_bytes(
                key="k_no_ts",
                value=b'"v_no_ts"',
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(seconds=5),
            )
        assert _get(store_partition, "k_no_ts") == "v_no_ts"

    def test_logs_a_warning(self, store_partition, caplog):
        with caplog.at_level("WARNING"):
            with store_partition.begin() as tx:
                tx.set(
                    key="k",
                    value="v",
                    prefix=b"pfx",
                    timestamp=NO_TIMESTAMP,
                    ttl=timedelta(seconds=5),
                )
        assert "negative event-time timestamp" in caplog.text

    def test_expiry_that_is_still_non_positive_is_still_rejected(self, store_partition):
        tx = store_partition.begin()
        # -1 + 1ms = 0: rejected loudly, exactly as before.
        with pytest.raises(ValueError, match="non-positive expiry"):
            tx.set(
                key="k",
                value="v",
                prefix=b"pfx",
                timestamp=NO_TIMESTAMP,
                ttl=timedelta(milliseconds=1),
            )
        assert tx.failed
