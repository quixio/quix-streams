"""
Transaction-level stamp guards (review batch 3), both backends:

- #12 negative event-time guard: a negative timestamp must not corrupt the
  high-water (``int_to_bytes`` is unsigned → ``struct.error`` on persist) and a
  negative computed expiry must raise a *clear* ``ValueError`` at ``_compute_stamp``
  instead of a raw ``struct.error`` crash-loop.
- #14 validate-before-stage + FAILED: on the unflipped ``set`` / ``set_bytes``
  paths the stamp is validated BEFORE the value is staged, and any stamp
  ``ValueError`` marks the transaction FAILED so a caught error cannot commit a
  stray un-stamped legacy value. The flipped path is FAILED-marked too.
"""

import struct
from datetime import timedelta

import pytest

from quixstreams.state.manager import SUPPORTED_STORES

BASE_TS = 1_000_000_000_000


def _flip(partition, ts=BASE_TS, ttl=timedelta(days=1)):
    """Flip an empty store into TTL mode via a first ttl= write."""
    with partition.begin() as tx:
        tx.set(key="seed", value="seed", prefix=b"seed", timestamp=ts, ttl=ttl)
    assert partition.uses_ttl_stamps is True


def _staged_default_keys(tx):
    return {
        key
        for prefix_map in tx._update_cache.get_updates("default").values()
        for key in prefix_map
    }


def _get(partition, key, prefix=b"pfx", timestamp=None):
    tx = partition.begin()
    return tx.get(key=key, prefix=prefix, cf_name="default", timestamp=timestamp)


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestValidateBeforeStageFailed:
    """#14"""

    def test_unflipped_set_missing_timestamp_fails_and_stages_nothing(
        self, store_partition
    ):
        tx = store_partition.begin()
        with pytest.raises(ValueError):
            tx.set(
                key="k", value="v", prefix=b"pfx", timestamp=None, ttl=timedelta(days=1)
            )
        assert tx.failed, "a stamp ValueError must mark the transaction FAILED"
        assert _staged_default_keys(tx) == set(), (
            "nothing must be staged when the stamp validation fails "
            "(a caught error must not leave a stray legacy value behind)"
        )

    def test_unflipped_set_bytes_invalid_ttl_fails_and_stages_nothing(
        self, store_partition
    ):
        tx = store_partition.begin()
        with pytest.raises(ValueError):
            tx.set_bytes(
                key="k", value=b"v", prefix=b"pfx", timestamp=BASE_TS, ttl=timedelta(0)
            )
        assert tx.failed
        assert _staged_default_keys(tx) == set()

    def test_flipped_set_missing_timestamp_fails(self, store_partition):
        _flip(store_partition)
        tx = store_partition.begin()
        with pytest.raises(ValueError):
            tx.set(
                key="k", value="v", prefix=b"pfx", timestamp=None, ttl=timedelta(days=1)
            )
        assert tx.failed


@pytest.mark.parametrize("store_type", SUPPORTED_STORES, indirect=True)
class TestNegativeEventTimeGuard:
    """#12"""

    def test_negative_first_timestamp_does_not_crash_high_water(self, store_partition):
        # Kafka NO_TIMESTAMP (-1) on the first ttl= write. On HEAD this sets the
        # high-water to -1 and int_to_bytes(-1) raises struct.error at persist.
        try:
            with store_partition.begin() as tx:
                tx.set(
                    key="k",
                    value="v",
                    prefix=b"pfx",
                    timestamp=-1,
                    ttl=timedelta(days=1),
                )
        except struct.error:  # pragma: no cover - the bug we are fixing
            pytest.fail("negative timestamp reached the unsigned high-water packer")

        hw = store_partition.high_water_ms
        assert hw is None or hw >= 0, f"high-water must never go negative, got {hw}"
        # The value (expiry = -1 + 1 day, still positive) is stored and readable.
        assert _get(store_partition, "k") == "v"

    def test_pre_epoch_expiry_raises_valueerror_not_structerror(self, store_partition):
        _flip(store_partition)
        tx = store_partition.begin()
        # A deeply pre-epoch timestamp makes timestamp + ttl < 0.
        with pytest.raises(ValueError):
            try:
                tx.set(
                    key="k",
                    value="v",
                    prefix=b"pfx",
                    timestamp=-(10**15),
                    ttl=timedelta(days=1),
                )
            except struct.error:  # pragma: no cover - the bug we are fixing
                pytest.fail(
                    "negative expiry reached the unsigned stamp packer "
                    "(raw struct.error instead of a clear ValueError)"
                )
        assert tx.failed

        # A subsequent valid write still flushes cleanly (no crash-loop, no
        # corrupted high-water left behind).
        ts = BASE_TS + 5000
        with store_partition.begin() as tx2:
            tx2.set(
                key="ok",
                value="fine",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )
        assert _get(store_partition, "ok", timestamp=ts) == "fine"

    def test_advance_high_water_ignores_negative(self, store_partition):
        store_partition.advance_high_water(1000)
        store_partition.advance_high_water(-1)
        assert store_partition.high_water_ms == 1000
        # From cold, a negative timestamp must not establish a negative high-water.
        store_partition.advance_high_water(None)
        assert store_partition.high_water_ms == 1000
