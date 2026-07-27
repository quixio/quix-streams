"""
Regression test for finding 3 (batch4 code review of commit ``56be260b``): H1
does not clamp the persisted high-water on load.

``RocksDBStorePartition._load_high_water`` restores
``_high_water_ms = int_from_bytes(raw)`` from the on-disk
``__ttl_high_water_ms__`` metadata key with **no** ``>= _MAX_PLAUSIBLE_STAMP_MS``
sanity clamp -- unlike ``advance_high_water``, which the H1 hardening already
guards against an implausibly large event-time timestamp. A store poisoned by
the pre-H1 bug (a huge high-water persisted to ``TTL_HIGH_WATER_KEY`` before the
``advance_high_water`` guard existed) reloads the poisoned value verbatim on
reopen: every finite-stamped record then reads as already-expired
(``stamp <= _high_water_ms``) and is swept on the next flush -- the exact
mass-eviction the H1 guarantee (never mass-delete) exists to prevent, now
resurrected across a restart.

Validates spec: the H1 never-mass-delete guarantee must survive a process
restart of a persisted store, not just protect the live
``advance_high_water`` call site.
"""

from quixstreams.state.metadata import METADATA_CF_NAME
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.metadata import (
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import (
    _MAX_PLAUSIBLE_STAMP_MS,
    encode_index_key,
    encode_ttl_value,
)
from quixstreams.state.serialization import int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000
POISONED_HIGH_WATER_MS = 10**18


def _seed_poisoned_store(tmp_path, name, *, expiry_ms):
    """Build a flipped on-disk TTL store with one finite-stamped record and a
    poisoned (implausibly large) persisted high-water, mimicking the on-disk
    artifact a pre-H1 store would have left behind. Returns the raw changelog
    key of the seeded record."""
    path = (tmp_path / name).as_posix()
    opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    partition = RocksDBStorePartition(path, options=opts, changelog_producer=None)

    raw_key = b"pfx|" + json_dumps("k0")
    stamped = encode_ttl_value(expiry_ms, json_dumps("v0"))
    default_cf = partition.get_or_create_column_family("default")
    default_cf[raw_key] = stamped

    metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
    metadata_cf[TTL_ENABLED_KEY] = b"\x01"
    metadata_cf[STATE_FORMAT_VERSION_KEY] = int_to_bytes(STATE_FORMAT_VERSION)
    metadata_cf[TTL_HIGH_WATER_KEY] = int_to_bytes(POISONED_HIGH_WATER_MS)

    index_cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    index_cf[encode_index_key(expiry_ms, raw_key)] = b""

    partition.close()
    return raw_key


class TestHighWaterLoadClampRocksDB:
    def test_reopen_ignores_poisoned_persisted_high_water(self, tmp_path):
        expiry_ms = NOW_MS + 7 * DAY_MS
        _seed_poisoned_store(tmp_path, "poisoned", expiry_ms=expiry_ms)

        opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
        reopened = RocksDBStorePartition(
            (tmp_path / "poisoned").as_posix(),
            options=opts,
            changelog_producer=None,
        )
        assert reopened.uses_ttl_stamps is True

        # Desired (H1 parity): the poisoned value is clamped/ignored on load.
        assert (
            reopened._high_water_ms is None
            or reopened._high_water_ms < _MAX_PLAUSIBLE_STAMP_MS
        ), f"poisoned high-water loaded verbatim: {reopened._high_water_ms}"

        # The finite-stamped record must still be readable (not mass-evicted).
        tx = reopened.begin()
        assert tx.get(key="k0", prefix=b"pfx") == "v0"
        reopened.close()
