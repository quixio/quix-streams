"""
Regression test for finding 3 (batch4 code review of commit ``56be260b``): H1
does not clamp the persisted high-water on load, memory-backend twin of
``test_rocksdb/test_high_water_load_clamp.py``.

``MemoryStorePartition._load_high_water`` restores
``_high_water_ms = int_from_bytes(raw)`` from the ``__metadata__``
``TTL_HIGH_WATER_KEY`` entry with **no** ``>= _MAX_PLAUSIBLE_STAMP_MS`` sanity
clamp -- unlike ``advance_high_water``, which the H1 hardening already guards
against an implausibly large event-time timestamp. A store poisoned by the
pre-H1 bug (a huge high-water persisted the same way ``write()`` persists it)
reloads the poisoned value verbatim: every finite-stamped record then reads as
already-expired (``stamp <= _high_water_ms``) and is swept on the next sweep --
the exact mass-eviction the H1 guarantee (never mass-delete) exists to
prevent, now resurrected across a reload.

Validates spec: the H1 never-mass-delete guarantee must survive a reload of
persisted high-water metadata, not just protect the live
``advance_high_water`` call site.
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import METADATA_CF_NAME
from quixstreams.state.rocksdb.metadata import TTL_HIGH_WATER_KEY
from quixstreams.state.rocksdb.ttl_codec import _MAX_PLAUSIBLE_STAMP_MS
from quixstreams.state.serialization import int_to_bytes

DAY_MS = 86_400_000
BASE_TS = 1_780_000_000_000
POISONED_HIGH_WATER_MS = 10**18


class TestHighWaterLoadClampMemory:
    def test_load_high_water_ignores_poisoned_persisted_value(self):
        partition = MemoryStorePartition(changelog_producer=None)
        ttl = timedelta(days=7)

        with partition.begin() as tx:
            tx.set(key="k0", value="v0", prefix=b"pfx", timestamp=BASE_TS, ttl=ttl)
        assert partition.uses_ttl_stamps is True

        # Sanity: readable right after the write, with the real (small) high-water.
        assert partition.begin().get(key="k0", prefix=b"pfx", timestamp=BASE_TS) == "v0"
        assert partition._high_water_ms == BASE_TS

        # Poison the persisted metadata exactly the way ``write()`` persists it
        # (mimicking an on-disk artifact from before the H1 guard existed), then
        # simulate a reload: clear the live clock and reload from metadata.
        partition._state[METADATA_CF_NAME][TTL_HIGH_WATER_KEY] = int_to_bytes(
            POISONED_HIGH_WATER_MS
        )
        partition._high_water_ms = None
        partition._load_high_water()

        # Desired (H1 parity): the poisoned value is clamped/ignored on load.
        assert (
            partition._high_water_ms is None
            or partition._high_water_ms < _MAX_PLAUSIBLE_STAMP_MS
        ), f"poisoned high-water loaded verbatim: {partition._high_water_ms}"

        # The finite-stamped record must still be readable (not mass-evicted).
        # No ``timestamp=`` here: the live read path gates on
        # ``partition.high_water_ms`` unconditionally, independent of
        # ``advance_high_water``, so this isolates the loaded value's effect.
        assert partition.begin().get(key="k0", prefix=b"pfx") == "v0"
        partition.close()
