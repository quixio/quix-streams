"""
Red-first test: the flipped-partition read path silently loses un-stamped
legacy values SHORTER than the 8-byte stamp prefix.

``RocksDBPartitionTransaction._get_bytes`` degrades safely for un-stamped legacy
values on a flipped partition: ``_safe_decode_stamp`` refuses to strip a prefix
that is not robustly a stamp, and the value is returned RAW (treated as
never-expires) with a warn-once. That is the fail-safe protecting values written
before the store flipped.

But an earlier branch short-circuits it::

    if len(raw_bytes) < TTL_STAMP_BYTES:
        return Marker.UNDEFINED

A value under 8 bytes cannot be a stamp, so it is necessarily an un-stamped
legacy payload -- exactly the class the fail-safe exists to protect. Instead of
degrading raw it reads back as MISSING. ``_safe_decode_stamp`` already returns
``None`` for short input, so this branch is redundant as well as harmful.

The asymmetry is the bug: a long un-stamped legacy value survives, a short one
(``1``, ``true``, ``null``, ``"ab"`` -- all under 8 serialized bytes) is lost.

Note this is NOT reachable through ordinary writes: a write on a flipped store
is stamped to >= 9 bytes, so it round-trips fine (pinned below as a regression
guard). It bites un-stamped survivors of an interrupted migration / flip window.
"""

from datetime import timedelta

from rocksdict import WriteBatch

from quixstreams.state.rocksdb import RocksDBOptions

TTL = timedelta(days=7)
TS = 1_000_000_000_000


def _flipped_partition(store_partition_factory, name):
    p = store_partition_factory(
        name=name, options=RocksDBOptions(legacy_records_ttl=TTL)
    )
    with p.begin() as tx:
        tx.set(key="anchor", value="a", prefix=b"pfx", timestamp=TS, ttl=TTL)
    assert p.uses_ttl_stamps is True
    return p


def _plant_unstamped(partition, raw_key: bytes, raw_value: bytes):
    """Write a genuine UN-STAMPED value straight into the default CF, bypassing
    the stamping write path -- an interrupted-migration survivor."""
    batch = WriteBatch(raw_mode=True)
    batch.put(raw_key, raw_value, partition.get_column_family_handle("default"))
    partition._write(batch)


class TestShortUnstampedRead:
    def test_short_unstamped_value_is_not_lost(self, store_partition_factory):
        """RED: the 1-byte un-stamped value reads back as ``None`` (missing).
        GREEN: it degrades RAW, exactly as its long counterpart already does."""
        p = _flipped_partition(store_partition_factory, "short")
        _plant_unstamped(p, b'pfx|"s"', b"1")  # JSON int 1 -> 1 byte

        with p.begin() as tx:
            got = tx.get(key="s", prefix=b"pfx")
        assert got == 1, f"short un-stamped legacy value was lost: got {got!r}"
        p.close()

    def test_long_unstamped_value_survives_parity(self, store_partition_factory):
        """The already-correct half: the fail-safe returns a long un-stamped
        value raw. Pins the asymmetry the short case must match."""
        p = _flipped_partition(store_partition_factory, "long")
        _plant_unstamped(p, b'pfx|"l"', b'"a-much-longer-legacy-value"')

        with p.begin() as tx:
            got = tx.get(key="l", prefix=b"pfx")
        assert got == "a-much-longer-legacy-value"
        p.close()

    def test_ordinary_short_writes_still_round_trip(self, store_partition_factory):
        """Regression guard: ordinary short writes on a flipped store are
        stamped to >= 9 bytes and must keep round-tripping untouched."""
        p = _flipped_partition(store_partition_factory, "ordinary")
        for value in (1, True, None, "ab", 0, "", []):
            with p.begin() as tx:
                tx.set(key="k", value=value, prefix=b"pfx", timestamp=TS, ttl=TTL)
            with p.begin() as tx:
                assert tx.get(key="k", prefix=b"pfx") == value
        p.close()


class TestShortUnstampedReadMemoryParity:
    """``MemoryStorePartition._get_bytes`` carried the identical branch, so the
    same short-value loss existed on the memory backend."""

    def _flipped_memory_partition(self):
        from unittest.mock import MagicMock

        from quixstreams.state.memory import MemoryStorePartition

        p = MemoryStorePartition(changelog_producer=MagicMock(), legacy_records_ttl=TTL)
        with p.begin() as tx:
            tx.set(key="anchor", value="a", prefix=b"pfx", timestamp=TS, ttl=TTL)
        assert p.uses_ttl_stamps is True
        return p

    def test_short_unstamped_value_is_not_lost_memory(self):
        """RED: reads back ``None``. GREEN: degrades raw, like RocksDB."""
        p = self._flipped_memory_partition()
        p._state.setdefault("default", {})[b'pfx|"s"'] = b"1"

        with p.begin() as tx:
            got = tx.get(key="s", prefix=b"pfx")
        assert got == 1, f"short un-stamped legacy value was lost: got {got!r}"

    def test_long_unstamped_value_survives_parity_memory(self):
        p = self._flipped_memory_partition()
        p._state.setdefault("default", {})[b'pfx|"l"'] = b'"a-much-longer-legacy-value"'

        with p.begin() as tx:
            assert tx.get(key="l", prefix=b"pfx") == "a-much-longer-legacy-value"

    def test_ordinary_short_writes_still_round_trip_memory(self):
        p = self._flipped_memory_partition()
        for value in (1, True, None, "ab", 0, "", []):
            with p.begin() as tx:
                tx.set(key="k", value=value, prefix=b"pfx", timestamp=TS, ttl=TTL)
            with p.begin() as tx:
                assert tx.get(key="k", prefix=b"pfx") == value
