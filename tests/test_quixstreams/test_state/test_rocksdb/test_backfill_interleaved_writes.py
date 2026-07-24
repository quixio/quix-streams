"""
Regression tests for two confirmed bugs in the TTL legacy-backfill resume path,
found by cursor-misalignment analysis:

Double-wrap corruption on resume after interleaved legacy write.
    The backfill re-sorts the *current* default CF on resume but reuses the old
    integer cursor. A new legacy key inserted between crash and resume shifts the
    already-stamped keys past the cursor. ``encode_ttl_value`` wraps an
    already-stamped value a second time, corrupting it (wrong expiry / garbled
    payload on decode).

Silently skipped key (new interleaved key never stamped).
    The interleaved key slides into the cursor window that the resume considers
    "already done" and is never stamped. After the backfill completes it remains
    raw (un-stamped) in a flipped partition.
"""

from datetime import timedelta

import pytest

from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import (
    TTL_BACKFILL_PROGRESS_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import (
    decode_index_key,
    decode_ttl_value,
)
from quixstreams.state.serialization import int_from_bytes

DAY_MS = 86_400_000


def _seed_legacy_records(partition, records, prefix=b"pfx"):
    """Write plain (un-stamped) records to a legacy partition."""
    with partition.begin() as tx:
        for key, value in records:
            tx.set(key=key, value=value, prefix=prefix)


def _decode_default_cf(partition):
    """Return ``{raw_key: (expires_at, payload)}`` for the default CF."""
    cf = partition.get_or_create_column_family("default")
    out = {}
    for key, value in cf.items():
        out[key] = decode_ttl_value(value)
    return out


def _raw_default_cf(partition):
    """Return the raw (un-decoded) ``{key: value}`` of the default CF."""
    cf = partition.get_or_create_column_family("default")
    return {key: value for key, value in cf.items()}


def _decode_index_cf(partition):
    """Return ``{user_key: expires_at}`` for the ``__ttl_index__`` CF."""
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


class TestInterleavedWriteCorruption:
    """
    Double-wrap corruption.

    Validates the cursor-based resume requirement: keys stamped by the
    interrupted run are NOT re-read (cursor-skipped), so they keep that run's
    expiry. When a new legacy key inserts before already-stamped keys in sorted
    order, the cursor-to-key mapping is invalidated and an already-stamped key is
    re-stamped (double-wrapped), corrupting its value.

    Expected correct behavior: every pre-existing key's value reads back equal to
    its original written value after the completed backfill. On current code an
    already-stamped key is ``encode_ttl_value``-wrapped twice, corrupting the
    payload (the 8-byte expiry prefix is treated as part of the value and
    re-prefixed).
    """

    def test_double_wrap_corruption_on_resume(self, store_partition_factory):
        # Seed 10 legacy keys (k0..k9) with small chunk size so we can crash
        # after a partial backfill.
        m = 10
        chunk_size = 3
        original_records = [(f"k{i}", f"v{i}") for i in range(m)]
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, original_records)
        partition.close()

        ttl = timedelta(days=7)

        # --- Run 1: crash after 2 chunk writes (6 keys stamped) ---
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        crash = {"n": 0}
        real_write = partition._write

        def crashing_write(batch):
            crash["n"] += 1
            if crash["n"] > 2:
                raise RuntimeError("simulated crash mid-backfill")
            return real_write(batch)

        partition._write = crashing_write

        ts1 = 1_000_000_000_000
        with pytest.raises(RuntimeError, match="simulated crash"):
            with partition.begin() as tx:
                tx.set(
                    key="trigger1",
                    value="t1",
                    prefix=b"pfx",
                    timestamp=ts1,
                    ttl=ttl,
                )
        partition._write = real_write

        # Verify cursor is persisted at 6 (2 chunks of 3).
        from quixstreams.state.metadata import METADATA_CF_NAME

        meta = partition.get_or_create_column_family(METADATA_CF_NAME)
        cursor_val = int_from_bytes(meta.get(TTL_BACKFILL_PROGRESS_KEY))
        assert cursor_val == 6, f"Expected cursor=6, got {cursor_val}"
        del meta
        partition.close()

        # --- Interleave a NEW legacy key that sorts BEFORE the k-keys ---
        # Key "aaa" serializes to b'pfx|"aaa"'. Since '"a' < '"k' in
        # lexicographic byte order, it will sort before all k0..k9 keys.
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        # Still legacy (flag never written).
        assert partition.uses_ttl_stamps is False

        # Write the interleaved key WITHOUT ttl — a plain legacy write.
        with partition.begin() as tx:
            tx.set(key="aaa", value="interleaved", prefix=b"pfx")

        # Confirm it sorted before the k-keys in the default CF.
        cf = partition.get_or_create_column_family("default")
        all_keys_sorted = sorted(cf.keys())
        aaa_serialized = None
        for k in all_keys_sorted:
            if b"aaa" in k:
                aaa_serialized = k
                break
        assert aaa_serialized is not None
        # "aaa" must sort before the first "k0" key.
        first_k_key = next(k for k in all_keys_sorted if b'"k' in k)
        assert (
            aaa_serialized < first_k_key
        ), f"Interleaved key {aaa_serialized!r} should sort before {first_k_key!r}"
        del cf
        partition.close()

        # --- Run 2: resume the backfill via a ttl= write ---
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        assert partition.uses_ttl_stamps is False

        ts2 = ts1 + 5_000
        with partition.begin() as tx:
            tx.set(
                key="trigger2",
                value="t2",
                prefix=b"pfx",
                timestamp=ts2,
                ttl=ttl,
            )

        assert partition.uses_ttl_stamps is True

        # --- Assert: every original key's value reads back to its original ---
        # On current (buggy) code, at least one key is double-wrapped and its
        # decoded payload will NOT match the original JSON-serialized value.
        decoded = _decode_default_cf(partition)
        run1_expiry = ts1 + 7 * DAY_MS
        run2_expiry = ts2 + 7 * DAY_MS

        for i in range(m):
            key_str = f"k{i}"
            matching = [k for k in decoded if f'"{key_str}"'.encode() in k]
            assert (
                len(matching) == 1
            ), f"Expected exactly one entry for {key_str}, found {len(matching)}"
            raw_key = matching[0]
            expires_at, payload = decoded[raw_key]
            expected_payload = f'"v{i}"'.encode()
            # The expiry should be one of the two run expiries (not a garbled
            # value from double-wrapping).
            assert expires_at in (run1_expiry, run2_expiry), (
                f"Key {key_str}: expires_at={expires_at} is not in "
                f"({run1_expiry}, {run2_expiry}); likely double-wrapped"
            )
            # The payload should be the clean original value, not a nested
            # stamped blob (which would have an 8-byte expiry prefix).
            assert payload == expected_payload, (
                f"Key {key_str}: payload={payload!r} != expected {expected_payload!r}; "
                f"likely double-wrapped (encode_ttl_value applied twice)"
            )

        partition.close()

    def test_interleaved_key_is_stamped_after_resume(self, store_partition_factory):
        """
        Silently skipped key.

        Validates that a key written between crash and resume is also TTL-stamped
        after the completed backfill. On current code, the interleaved key slides
        into the "already done" cursor window (because the new sorted list shifted)
        and is never stamped.

        Expected correct behavior: after the backfill completes, the interleaved
        key has a valid TTL stamp (its raw bytes decode via ``decode_ttl_value``
        to the backfill expiry) and has a ``__ttl_index__`` entry.
        """
        m = 10
        chunk_size = 3
        original_records = [(f"k{i}", f"v{i}") for i in range(m)]
        partition = store_partition_factory(name="db2")
        _seed_legacy_records(partition, original_records)
        partition.close()

        ttl = timedelta(days=7)

        # --- Run 1: crash after 2 chunk writes (6 keys stamped) ---
        partition = store_partition_factory(
            name="db2",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        crash = {"n": 0}
        real_write = partition._write

        def crashing_write(batch):
            crash["n"] += 1
            if crash["n"] > 2:
                raise RuntimeError("simulated crash mid-backfill")
            return real_write(batch)

        partition._write = crashing_write

        ts1 = 1_000_000_000_000
        with pytest.raises(RuntimeError, match="simulated crash"):
            with partition.begin() as tx:
                tx.set(
                    key="trigger1",
                    value="t1",
                    prefix=b"pfx",
                    timestamp=ts1,
                    ttl=ttl,
                )
        partition._write = real_write
        partition.close()

        # --- Interleave a NEW legacy key ---
        partition = store_partition_factory(
            name="db2",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        with partition.begin() as tx:
            tx.set(key="aaa", value="interleaved", prefix=b"pfx")
        partition.close()

        # --- Run 2: resume ---
        partition = store_partition_factory(
            name="db2",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        ts2 = ts1 + 5_000
        with partition.begin() as tx:
            tx.set(
                key="trigger2",
                value="t2",
                prefix=b"pfx",
                timestamp=ts2,
                ttl=ttl,
            )

        assert partition.uses_ttl_stamps is True

        # --- Assert: the interleaved "aaa" key is TTL-stamped ---
        raw = _raw_default_cf(partition)
        aaa_key = next((k for k in raw if b"aaa" in k), None)
        assert aaa_key is not None, "Interleaved key 'aaa' not found in default CF"

        aaa_raw = raw[aaa_key]
        # On current (buggy) code, this key was skipped by the backfill and
        # remains as its raw legacy value (un-stamped JSON). decode_ttl_value
        # would either raise or return a garbled expiry.
        expires_at, payload = decode_ttl_value(aaa_raw)
        run2_expiry = ts2 + 7 * DAY_MS
        assert expires_at == run2_expiry, (
            f"Interleaved key 'aaa': expires_at={expires_at} != expected "
            f"{run2_expiry}; the key was likely skipped by the resume and "
            f"never stamped"
        )
        assert payload == b'"interleaved"', (
            f"Interleaved key 'aaa': payload={payload!r} != expected "
            f"b'\"interleaved\"'; value was corrupted or not stamped"
        )

        # Also verify the key has a __ttl_index__ entry.
        index = _decode_index_cf(partition)
        assert aaa_key in index, (
            f"Interleaved key 'aaa' ({aaa_key!r}) has no __ttl_index__ entry; "
            f"it was skipped by the backfill"
        )
        assert index[aaa_key] == run2_expiry

        partition.close()
