"""
Unit tests for the chunked legacy-records backfill (shortcut 73191 follow-up).

Covers ``dev-planning/state-ttl-legacy-backfill/spec-chunked-backfill.md`` §10:

1. Chunked backfill of a store >> chunk size completes fully (every record
   stamped at the uniform expiry, ``__ttl_index__`` populated, flag+format set,
   one ``_write`` per chunk).
2. Crash-before-flag re-runs cleanly and converges with no double-wrap.
3. Already-stamped recognizer: mixed stamped (prior run) + legacy records
   re-stamp correctly, skip already-at-target, no double-wrap.
4. Per-chunk changelog production rebuilds an identical stamped store.
5. Empty store / store smaller than one chunk behave like the existing flip.
6. Config validation for ``legacy_backfill_chunk_size``.

The chunk size is configured via ``RocksDBOptions(legacy_backfill_chunk_size=N)``.
"""

from datetime import timedelta

import pytest

from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    METADATA_CF_NAME,
)
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import (
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ENABLED_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_index_key,
    decode_ttl_value,
    encode_index_key,
    encode_ttl_value,
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


def _decode_index_cf(partition):
    """Return ``{user_key: expires_at}`` for the ``__ttl_index__`` CF."""
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _capture_default_cf_changelog(changelog_producer_mock):
    """Return the list of ``(key, value)`` default-CF changelog messages."""
    msgs = []
    for call in changelog_producer_mock.produce.call_args_list:
        headers = call.kwargs["headers"]
        if headers[CHANGELOG_CF_MESSAGE_HEADER] == "default":
            msgs.append((call.kwargs["key"], call.kwargs["value"]))
    return msgs


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestChunkSizeValidation:
    def test_default_is_10k(self):
        assert RocksDBOptions().legacy_backfill_chunk_size == 10_000

    def test_zero_raises(self):
        with pytest.raises(ValueError, match="strictly positive int"):
            RocksDBOptions(legacy_backfill_chunk_size=0)

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="strictly positive int"):
            RocksDBOptions(legacy_backfill_chunk_size=-5)

    def test_positive_accepted(self):
        assert RocksDBOptions(legacy_backfill_chunk_size=100).legacy_backfill_chunk_size == 100


# ---------------------------------------------------------------------------
# Chunked backfill behavior
# ---------------------------------------------------------------------------


class TestChunkedBackfill:
    # Case 1 — store >> chunk size completes fully, one _write per chunk.
    def test_large_store_backfills_in_chunks(self, store_partition_factory):
        m = 1000
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [(f"k{i}", f"v{i}") for i in range(m)])
        partition.close()

        chunk_size = 100
        ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )

        # Spy on the raw writer to count chunk commits.
        write_calls = {"n": 0}
        real_write = partition._write

        def counting_write(batch):
            write_calls["n"] += 1
            return real_write(batch)

        partition._write = counting_write

        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(
                key="knew",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        assert partition.uses_ttl_stamps is True
        expected_legacy_expiry = ts + 7 * DAY_MS

        decoded = _decode_default_cf(partition)
        legacy = {k: v for k, v in decoded.items() if b"knew" not in k}
        assert len(legacy) == m
        for expires_at, payload in legacy.values():
            assert expires_at == expected_legacy_expiry
            # payload decodes exactly once -> no double-wrap.
            assert payload.startswith(b'"v')

        index = _decode_index_cf(partition)
        for key in legacy:
            assert index[key] == expected_legacy_expiry

        # The chunk loop issued ceil(m / chunk_size) raw writes for the
        # backfill, plus exactly ONE more for the final flip batch (the
        # in-batch stamps + flip metadata committed via write() -> _write()).
        assert write_calls["n"] == (m // chunk_size) + 1

        meta = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert meta.get(TTL_ENABLED_KEY) == b"\x01"
        assert int_from_bytes(meta.get(STATE_FORMAT_VERSION_KEY)) == (
            STATE_FORMAT_VERSION
        )
        partition.close()

    # Case 5 — store smaller than one chunk: single chunk, still converges.
    def test_store_smaller_than_chunk(self, store_partition_factory):
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [("k1", "v1"), ("k2", "v2")])
        partition.close()

        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=timedelta(days=7),
                legacy_backfill_chunk_size=1000,
            ),
        )
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=timedelta(days=1))

        decoded = _decode_default_cf(partition)
        legacy = {k: v for k, v in decoded.items() if b"knew" not in k}
        assert len(legacy) == 2
        for expires_at, _ in legacy.values():
            assert expires_at == ts + 7 * DAY_MS
        partition.close()

    # Case 5 — empty store: no chunks, in-batch flip only.
    def test_empty_store_small_chunk(self, store_partition_factory):
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=timedelta(days=7),
                legacy_backfill_chunk_size=10,
            ),
        )
        ts = 5_000
        with partition.begin() as tx:
            tx.set(key="k1", value="v1", prefix=b"pfx", timestamp=ts, ttl=timedelta(days=1))

        assert partition.uses_ttl_stamps is True
        decoded = _decode_default_cf(partition)
        assert len(decoded) == 1
        ((expires_at, _),) = decoded.values()
        assert expires_at == ts + DAY_MS
        partition.close()

    # Case 2 — crash before flag: re-run converges, no double-wrap.
    def test_crash_before_flag_reruns_and_converges(self, store_partition_factory):
        m = 350
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [(f"k{i}", f"v{i}") for i in range(m)])
        partition.close()

        chunk_size = 100
        ttl = timedelta(days=7)

        # First (interrupted) run: let the chunk loop commit a few chunks, then
        # raise BEFORE the flip metadata batch lands. We inject the crash by
        # patching the partition's raw writer to raise after 2 chunk writes.
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
                raise RuntimeError("simulated OOM-kill mid-backfill")
            return real_write(batch)

        partition._write = crashing_write

        ts = 1_000_000_000_000
        with pytest.raises(RuntimeError, match="simulated OOM-kill"):
            with partition.begin() as tx:
                tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=ttl)

        # The flag was never written; some chunks committed. Restore the raw
        # writer and confirm the partition is still legacy on a fresh open.
        partition._write = real_write
        partition.close()

        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        # Still legacy: flip metadata never landed.
        assert partition.uses_ttl_stamps is False
        meta = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert meta.get(TTL_ENABLED_KEY) is None
        del meta

        # Re-run: a fresh ttl= write triggers the backfill again. It re-reads
        # the already-stamped chunks and converges to the COMPLETING run's
        # uniform expiry, with no double-wrap.
        ts2 = ts + 5_000  # high-water advanced, so expiry differs from run 1
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts2, ttl=ttl)

        assert partition.uses_ttl_stamps is True
        decoded = _decode_default_cf(partition)
        legacy = {k: v for k, v in decoded.items() if b"knew" not in k}
        assert len(legacy) == m
        expected = ts2 + 7 * DAY_MS
        for expires_at, payload in legacy.values():
            assert expires_at == expected
            # Single decode yields the original serialized JSON payload (no
            # nested stamp). A double-wrap would leave an 8-byte stamp prefix
            # in front of the JSON instead of a clean ``"v..."`` string.
            assert payload.startswith(b'"v')
        # Index is consistent: one entry per legacy key at the uniform expiry.
        index = _decode_index_cf(partition)
        for key in legacy:
            assert index[key] == expected
        partition.close()

    # Case 3 — mixed already-stamped (prior run) + legacy, no double-wrap.
    def test_mixed_stamped_and_legacy_restamp(self, store_partition_factory):
        # Build a store by hand on a SINGLE open partition (no reopen, so no
        # Windows lock-release timing): some default-CF values already stamped
        # with a prior expiry E_prior, some stamped at E_now (skip target), some
        # plain legacy. Then run the backfill with current expiry E_now.
        ts = 1_000_000_000_000
        e_now = ts + 7 * DAY_MS
        e_prior = ts + 3 * DAY_MS

        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=timedelta(days=7),
                legacy_backfill_chunk_size=2,
            ),
        )
        default_cf = partition.get_or_create_column_family("default")
        # Plain legacy values (no stamp).
        default_cf[b"pfx|legacy1"] = b'"L1"'
        default_cf[b"pfx|legacy2"] = b'"L2"'
        # Prior-run stamped value (different expiry) -> must re-stamp to e_now.
        default_cf[b"pfx|prior1"] = encode_ttl_value(e_prior, b'"P1"')
        # Already at target expiry -> must be skipped (no-op).
        default_cf[b"pfx|attarget"] = encode_ttl_value(e_now, b'"AT"')
        # Pre-create the prior-run index entries: the stale e_prior pointer for
        # prior1 (must be cleaned), and the already-correct e_now pointer for
        # attarget (a skip must leave its existing index entry intact — the
        # backfill does not re-emit index entries for skipped records).
        index_cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
        index_cf[encode_index_key(e_prior, b"pfx|prior1")] = b""
        index_cf[encode_index_key(e_now, b"pfx|attarget")] = b""

        # Drive the backfill directly with a known expires_at_ms = e_now.
        restamped = partition.backfill_legacy_records(
            expires_at_ms=e_now,
            changelog_producer=None,
            processed_offsets=None,
            staged_default_keys=set(),
            chunk_size=2,
        )

        decoded = _decode_default_cf(partition)
        # legacy1, legacy2, prior1 re-stamped; attarget skipped -> 3 restamped.
        assert restamped == 3
        assert decoded[b"pfx|legacy1"] == (e_now, b'"L1"')
        assert decoded[b"pfx|legacy2"] == (e_now, b'"L2"')
        # prior1 re-stamped from the STRIPPED payload -> single decode == "P1".
        assert decoded[b"pfx|prior1"] == (e_now, b'"P1"')
        # attarget left exactly as it was (single decode, original payload).
        assert decoded[b"pfx|attarget"] == (e_now, b'"AT"')

        # Index: stale e_prior entry deleted, every key now at e_now.
        index = _decode_index_cf(partition)
        assert index == {
            b"pfx|legacy1": e_now,
            b"pfx|legacy2": e_now,
            b"pfx|prior1": e_now,
            b"pfx|attarget": e_now,
        }
        partition.close()

    # Case 4 — per-chunk changelog production rebuilds an identical store.
    def test_per_chunk_changelog_rebuilds_identical(
        self, store_partition_factory, changelog_producer_mock
    ):
        m = 250
        chunk_size = 50
        legacy_ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="src",
            options=RocksDBOptions(
                legacy_records_ttl=legacy_ttl,
                legacy_backfill_chunk_size=chunk_size,
            ),
            changelog_producer=changelog_producer_mock,
        )
        _seed_legacy_records(partition, [(f"k{i}", f"v{i}") for i in range(m)])
        changelog_producer_mock.produce.reset_mock()

        ts = 1_000_000_000_000
        tx = partition.begin()
        tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=legacy_ttl)
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=0)
        source_default = _decode_default_cf(partition)

        # The producer was flushed once per non-empty chunk during the loop.
        assert changelog_producer_mock.flush.call_count >= m // chunk_size

        msgs = _capture_default_cf_changelog(changelog_producer_mock)
        # Every pre-existing key reached the changelog (option a durability).
        produced_keys = {k for k, _ in msgs}
        assert produced_keys >= set(source_default.keys())
        partition.close()

        # Replay into a fresh partition with a wallclock strictly before every
        # stamp -> identical stamped store (exercises wallclock recovery).
        recovered = store_partition_factory(name="dst")
        recovered._now_ms = lambda: ts - DAY_MS  # noqa: E731
        offset = 0
        for key, value in msgs:
            recovered.recover_from_changelog_message(
                key=key, value=value, cf_name="default", offset=offset
            )
            offset += 1

        recovered_default = _decode_default_cf(recovered)
        assert recovered_default == source_default
        recovered_index = _decode_index_cf(recovered)
        for key, (expires_at, _) in source_default.items():
            if expires_at != SENTINEL_NEVER:
                assert recovered_index.get(key) == expires_at
        recovered.close()
