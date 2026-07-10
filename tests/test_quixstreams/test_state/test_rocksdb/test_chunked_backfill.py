"""
Unit tests for the chunked legacy-records backfill:

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
    TTL_BACKFILL_PROGRESS_KEY,
    TTL_ENABLED_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
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


def _decode_index_cf(partition):
    """Return ``{user_key: expires_at}`` for the ``__ttl_index__`` CF."""
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _capture_default_cf_changelog(changelog_producer_mock):
    """Return ``(key, value, ttl_stamped)`` default-CF changelog messages,
    carrying the ``__ttl_stamped__`` header bit."""
    from quixstreams.state.metadata import CHANGELOG_TTL_STAMPED_HEADER

    msgs = []
    for call in changelog_producer_mock.produce.call_args_list:
        headers = call.kwargs["headers"]
        if headers[CHANGELOG_CF_MESSAGE_HEADER] == "default":
            ttl_stamped = bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER))
            msgs.append((call.kwargs["key"], call.kwargs["value"], ttl_stamped))
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
        assert (
            RocksDBOptions(legacy_backfill_chunk_size=100).legacy_backfill_chunk_size
            == 100
        )


# ---------------------------------------------------------------------------
# Chunked backfill behavior
# ---------------------------------------------------------------------------


class TestChunkedBackfill:
    # Store >> chunk size completes fully, one _write per chunk.
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

    # Store smaller than one chunk: single chunk, still converges.
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
            tx.set(
                key="knew",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        decoded = _decode_default_cf(partition)
        legacy = {k: v for k, v in decoded.items() if b"knew" not in k}
        assert len(legacy) == 2
        for expires_at, _ in legacy.values():
            assert expires_at == ts + 7 * DAY_MS
        partition.close()

    # Empty store: no chunks, in-batch flip only.
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
            tx.set(
                key="k1", value="v1", prefix=b"pfx", timestamp=ts, ttl=timedelta(days=1)
            )

        assert partition.uses_ttl_stamps is True
        decoded = _decode_default_cf(partition)
        assert len(decoded) == 1
        ((expires_at, _),) = decoded.values()
        assert expires_at == ts + DAY_MS
        partition.close()

    # Crash before flag: cursor-resumed re-run converges, no double-wrap.
    #
    # The re-run re-censuses the identical sorted key list and resumes at the
    # persisted cursor. Keys
    # stamped by the interrupted run are NOT re-read (cursor-skipped), so they
    # keep that run's expiry; the remaining keys are stamped by the completing
    # run. Both runs use ``encode_ttl_value`` wrap-whole, so every key is
    # stamped EXACTLY ONCE (no double-wrap) regardless of which run touched it.
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

        # The flag was never written; 2 chunks (200 keys) committed and the
        # cursor advanced with them. Restore the raw writer and reopen.
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
        # The cursor persisted the 200 keys stamped by the interrupted run.
        assert int_from_bytes(meta.get(TTL_BACKFILL_PROGRESS_KEY)) == 200
        del meta

        # Re-run: a fresh ttl= write triggers the backfill again. It re-censuses
        # the identical sorted list, skips the first 200 keys via the cursor,
        # and stamps the remaining 150 with the COMPLETING run's expiry.
        ts2 = ts + 5_000  # high-water advanced, so expiry differs from run 1
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts2, ttl=ttl)

        assert partition.uses_ttl_stamps is True
        decoded = _decode_default_cf(partition)
        legacy = {k: v for k, v in decoded.items() if b"knew" not in k}
        # Every legacy key is present and stamped EXACTLY ONCE (no double-wrap).
        assert len(legacy) == m
        run1_expiry = ts + 7 * DAY_MS
        run2_expiry = ts2 + 7 * DAY_MS
        for expires_at, payload in legacy.values():
            # Each key carries one of the two run expiries — never a nested
            # stamp. A double-wrap would leave an 8-byte prefix in front of the
            # JSON instead of a clean ``"v..."`` string.
            assert expires_at in (run1_expiry, run2_expiry)
            assert payload.startswith(b'"v')
        # 200 keys kept run 1's expiry; 150 got run 2's (cursor split).
        assert sum(1 for e, _ in legacy.values() if e == run1_expiry) == 200
        assert sum(1 for e, _ in legacy.values() if e == run2_expiry) == 150
        # Index is consistent: exactly one entry per legacy key at its expiry
        # (no stale pointers — cursor-skipped keys were never re-stamped, so no
        # double index entry exists). The in-batch ``knew`` key adds one more.
        index = _decode_index_cf(partition)
        assert len(index) == m + 1
        for key, (expires_at, _) in legacy.items():
            assert index[key] == expires_at
        partition.close()

    # First-run wrap-whole over a fresh legacy store (no inference).
    #
    # The byte-sniffing already-stamped recognizer is retired from the backfill
    # path: on a first run (cursor=0) every value is genuine legacy and is
    # wrapped whole exactly once. This drives
    # ``backfill_legacy_records`` directly to assert that contract.
    def test_first_run_wraps_every_value_whole(self, store_partition_factory):
        e_now = 1_000_000_000_000 + 7 * DAY_MS

        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=timedelta(days=7),
                legacy_backfill_chunk_size=2,
            ),
        )
        default_cf = partition.get_or_create_column_family("default")
        default_cf[b"pfx|legacy1"] = b'"L1"'
        default_cf[b"pfx|legacy2"] = b'"L2"'
        default_cf[b"pfx|legacy3"] = b'"L3"'

        # Drive the backfill directly with a known expires_at_ms = e_now.
        restamped = partition.backfill_legacy_records(
            expires_at_ms=e_now,
            changelog_producer=None,
            processed_offsets=None,
            staged_default_keys=set(),
            chunk_size=2,
        )
        assert restamped == 3

        decoded = _decode_default_cf(partition)
        assert decoded[b"pfx|legacy1"] == (e_now, b'"L1"')
        assert decoded[b"pfx|legacy2"] == (e_now, b'"L2"')
        assert decoded[b"pfx|legacy3"] == (e_now, b'"L3"')

        # One index entry per key at the uniform expiry.
        index = _decode_index_cf(partition)
        assert index == {
            b"pfx|legacy1": e_now,
            b"pfx|legacy2": e_now,
            b"pfx|legacy3": e_now,
        }
        partition.close()

    # Per-chunk changelog production rebuilds an identical store.
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
        produced_keys = {k for k, _, _ in msgs}
        assert produced_keys >= set(source_default.keys())
        partition.close()

        # Replay into a fresh partition with a wallclock strictly before every
        # stamp -> identical stamped store (exercises wallclock recovery).
        recovered = store_partition_factory(name="dst")
        recovered._now_ms = lambda: ts - DAY_MS  # noqa: E731
        offset = 0
        for key, value, ttl_stamped in msgs:
            recovered.recover_from_changelog_message(
                key=key,
                value=value,
                cf_name="default",
                offset=offset,
                ttl_stamped=ttl_stamped,
            )
            offset += 1

        recovered_default = _decode_default_cf(recovered)
        assert recovered_default == source_default
        recovered_index = _decode_index_cf(recovered)
        for key, (expires_at, _) in source_default.items():
            if expires_at != SENTINEL_NEVER:
                assert recovered_index.get(key) == expires_at
        recovered.close()
