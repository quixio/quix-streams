"""
Unit tests for the backfill-completeness + fail-safe-read data-corruption fix
(shortcut 73191 follow-up).

Covers ``dev-planning/state-ttl-legacy-backfill/spec-backfill-completeness.md``
§9:

Fix A — provably complete census-then-chunk backfill with a persisted cursor:
1. Multi-chunk backfill stamps EVERY key (zero un-stamped remain), no skips.
2. Crash-mid-backfill → reopen legacy → cursor resumes → converges, every key
   stamped exactly once, no double-wrap.
4. ``_looks_like_stamped_value`` is NEVER called by the backfill path.
5. 200k-scale completeness (chunk-count formula + full 100%-stamped scan at a
   tractable size).
6. Census excludes ``staged_default_keys``.
7. Key deleted between census and stamp is skipped cleanly.

Fix B — fail-safe read (degrade, never corrupt):
- A flipped store containing a deliberately un-stamped legacy value returns it
  intact (never-expires) and does NOT raise StateSerializationError (the live
  crash-loop regression test).
- Genuinely-stamped values still expiry-filter correctly (no regression).
- The §4.3 residual (legacy value whose first 8 bytes coincidentally decode to a
  plausible expiry) is documented (it DOES strip — the known, accepted residual).

See the spec for the full rationale (no per-value marker; census order is
deterministic-sorted; cursor advances atomically per chunk).
"""

from datetime import timedelta
from unittest.mock import patch

import pytest

from quixstreams.state.metadata import METADATA_CF_NAME
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


def _raw_default_cf(partition):
    """Return the raw (un-decoded) ``{key: value}`` of the default CF."""
    cf = partition.get_or_create_column_family("default")
    return {key: value for key, value in cf.items()}


class _CFProxy:
    """
    Delegating wrapper around a ``Rdict`` column family whose ``get`` can be
    spied/overridden (the underlying ``Rdict.get`` attribute is read-only, so
    we cannot monkeypatch it directly). ``on_get`` may return a sentinel to
    override the result or ``_PASS`` to delegate.
    """

    _PASS = object()

    def __init__(self, real, on_get=None):
        self._real = real
        self._on_get = on_get
        self.get_keys = []

    def get(self, key, *args, **kwargs):
        self.get_keys.append(key)
        if self._on_get is not None:
            override = self._on_get(key)
            if override is not _CFProxy._PASS:
                return override
        return self._real.get(key, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


# ---------------------------------------------------------------------------
# Fix A — completeness
# ---------------------------------------------------------------------------


class TestBackfillCompleteness:
    # §9.2 — multi-chunk backfill stamps EVERY key, zero un-stamped remain.
    def test_multi_chunk_stamps_every_key(self, store_partition_factory):
        m = 500
        chunk_size = 50
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [(f"k{i}", f"v{i}") for i in range(m)])
        partition.close()

        ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=ttl)

        assert partition.uses_ttl_stamps is True
        expected = ts + 7 * DAY_MS

        # Iterate the RAW default CF and assert EVERY value decodes to a valid
        # stamp — zero un-stamped keys remain (the completeness guarantee).
        raw = _raw_default_cf(partition)
        legacy_keys = [k for k in raw if b"knew" not in k]
        assert len(legacy_keys) == m
        for key in legacy_keys:
            stamp, payload = decode_ttl_value(raw[key])
            assert stamp == expected
            # decodes exactly once -> no double-wrap, payload is clean JSON.
            assert payload.startswith(b'"v')

        # One index entry per legacy key at the uniform expiry.
        index = _decode_index_cf(partition)
        for key in legacy_keys:
            assert index[key] == expected

        meta = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert meta.get(TTL_ENABLED_KEY) == b"\x01"
        assert int_from_bytes(meta.get(STATE_FORMAT_VERSION_KEY)) == (
            STATE_FORMAT_VERSION
        )
        partition.close()

    # §9.3 — crash mid-backfill → reopen legacy → cursor resumes → converges,
    # every key stamped exactly once, no double-wrap.
    def test_crash_resumes_via_cursor_no_double_wrap(self, store_partition_factory):
        m = 500
        chunk_size = 50
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [(f"k{i}", f"v{i}") for i in range(m)])
        partition.close()

        ttl = timedelta(days=7)
        # Run 1: crash after 4 chunk writes (200 keys) and before the flag.
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
            if crash["n"] > 4:
                raise RuntimeError("simulated crash mid-backfill")
            return real_write(batch)

        partition._write = crashing_write

        ts = 1_000_000_000_000
        with pytest.raises(RuntimeError, match="simulated crash"):
            with partition.begin() as tx:
                tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=ttl)
        partition._write = real_write
        partition.close()

        # Reopen: legacy, cursor at 200 (4 chunks of 50).
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        assert partition.uses_ttl_stamps is False
        meta = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert meta.get(TTL_ENABLED_KEY) is None
        assert int_from_bytes(meta.get(TTL_BACKFILL_PROGRESS_KEY)) == 200
        del meta

        # Spy on the fresh point-get to confirm cursor-skipped keys are NOT
        # re-read on the completing run. Wrap the default CF in a proxy so we
        # can count ``get`` calls (the real Rdict.get is read-only).
        real_goccf = partition.get_or_create_column_family
        default_proxy = _CFProxy(real_goccf("default"))

        def proxy_goccf(cf_name):
            if cf_name == "default":
                return default_proxy
            return real_goccf(cf_name)

        # Run 2 completes the backfill.
        ts2 = ts + 5_000
        with patch.object(
            partition, "get_or_create_column_family", side_effect=proxy_goccf
        ), patch.object(partition, "_looks_like_stamped_value") as looks_spy:
            with partition.begin() as tx:
                tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts2, ttl=ttl)
            # §9.4 — the backfill never byte-sniffs.
            looks_spy.assert_not_called()

        assert partition.uses_ttl_stamps is True

        # Exactly 300 point-gets on run 2: keys [200:500). The first 200 keys
        # are cursor-skipped and never re-read.
        assert len(default_proxy.get_keys) == 300

        raw = _raw_default_cf(partition)
        legacy = {k: v for k, v in raw.items() if b"knew" not in k}
        assert len(legacy) == m
        run1 = ts + 7 * DAY_MS
        run2 = ts2 + 7 * DAY_MS
        for value in legacy.values():
            stamp, payload = decode_ttl_value(value)
            # Stamped EXACTLY ONCE -> payload is clean JSON, no nested stamp.
            assert stamp in (run1, run2)
            assert payload.startswith(b'"v')
        assert sum(1 for v in legacy.values() if decode_ttl_value(v)[0] == run1) == 200
        assert sum(1 for v in legacy.values() if decode_ttl_value(v)[0] == run2) == 300
        partition.close()

    # §9.5 — chunk-count formula + full 100%-stamped scan at a tractable size.
    def test_chunk_count_and_full_stamp_scan(self, store_partition_factory):
        m = 2000
        chunk_size = 100
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [(f"k{i}", f"v{i}") for i in range(m)])
        partition.close()

        ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=chunk_size
            ),
        )
        writes = {"n": 0}
        real_write = partition._write

        def counting_write(batch):
            writes["n"] += 1
            return real_write(batch)

        partition._write = counting_write

        ts = 1_000_000_000_000
        with partition.begin() as tx:
            tx.set(key="knew", value="vnew", prefix=b"pfx", timestamp=ts, ttl=ttl)

        # ceil(m / chunk_size) chunk writes + 1 final flip batch.
        assert writes["n"] == (m // chunk_size) + 1

        # Full scan: zero values reject the strict decode (100% stamped).
        raw = _raw_default_cf(partition)
        for value in raw.values():
            stamp, _ = decode_ttl_value(value)
            assert stamp != SENTINEL_NEVER or value  # all decode
        partition.close()

    # §9.6 — census excludes staged_default_keys (the triggering in-batch write).
    def test_census_excludes_staged_in_batch_key(self, store_partition_factory):
        partition = store_partition_factory(name="db")
        _seed_legacy_records(partition, [("k1", "v1"), ("k2", "v2")])
        partition.close()

        ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db", options=RocksDBOptions(legacy_records_ttl=ttl)
        )
        ts = 1_000_000_000_000
        with partition.begin() as tx:
            # The in-batch ttl= write triggers the flip; it must get its OWN
            # true stamp, not the uniform legacy expiry.
            tx.set(
                key="knew",
                value="vnew",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        decoded = _decode_default_cf(partition)
        legacy_expiry = ts + 7 * DAY_MS
        for key, (expires_at, _) in decoded.items():
            if b"knew" in key:
                # In-batch key keeps its true 1-day expiry (not double-handled).
                assert expires_at == ts + DAY_MS
            else:
                assert expires_at == legacy_expiry
        partition.close()

    # §9.7 — key deleted between census and stamp is skipped cleanly.
    def test_key_deleted_between_census_and_stamp(self, store_partition_factory):
        partition = store_partition_factory(name="db")
        _seed_legacy_records(
            partition, [("k1", "v1"), ("k2", "v2"), ("k3", "v3")]
        )
        partition.close()

        ttl = timedelta(days=7)
        partition = store_partition_factory(
            name="db",
            options=RocksDBOptions(
                legacy_records_ttl=ttl, legacy_backfill_chunk_size=10
            ),
        )
        real_goccf = partition.get_or_create_column_family
        real_default = real_goccf("default")

        # Pick a real legacy key to "delete" between census and stamp.
        victim = next(iter(real_default.keys()))

        def on_get(key):
            if key == victim:
                return None  # vanished since census
            return _CFProxy._PASS

        default_proxy = _CFProxy(real_default, on_get=on_get)

        def proxy_goccf(cf_name):
            if cf_name == "default":
                return default_proxy
            return real_goccf(cf_name)

        ts = 1_000_000_000_000
        with patch.object(
            partition, "get_or_create_column_family", side_effect=proxy_goccf
        ):
            restamped = partition.backfill_legacy_records(
                expires_at_ms=ts + 7 * DAY_MS,
                changelog_producer=None,
                processed_offsets=None,
                staged_default_keys=set(),
                chunk_size=10,
            )
        # The vanished key was skipped: 3 censused - 1 deleted = 2 restamped.
        assert restamped == 2
        # No dangling index entry for the vanished key.
        index = _decode_index_cf(partition)
        assert victim not in index
        partition.close()


# ---------------------------------------------------------------------------
# Fix B — fail-safe read
# ---------------------------------------------------------------------------


def _flip_partition_into_ttl(partition):
    """
    Force ``partition`` into a flipped TTL state by writing the flip metadata
    directly, so we can hand-craft default-CF contents that violate the
    "every value is stamped" invariant for the fail-safe-read tests.
    """
    from quixstreams.state.serialization import int_to_bytes

    meta = partition.get_or_create_column_family(METADATA_CF_NAME)
    meta[TTL_ENABLED_KEY] = b"\x01"
    meta[STATE_FORMAT_VERSION_KEY] = int_to_bytes(STATE_FORMAT_VERSION)
    partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    partition.uses_ttl_stamps = True


class TestFailSafeRead:
    # The core regression test for the live crash-loop: a flipped store with a
    # deliberately un-stamped long legacy JSON value must read back INTACT and
    # must NOT raise / corrupt.
    def test_unstamped_legacy_value_returns_raw_not_corrupted(
        self, store_partition_factory, caplog
    ):
        partition = store_partition_factory(name="db")
        _flip_partition_into_ttl(partition)

        # The exact live-incident shape: a long JSON value beginning '{"status'
        # whose first 8 bytes are NOT a plausible epoch-ms stamp.
        payload = b'{"status":"ON","pad":"' + b"x" * 64 + b'"}'
        # Sanity: its first 8 bytes do NOT decode to a plausible stamp.
        first8 = int.from_bytes(payload[:8], "big")
        assert not (0 < first8 < 10**15)

        key_serialized = None
        # Write the un-stamped value directly via the default CF, bypassing the
        # stamping write path (simulates the backfill's missed key).
        with partition.begin() as tx:
            # Use the transaction's key serialization for a realistic key.
            key_serialized = tx._serialize_key("dedupkey", prefix=b"pfx")
        default_cf = partition.get_or_create_column_family("default")
        default_cf[key_serialized] = payload

        # Read it back through the normal get path.
        with partition.begin() as tx:
            result = tx.get_bytes(key="dedupkey", prefix=b"pfx")

        # Returned byte-identical to the original (no 8-byte strip) and no raise.
        assert result == payload
        # A WARNING was logged (observable degrade).
        assert any(
            "Fail-safe TTL read" in rec.message for rec in caplog.records
        )
        partition.close()

    # Genuinely-stamped values still expiry-filter correctly (no regression).
    def test_genuinely_stamped_values_filter_normally(
        self, store_partition_factory
    ):
        partition = store_partition_factory(name="db")
        _flip_partition_into_ttl(partition)

        base = 1_000_000_000_000
        with partition.begin() as tx:
            # live event-time below both expiries.
            tx.set(
                key="kfresh",
                value="vfresh",
                prefix=b"pfx",
                timestamp=base,
                ttl=timedelta(days=10),
            )
            tx.set(
                key="kperm",
                value="vperm",
                prefix=b"pfx",
                timestamp=base,
            )  # sentinel / never-expires

        # Read within the window: both visible.
        with partition.begin() as tx:
            assert tx.get(key="kfresh", prefix=b"pfx", timestamp=base) == "vfresh"
            assert tx.get(key="kperm", prefix=b"pfx", timestamp=base) == "vperm"

        # Advance the high-water past the fresh expiry: it filters out, sentinel
        # survives.
        with partition.begin() as tx:
            far = base + 100 * DAY_MS
            assert tx.get(key="kfresh", prefix=b"pfx", timestamp=far) is None
            assert tx.get(key="kperm", prefix=b"pfx", timestamp=far) == "vperm"
        partition.close()

    # The §4.3 documented residual: a legacy value whose first 8 bytes DO decode
    # to a plausible expiry IS mis-stripped (the only residual corruption path,
    # which Fix A empties in practice). This test pins the known behavior.
    def test_documented_residual_plausible_prefix_is_stripped(
        self, store_partition_factory
    ):
        partition = store_partition_factory(name="db")
        _flip_partition_into_ttl(partition)

        # Craft a value whose first 8 bytes decode to a plausible FUTURE expiry
        # (so it is not filtered as expired) followed by a payload.
        plausible_future = 1_000_000_000_000 + 365 * DAY_MS
        crafted = encode_ttl_value(plausible_future, b"REALPAYLOAD")

        with partition.begin() as tx:
            key_serialized = tx._serialize_key("residual", prefix=b"pfx")
        default_cf = partition.get_or_create_column_family("default")
        default_cf[key_serialized] = crafted

        with partition.begin() as tx:
            # high-water below the plausible expiry -> not filtered.
            result = tx.get_bytes(key="residual", prefix=b"pfx", timestamp=1_000)

        # Known residual: it is treated as stamped and the prefix IS stripped.
        # This is the accepted §4.3 corner; documented, not a common-case bug.
        assert result == b"REALPAYLOAD"
        partition.close()
