"""
Red-first tests for the AUTOMATIC regime-split v3.24.0-stamp adoption feature.

Replaces the opt-in ``adopt_v3240_stamps`` flag with two regimes:

- **Warm restart** (local store intact with v3.24.0 TTL artifacts):
  Deterministic adopt in place — no heuristic, no backup, no CRITICAL dead-end.
  Requires ``_enforce_format_version`` to accept v2 (or upgrade from v1/absent).

- **Cold rebuild / fresh volume / memory** (only raw stamped bytes):
  Provisional auto-adopt with backup CF, sweep suppression, and corroboration.
  Rollback via ``QUIXSTREAMS_STATE_TTL_ROLLBACK=1`` env var.

Every test is expected RED on HEAD (``323d391a``); the feature is unbuilt.

Spec: ``dev-planning/state-ttl-v3240-auto-adopt/spec.md``
"""

import logging
import os
import struct
from typing import Optional, cast
from unittest.mock import MagicMock, PropertyMock

import pytest
from rocksdict import WriteBatch

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    METADATA_CF_NAME,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
    Marker,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.exceptions import IncompatibleStateStoreError
from quixstreams.state.rocksdb.metadata import (
    STATE_FORMAT_VERSION,
    STATE_FORMAT_VERSION_KEY,
    TTL_ENABLED_KEY,
    TTL_HIGH_WATER_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import (
    encode_index_key,
    encode_ttl_value,
)
from quixstreams.state.serialization import int_to_bytes
from quixstreams.utils.json import dumps as json_dumps

DAY_MS = 86_400_000
NOW_MS = 1_780_000_000_000

# -- spec §4: new metadata keys / CFs expected after the feature lands ----------
# These constants will exist after ArchDev implements the feature. Import
# failures from production code are caught per-test where needed so the file
# itself still loads on HEAD (the feature is unbuilt).
TTL_ADOPT_BACKUP_CF_NAME = "__ttl_adopt_backup__"
TTL_ADOPT_PENDING_KEY = b"__ttl_adopt_pending__"


# ---------------------------------------------------------------------------
# Helpers (reused across scenarios, adapted from test_v3240_adoption_opt_in.py)
# ---------------------------------------------------------------------------


def _make_producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _rocksdb_partition(
    tmp_path,
    name="db",
    options=None,
    changelog_producer=None,
):
    path = (tmp_path / name).as_posix()
    opts = options or RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    return RocksDBStorePartition(
        path, options=opts, changelog_producer=changelog_producer
    )


def _replay_default(partition, msgs, now_ms=NOW_MS):
    """Replay header-absent ``(raw_key, value, ttl_stamped)`` default-CF messages."""
    partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value, ttl_stamped in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name="default",
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


def _v3240_msg(key_str, user_value, expiry_ms, prefix=b"pfx"):
    """One v3.24.0-style default-CF changelog message: 8-byte-stamped value
    with NO ``__ttl_stamped__`` header (the shape v3.24.0 wrote)."""
    raw_key = prefix + b"|" + json_dumps(key_str)
    stamped = encode_ttl_value(expiry_ms, json_dumps(user_value))
    return (raw_key, stamped, False)


def _pending_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


def _index_count(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    return sum(1 for _ in cf.keys())


def _raw_default_get(partition, raw_key):
    """Byte-exact on-disk default-CF value (no strip, no deserialize)."""
    return partition.get(raw_key, cf_name="default")


def _read_via_tx(partition, key, prefix=b"pfx", timestamp=None):
    tx = partition.begin()
    return tx.get(key=key, prefix=prefix, cf_name="default", timestamp=timestamp)


def _read_bytes_via_tx(partition, raw_key_str, prefix=b"pfx", timestamp=None):
    """User read path returning raw bytes (strips the stamp iff the partition is
    flipped)."""
    tx = partition.begin()
    return tx.get_bytes(
        key=raw_key_str, prefix=prefix, cf_name="default", timestamp=timestamp
    )


def _seed_warm_ttl_store(
    tmp_path,
    name,
    *,
    format_version: Optional[int] = STATE_FORMAT_VERSION,
    write_enabled_flag: bool = True,
    write_index_cf: bool = True,
    write_high_water: bool = True,
    n_records: int = 4,
    expiry_ms: int = NOW_MS + 7 * DAY_MS,
):
    """Build a LOCAL store that looks like a v3.24.0 TTL preview's warm artifacts.

    This directly writes the TTL metadata markers (``__ttl_enabled__``,
    ``__ttl_format_version__``, ``__ttl_high_water_ms__``) and optionally the
    ``__ttl_index__`` CF into a fresh RocksDB partition, then closes it.
    Reopening the partition simulates a warm restart of a v3.24.0 preview store.

    Returns the dict of ``{raw_key: stamped_value}`` for the seeded records.
    """
    path = (tmp_path / name).as_posix()
    opts = RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0)
    partition = RocksDBStorePartition(path, options=opts, changelog_producer=None)

    # Write stamped default-CF records manually.
    records = {}
    default_cf = partition.get_or_create_column_family("default")
    for i in range(n_records):
        raw_key = b"pfx|" + json_dumps(f"k{i}")
        stamped = encode_ttl_value(expiry_ms, json_dumps(f"v{i}"))
        default_cf[raw_key] = stamped
        records[raw_key] = stamped

    # Write TTL metadata markers.
    metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
    if write_enabled_flag:
        metadata_cf[TTL_ENABLED_KEY] = b"\x01"
    if format_version is not None:
        metadata_cf[STATE_FORMAT_VERSION_KEY] = int_to_bytes(format_version)
    if write_high_water:
        # High-water is the max EVENT-time position, which is BEFORE the records'
        # expiry (expiry = event_time + ttl). Seed it at NOW_MS (< expiry_ms) so the
        # warm-restored records read back alive; seeding it at expiry_ms would make
        # every record read as exactly-expired (stamp <= high_water).
        metadata_cf[TTL_HIGH_WATER_KEY] = int_to_bytes(NOW_MS)

    # Create the __ttl_index__ CF with index entries.
    if write_index_cf:
        index_cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
        for raw_key in records:
            index_cf[encode_index_key(expiry_ms, raw_key)] = b""

    partition.close()
    return records


def _replay_memory(partition, msgs, now_ms=None):
    """Replay ``(key, value, cf_name, ttl_stamped)`` into a memory partition."""
    if now_ms is not None:
        partition._now_ms = lambda: now_ms  # noqa: E731
    offset = 0
    for key, value, cf_name, ttl_stamped in msgs:
        partition.recover_from_changelog_message(
            key=key,
            value=value,
            cf_name=cf_name,
            offset=offset,
            ttl_stamped=ttl_stamped,
        )
        offset += 1


def _replay_memory_default(partition, msgs, now_ms=None):
    """Replay ``(key, value, ttl_stamped)`` default-CF messages to memory."""
    full = [(k, v, "default", s) for k, v, s in msgs]
    _replay_memory(partition, full, now_ms=now_ms)


def _read_memory_tx(partition, key, prefix=b"pfx", timestamp=None):
    tx = partition.begin()
    return tx.get(key=key, prefix=prefix, cf_name="default", timestamp=timestamp)


# ===========================================================================
# Scenario 6: No parameter — adopt_v3240_stamps fully removed
# Spec §4: "DELETE adopt_v3240_stamps from all sites in §2.4"
# ===========================================================================


class TestAdoptV3240StampsRemoved:
    """Validates spec §4, §2.4: the ``adopt_v3240_stamps`` parameter is fully
    removed from ``RocksDBOptions``, ``RocksDBOptionsType``, and
    ``MemoryStorePartition``. Constructing with the old keyword must raise
    ``TypeError``.

    RED on HEAD: the parameter exists and construction succeeds.
    """

    def test_rocksdb_options_rejects_adopt_v3240_stamps(self):
        """Spec §4: RocksDBOptions must not accept adopt_v3240_stamps."""
        with pytest.raises(TypeError):
            RocksDBOptions(adopt_v3240_stamps=True)

    def test_rocksdb_options_has_no_adopt_field(self):
        """Spec §4: the dataclass field must not exist."""
        opts = RocksDBOptions()
        assert not hasattr(opts, "adopt_v3240_stamps"), (
            "RocksDBOptions still has adopt_v3240_stamps field — "
            "spec §4 requires its removal"
        )

    def test_memory_partition_rejects_adopt_v3240_stamps(self):
        """Spec §4: MemoryStorePartition must not accept adopt_v3240_stamps."""
        producer = _make_producer()
        with pytest.raises(TypeError):
            MemoryStorePartition(changelog_producer=producer, adopt_v3240_stamps=True)


# ===========================================================================
# Scenario 1: Warm restart, genuine v3.24.0 → deterministic adopt in place
# Spec §5.1: warm-deterministic adopt (sound; no heuristic, no backup)
# ===========================================================================


class TestWarmDeterministicAdopt:
    """Validates spec §5.1: a store carrying the v3.24.0 LOCAL artifacts
    (``__ttl_enabled__``, ``__ttl_format_version__``, ``__ttl_index__``) is
    recognized at open and upgraded IN PLACE.

    RED on HEAD: ``_enforce_format_version`` raises
    ``IncompatibleStateStoreError`` for format version < 2 or missing marker.
    """

    def test_warm_restart_v3240_format_v2_adopts_in_place(self, tmp_path, caplog):
        """A v3.24.0 preview store with format_version=2 and all TTL markers
        opens cleanly, is already flipped, reads strip stamps correctly, and
        needs no backup or pending marker.

        Spec §5.1 steps 1-4: flip (if not already), format upgrade, ensure
        index, produce done-marker.
        """
        expiry = NOW_MS + 7 * DAY_MS
        _seed_warm_ttl_store(tmp_path, "warm_v2", format_version=2, expiry_ms=expiry)

        producer = _make_producer()
        with caplog.at_level(logging.INFO):
            partition = _rocksdb_partition(
                tmp_path, name="warm_v2", changelog_producer=producer
            )

        # The partition must open FLIPPED without raising.
        assert partition.uses_ttl_stamps is True

        # Values read back STRIPPED to the original user payload.
        for i in range(4):
            val = _read_via_tx(partition, f"k{i}", timestamp=NOW_MS)
            assert val == f"v{i}", f"Warm key k{i} should read stripped"

        # Index present and populated.
        assert _index_count(partition) >= 4

        # No backup CF, no provisional marker (warm path is sound).
        cfs = partition.list_column_families()
        assert (
            TTL_ADOPT_BACKUP_CF_NAME not in cfs
        ), "Warm path must NOT create a backup CF"
        metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert (
            metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is None
        ), "Warm path must NOT set __ttl_adopt_pending__"

        # No CRITICAL "set the flag" message (the old dead-end).
        assert not any(
            r.levelno == logging.CRITICAL and "adopt_v3240_stamps" in r.getMessage()
            for r in caplog.records
        ), "Warm adopt must not log a CRITICAL naming the removed flag"

        partition.close()

    def test_warm_restart_v3240_format_v1_upgrades_no_reject(self, tmp_path, caplog):
        """Spec §5.1 step 2: a preview store with format_version=1 (or absent)
        opens without ``IncompatibleStateStoreError``, is upgraded to
        format_version=2 in place, and resumes TTL mode.

        RED on HEAD: ``_enforce_format_version`` raises
        ``IncompatibleStateStoreError`` on version < 2.
        """

        expiry = NOW_MS + 7 * DAY_MS
        _seed_warm_ttl_store(
            tmp_path,
            "warm_v1",
            format_version=1,  # older preview format
            expiry_ms=expiry,
        )

        producer = _make_producer()
        # On HEAD this raises IncompatibleStateStoreError. The feature must
        # make it succeed.
        partition = _rocksdb_partition(
            tmp_path, name="warm_v1", changelog_producer=producer
        )

        assert (
            partition.uses_ttl_stamps is True
        ), "Warm restart with format v1 must upgrade and flip, not reject"

        # Verify the marker was rewritten to STATE_FORMAT_VERSION (2).
        metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
        raw_version = metadata_cf.get(STATE_FORMAT_VERSION_KEY, default=None)
        assert raw_version is not None
        from quixstreams.state.serialization import int_from_bytes

        assert int_from_bytes(cast(bytes, raw_version)) == STATE_FORMAT_VERSION

        partition.close()

    def test_warm_restart_v3240_no_format_marker_upgrades(self, tmp_path):
        """Spec §5.1 step 2: a preview store with NO format version marker
        (``__ttl_format_version__`` absent) but ``__ttl_enabled__`` present
        upgrades in place instead of raising.

        RED on HEAD: ``_enforce_format_version`` raises
        ``IncompatibleStateStoreError`` on missing marker.
        """
        expiry = NOW_MS + 7 * DAY_MS
        _seed_warm_ttl_store(
            tmp_path,
            "warm_nofmt",
            format_version=None,  # no marker written
            expiry_ms=expiry,
        )

        producer = _make_producer()
        partition = _rocksdb_partition(
            tmp_path, name="warm_nofmt", changelog_producer=producer
        )

        assert (
            partition.uses_ttl_stamps is True
        ), "Warm restart with missing format marker must upgrade, not reject"
        partition.close()

    def test_warm_restart_index_cf_only_no_enabled_flag_adopts(self, tmp_path):
        """Spec §5.0: ``warm_ttl_store`` detects ``__ttl_index__`` CF even if
        ``__ttl_enabled__`` is absent. The partition should flip deterministically.

        RED on HEAD: no warm signal probe exists; the store opens legacy.
        """
        expiry = NOW_MS + 7 * DAY_MS
        _seed_warm_ttl_store(
            tmp_path,
            "warm_idx_only",
            format_version=2,
            write_enabled_flag=False,  # no __ttl_enabled__
            write_index_cf=True,
            write_high_water=False,
            expiry_ms=expiry,
        )

        producer = _make_producer()
        partition = _rocksdb_partition(
            tmp_path, name="warm_idx_only", changelog_producer=producer
        )

        assert partition.uses_ttl_stamps is True, (
            "Warm path must detect __ttl_index__ CF and flip even without "
            "__ttl_enabled__"
        )
        partition.close()


# ===========================================================================
# Scenario 2: Warm restart, genuine pre-TTL legacy (no TTL markers) → stays
# legacy. Spec §5.0: warm_ttl_store False → untouched.
# ===========================================================================


class TestWarmLegacyUntouched:
    """Validates spec §5.0: a genuine pre-TTL legacy store with NO ``__ttl_*``
    artifacts opens legacy and is never flipped.

    GREEN on HEAD: HEAD already opens a legacy store as legacy.
    """

    def test_warm_legacy_no_ttl_markers_stays_legacy(self, tmp_path):
        """A store with no TTL markers remains legacy, values byte-identical."""
        # Seed phase: write plain values with a partition, then close.
        originals = {}
        p1 = _rocksdb_partition(tmp_path, name="legacy")
        with p1.begin() as tx:
            for i in range(4):
                tx.set(key=f"k{i}", value=f"v{i}", prefix=b"pfx", timestamp=None)
        # Capture the raw on-disk values.
        default_cf = p1.get_or_create_column_family("default")
        for raw_key, value in default_cf.items():
            originals[bytes(raw_key)] = bytes(value)
        del default_cf
        p1.close()

        # Reopen — the warm restart.
        partition2 = _rocksdb_partition(tmp_path, name="legacy")
        assert partition2.uses_ttl_stamps is False, "Legacy store must stay legacy"
        # No TTL CFs or markers created.
        cfs = partition2.list_column_families()
        assert TTL_INDEX_CF_NAME not in cfs
        assert TTL_ADOPT_BACKUP_CF_NAME not in cfs

        # Values byte-identical.
        default_cf2 = partition2.get_or_create_column_family("default")
        for raw_key, expected in originals.items():
            assert bytes(default_cf2.get(raw_key)) == expected
        del default_cf2
        partition2.close()


# ===========================================================================
# Scenario 8: Format-version relaxation
# Spec §5.1 step 2: v2-format store no longer raises.
# ===========================================================================


class TestFormatVersionRelaxation:
    """Validates spec §5.1 step 2: ``_enforce_format_version`` is relaxed so
    a v2-format store (or v1 or absent) no longer raises
    ``IncompatibleStateStoreError`` — it upgrades in place.

    RED on HEAD for v1/absent: HEAD raises ``IncompatibleStateStoreError``.
    GREEN on HEAD for v2: HEAD already accepts v2.
    """

    def test_format_version_2_accepted_unchanged(self, tmp_path):
        """Already GREEN on HEAD — format v2 is current and accepted."""
        _seed_warm_ttl_store(tmp_path, "fmtv2", format_version=2)
        partition = _rocksdb_partition(tmp_path, name="fmtv2")
        assert partition.uses_ttl_stamps is True
        partition.close()

    def test_format_version_1_upgraded_not_rejected(self, tmp_path):
        """RED on HEAD: v1 raises IncompatibleStateStoreError."""
        _seed_warm_ttl_store(tmp_path, "fmtv1", format_version=1)
        # Must not raise.
        partition = _rocksdb_partition(tmp_path, name="fmtv1")
        assert partition.uses_ttl_stamps is True
        partition.close()

    def test_format_version_absent_upgraded_not_rejected(self, tmp_path):
        """RED on HEAD: absent marker raises IncompatibleStateStoreError."""
        _seed_warm_ttl_store(tmp_path, "fmtno", format_version=None)
        partition = _rocksdb_partition(tmp_path, name="fmtno")
        assert partition.uses_ttl_stamps is True
        partition.close()

    def test_unknown_lower_format_version_still_raises(self, tmp_path):
        """Finding #3: a sub-current marker that is NOT a recognized v3.24.0
        preview shape (below MIN_UPGRADEABLE_STATE_FORMAT_VERSION) must keep the
        forward-incompatibility guard and raise, not be silently upgraded.

        RED before the #3 narrowing (which upgraded ANY marker < current in
        place); GREEN after.
        """
        _seed_warm_ttl_store(tmp_path, "fmtv0", format_version=0)
        with pytest.raises(IncompatibleStateStoreError):
            _rocksdb_partition(tmp_path, name="fmtv0")


# ===========================================================================
# Scenario 3: Cold rebuild, genuine v3.24.0 (stamp-shaped, not-all-past) →
# provisional adopt.
# Spec §5.2: cold-heuristic provisional adopt + backup.
# ===========================================================================


class TestColdProvisionalAdopt:
    """Validates spec §5.2-5.4: a cold-rebuilt all-stamped (not-all-past)
    census triggers provisional adoption with backup, sweep suppression,
    and corroboration.

    RED on HEAD: HEAD requires ``adopt_v3240_stamps=True`` flag; without it
    the store stays legacy and logs a CRITICAL naming the flag. The spec
    requires automatic provisional adoption without any flag.
    """

    def test_cold_rebuild_v3240_provisional_adopt(self, tmp_path, caplog):
        """Spec §5.2: a header-absent all-stamped (>=1 future) census triggers
        automatic provisional adoption.

        Asserts: flipped, reads stripped, index built,
        ``__ttl_adopt_backup__`` populated, ``__ttl_adopt_pending__`` set,
        WARN (not CRITICAL).
        """
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]

        partition = _rocksdb_partition(
            tmp_path, name="cold_prov", changelog_producer=producer
        )
        with caplog.at_level(logging.WARNING):
            _replay_default(partition, msgs, now_ms=NOW_MS)
            partition.complete_recovery()

        # Flipped (provisional).
        assert (
            partition.uses_ttl_stamps is True
        ), "Cold auto-adopt must flip the store provisionally"

        # Values read back stripped.
        for i in range(4):
            val = _read_via_tx(partition, f"k{i}", timestamp=NOW_MS)
            assert val == f"v{i}", f"Cold-adopted key k{i} should read stripped"

        # Index built.
        assert _index_count(partition) == 4

        # Backup CF populated.
        cfs = partition.list_column_families()
        assert (
            TTL_ADOPT_BACKUP_CF_NAME in cfs
        ), "Cold provisional adopt must create __ttl_adopt_backup__"
        backup_cf = partition.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
        backup_count = sum(1 for _ in backup_cf.keys())
        assert (
            backup_count == 4
        ), f"Backup should hold all 4 originals, got {backup_count}"

        # Provisional pending marker set.
        metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert (
            metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None
        ), "Cold provisional adopt must set __ttl_adopt_pending__"

        # WARN (not CRITICAL) — spec §5.8 downgrades from CRITICAL.
        assert any(
            r.levelno == logging.WARNING and "auto-adopted" in r.getMessage().lower()
            for r in caplog.records
        ), "Cold provisional adopt must log WARN, not CRITICAL"
        assert not any(
            r.levelno == logging.CRITICAL and "adopt_v3240_stamps" in r.getMessage()
            for r in caplog.records
        ), "No CRITICAL naming the removed flag should be emitted"

        partition.close()

    def test_cold_rebuild_memory_provisional_adopt(self, caplog):
        """Spec §5.7: memory is ALWAYS cold. A header-absent all-stamped
        (>=1 future) census triggers the same provisional adopt (in-RAM
        backup + sweep suppression).

        RED on HEAD: memory requires ``adopt_v3240_stamps=True``.
        """
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]
        full = [(k, v, "default", s) for k, v, s in msgs]

        partition = MemoryStorePartition(changelog_producer=producer)
        with caplog.at_level(logging.WARNING):
            _replay_memory(partition, full, now_ms=NOW_MS)
            partition.complete_recovery()

        assert (
            partition.uses_ttl_stamps is True
        ), "Memory cold auto-adopt must flip the store"

        # No CRITICAL naming the removed flag.
        assert not any(
            r.levelno == logging.CRITICAL and "adopt_v3240_stamps" in r.getMessage()
            for r in caplog.records
        )

        partition.close()


# ===========================================================================
# Scenario 7: Sweep suppression with __ttl_adopt_pending__
# Spec §5.3: sweep is a complete no-op while provisional.
# ===========================================================================


class TestSweepSuppression:
    """Validates spec §5.3: while ``_adopt_provisional`` is True, the sweep
    performs zero TTL evictions.

    RED on HEAD: the provisional flag does not exist; sweep runs normally.
    """

    def test_provisional_adopt_suppresses_sweep(self, tmp_path, caplog):
        """After a cold provisional adopt, a flush with advanced high-water
        does NOT delete any past-stamped adopted key.

        Then after corroboration (spec §5.4), eviction resumes.
        """

        producer = _make_producer()
        expiry_past = NOW_MS - DAY_MS  # already expired
        expiry_future = NOW_MS + 7 * DAY_MS
        # Mix of past and future stamps.
        msgs = [
            _v3240_msg("k_past", "v_past", expiry_past),
            _v3240_msg("k_future", "v_future", expiry_future),
        ]

        partition = _rocksdb_partition(
            tmp_path, name="sweep_guard", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        # Precondition: partition is provisionally adopted.
        assert partition.uses_ttl_stamps is True

        # Attempt to sweep: advance high-water past the expired key.
        partition._high_water_ms = NOW_MS + 1
        # A flush with TTL-enabled data would normally sweep expired keys.
        tx = partition.begin()
        tx.set(
            key="klive",
            value="vlive",
            prefix=b"pfx",
            timestamp=NOW_MS,
        )
        tx.prepare(processed_offsets={"topic": 1})
        # The replay above advanced the saved changelog offset (one per message),
        # so a live flush must use a strictly-higher offset (monotonic guard).
        tx.flush(changelog_offset=100)

        # The past-stamped adopted key must STILL be present (sweep suppressed).
        raw_past_key = b"pfx|" + json_dumps("k_past")
        val = _raw_default_get(partition, raw_past_key)
        assert val is not None, (
            "Sweep must be suppressed while __ttl_adopt_pending__ is set; "
            "the past-stamped key should survive"
        )

        partition.close()


# ===========================================================================
# Scenario 3 continued + Scenario 7 continued: Corroboration
# Spec §5.4: first live ttl= write corroborates — done-marker produced,
# backup dropped, pending cleared, sweep resumes.
# ===========================================================================


class TestCorroboration:
    """Validates spec §5.4: a live ``state.set(..., ttl=...)`` write on a
    provisionally-adopted partition corroborates the adoption.

    RED on HEAD: the provisional mechanism does not exist.
    """

    def test_live_ttl_write_corroborates_provisional_adopt(self, tmp_path, caplog):
        """Spec §5.4: after corroboration, done-marker produced, backup
        dropped, pending cleared, sweep resumes (expired adopted keys evicted).
        """
        from datetime import timedelta

        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(3)]

        partition = _rocksdb_partition(
            tmp_path, name="corroborate", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        # Precondition: provisional.
        assert partition.uses_ttl_stamps is True
        metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert (
            metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None
        ), "Store must be in provisional state before corroboration"

        # Live ttl= write — the corroboration trigger.
        with caplog.at_level(logging.INFO):
            tx = partition.begin()
            tx.set(
                key="klive",
                value="vlive",
                prefix=b"pfx",
                timestamp=NOW_MS,
                ttl=timedelta(days=1),
            )
            tx.prepare(processed_offsets={"topic": 1})
            # Replay advanced the saved changelog offset; use a higher one.
            tx.flush(changelog_offset=100)

        # Done-marker produced (changelog-first).
        from quixstreams.state.metadata import (
            TTL_MIGRATION_DONE_KEY,
            TTL_SYSTEM_CF_NAME,
        )

        system_cf = partition.get_or_create_column_family(TTL_SYSTEM_CF_NAME)
        assert (
            system_cf.get(TTL_MIGRATION_DONE_KEY, default=None) is not None
        ), "Corroboration must produce the durable done-marker"

        # Backup dropped.
        cfs = partition.list_column_families()
        backup_exists = TTL_ADOPT_BACKUP_CF_NAME in cfs
        if backup_exists:
            backup_cf = partition.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
            backup_count = sum(1 for _ in backup_cf.keys())
            assert backup_count == 0, "Backup CF should be empty after corroboration"

        # Pending cleared.
        metadata_cf2 = partition.get_or_create_column_family(METADATA_CF_NAME)
        assert (
            metadata_cf2.get(TTL_ADOPT_PENDING_KEY, default=None) is None
        ), "Corroboration must clear __ttl_adopt_pending__"

        # INFO logged.
        assert any(
            r.levelno == logging.INFO and "corroborat" in r.getMessage().lower()
            for r in caplog.records
        ), "Corroboration must log INFO"

        partition.close()


# ===========================================================================
# Scenario 4: Cold rebuild false-positive → REVERSIBLE via rollback env var
# Spec §5.6: QUIXSTREAMS_STATE_TTL_ROLLBACK=1 restores byte-identical.
# ===========================================================================


class TestRollbackEnvVar:
    """Validates spec §5.6: a false-positive cold provisional adopt is
    reversible via ``QUIXSTREAMS_STATE_TTL_ROLLBACK=1``.

    RED on HEAD: the env-var rollback mechanism does not exist.
    """

    def test_rollback_restores_byte_identical(self, tmp_path, caplog):
        """Spec §5.6: after rollback, ``uses_ttl_stamps`` is False, every
        value is byte-identical to pre-adoption, index/backup/marker gone.
        """
        producer = _make_producer()
        # Legacy set_bytes values that look like stamps (false positive).
        originals = {}
        msgs = []
        for i in range(5):
            raw_key = b"pfx|" + json_dumps(f"c{i}")
            # A counter value that passes _safe_decode_stamp
            value = struct.pack(">Q", NOW_MS + i * DAY_MS) + f"-data-{i}".encode()
            originals[raw_key] = value
            msgs.append((raw_key, value, False))

        # First open: cold rebuild → provisional auto-adopt.
        partition = _rocksdb_partition(
            tmp_path, name="rollback", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        # Precondition: flipped (provisionally).
        assert partition.uses_ttl_stamps is True
        partition.close()

        # Set the rollback env var and reopen.
        os.environ["QUIXSTREAMS_STATE_TTL_ROLLBACK"] = "1"
        try:
            producer2 = _make_producer()
            partition2 = _rocksdb_partition(
                tmp_path, name="rollback", changelog_producer=producer2
            )

            # Rollback result: store is legacy.
            assert (
                partition2.uses_ttl_stamps is False
            ), "After rollback the store must be legacy"

            # Every value byte-identical.
            for raw_key, expected in originals.items():
                actual = _raw_default_get(partition2, raw_key)
                assert (
                    actual == expected
                ), f"Value for {raw_key!r} must be byte-identical after rollback"

            # Index/backup/marker gone.
            cfs = partition2.list_column_families()
            assert (
                TTL_ADOPT_BACKUP_CF_NAME not in cfs
            ), "Backup CF must be dropped after rollback"
            assert (
                TTL_INDEX_CF_NAME not in cfs
            ), "Index CF must be dropped after rollback"
            metadata_cf = partition2.get_or_create_column_family(METADATA_CF_NAME)
            assert metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is None

            partition2.close()
        finally:
            os.environ.pop("QUIXSTREAMS_STATE_TTL_ROLLBACK", None)

    def test_rollback_fresh_volume_suppresses_adopt(self, tmp_path, caplog):
        """Spec §5.6: on a fresh volume (no local marker/backup), the env var
        suppresses the cold-heuristic provisional adopt entirely.

        The store stays legacy and the census is quarantined.
        """
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]

        os.environ["QUIXSTREAMS_STATE_TTL_ROLLBACK"] = "1"
        try:
            partition = _rocksdb_partition(
                tmp_path, name="suppress", changelog_producer=producer
            )
            _replay_default(partition, msgs, now_ms=NOW_MS)
            partition.complete_recovery()

            assert partition.uses_ttl_stamps is False, (
                "With QUIXSTREAMS_STATE_TTL_ROLLBACK=1, cold adopt must be "
                "suppressed; store stays legacy"
            )
            partition.close()
        finally:
            os.environ.pop("QUIXSTREAMS_STATE_TTL_ROLLBACK", None)

    def test_rollback_memory_twin(self, caplog):
        """Spec §5.7: memory twin of the rollback — the env var suppresses
        auto-adopt on the next open.

        RED on HEAD: neither auto-adopt nor rollback exists for memory.
        """
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]
        full = [(k, v, "default", s) for k, v, s in msgs]

        os.environ["QUIXSTREAMS_STATE_TTL_ROLLBACK"] = "1"
        try:
            partition = MemoryStorePartition(changelog_producer=producer)
            _replay_memory(partition, full, now_ms=NOW_MS)
            partition.complete_recovery()

            assert (
                partition.uses_ttl_stamps is False
            ), "Memory: with rollback env var, cold adopt must be suppressed"
            partition.close()
        finally:
            os.environ.pop("QUIXSTREAMS_STATE_TTL_ROLLBACK", None)


# ===========================================================================
# Scenario 5: Cold rebuild, all-past stamped census → quarantined
# Spec §5.2: KEEP the all-past REFUSE guard as QUARANTINE.
# ===========================================================================


class TestAllPastQuarantined:
    """Validates spec §5.2: an all-past stamped census is quarantined — never
    adopted, store stays legacy, values byte-identical.

    GREEN on HEAD: HEAD already quarantines all-past (with ``adopt_v3240_stamps=True``
    it refuses, and without it stays legacy). The spec KEEPS this guard, so
    the behavior must persist after the flag is removed. RED because the
    construction path changes (no flag to set).
    """

    def test_all_past_stays_legacy_quarantined(self, tmp_path, caplog):
        """Spec §5.2: all-past → quarantined, not adopted."""
        producer = _make_producer()
        expiry_past = NOW_MS - 7 * DAY_MS  # all in the past
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry_past) for i in range(4)]

        partition = _rocksdb_partition(
            tmp_path, name="allpast", changelog_producer=producer
        )
        with caplog.at_level(logging.WARNING):
            _replay_default(partition, msgs, now_ms=NOW_MS)
            partition.complete_recovery()

        assert (
            partition.uses_ttl_stamps is False
        ), "All-past census must stay legacy (quarantined)"

        # Values byte-identical.
        for i in range(4):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            expected = encode_ttl_value(expiry_past, json_dumps(f"v{i}"))
            assert _raw_default_get(partition, raw_key) == expected

        # Census preserved (quarantined).
        assert _pending_keys(partition) == {
            b"pfx|" + json_dumps(f"k{i}") for i in range(4)
        }

        # No index or backup created.
        assert _index_count(partition) == 0
        cfs = partition.list_column_families()
        assert TTL_ADOPT_BACKUP_CF_NAME not in cfs

        # WARN (downgraded from CRITICAL per spec §5.8).
        assert any(
            r.levelno == logging.WARNING and "quarantin" in r.getMessage().lower()
            for r in caplog.records
        ) or any(
            r.levelno == logging.WARNING and "refused" in r.getMessage().lower()
            for r in caplog.records
        ), "All-past quarantine must log WARN"

        partition.close()

    def test_all_past_memory_twin(self, caplog):
        """Spec §5.7: memory twin — all-past quarantined, not adopted."""
        producer = _make_producer()
        expiry_past = NOW_MS - 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry_past) for i in range(4)]
        full = [(k, v, "default", s) for k, v, s in msgs]

        partition = MemoryStorePartition(changelog_producer=producer)
        _replay_memory(partition, full, now_ms=NOW_MS)
        partition.complete_recovery()

        assert (
            partition.uses_ttl_stamps is False
        ), "Memory: all-past census must stay legacy (quarantined)"
        partition.close()


# ===========================================================================
# Scenario sub-100%: one bad value still vetoes (regression guard)
# Spec §5.2 implicit: quorum must be 100%.
# ===========================================================================


class TestSubQuorumVeto:
    """Validates that a sub-100% quorum still vetoes adoption.

    GREEN on HEAD: HEAD already vetoes on sub-100% quorum.
    """

    def test_single_bad_value_veto(self, tmp_path, caplog):
        """One non-validating value blocks auto-adopt — store stays legacy,
        census discarded."""
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]
        # One genuine short legacy value.
        msgs.append((b"pfx|" + json_dumps("k_legacy"), b"short", False))

        partition = _rocksdb_partition(
            tmp_path, name="veto", changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)
        partition.complete_recovery()

        assert partition.uses_ttl_stamps is False
        assert _pending_keys(partition) == set()
        partition.close()


# ===========================================================================
# Finding #2 (spec §5.1 step 3): a warm store detected via a format-version /
# high-water marker but with NO local __ttl_index__ CF flips into TTL mode; the
# index must be rebuilt from the stamped default values, else records with real
# expiries are unindexed and never swept.
# ===========================================================================


class TestWarmIndexRebuild:
    """Red-first for finding #2.

    RED on the current tree: the warm-flip branch creates an EMPTY ``__ttl_index__``
    CF and never rebuilds it, so stamped records with real expiries carry no index
    entry and the sweep can never expire them.
    """

    def test_warm_flip_no_index_cf_rebuilds_index(self, tmp_path):
        expiry = NOW_MS + 7 * DAY_MS
        _seed_warm_ttl_store(
            tmp_path,
            "warm_no_idx",
            format_version=STATE_FORMAT_VERSION,
            # No __ttl_enabled__ flag — the format-version marker is the warm
            # signal that fires _has_warm_ttl_artifacts.
            write_enabled_flag=False,
            # No __ttl_index__ CF at all — the preview never maintained one.
            write_index_cf=False,
            write_high_water=True,
            expiry_ms=expiry,
            n_records=4,
        )

        producer = _make_producer()
        partition = _rocksdb_partition(
            tmp_path, name="warm_no_idx", changelog_producer=producer
        )

        # The warm signal (format marker) flips the store deterministically.
        assert (
            partition.uses_ttl_stamps is True
        ), "a format-version marker is a warm TTL signal and must flip the store"

        # §5.1 step 3: the index must be rebuilt from the stamped default values.
        assert _index_count(partition) == 4, (
            "warm flip with no index CF must rebuild __ttl_index__ from the "
            "stamped default values (else records never expire)"
        )

        # Values still read back stripped-correct (kept verbatim on disk).
        for i in range(4):
            val = _read_via_tx(partition, f"k{i}", timestamp=NOW_MS)
            assert val == f"v{i}", f"warm key k{i} should read stripped"

        # Behavioral proof: advancing high-water past the expiry evicts them.
        partition._high_water_ms = expiry + 1
        batch = WriteBatch(raw_mode=True)
        partition._run_sweep(batch)
        partition._write(batch)
        for i in range(4):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            assert (
                _raw_default_get(partition, raw_key) is Marker.UNDEFINED
            ), f"warm-restored key k{i} must expire once the index is rebuilt"

        partition.close()


# ===========================================================================
# Finding #1 (spec §5.4 parity): the MEMORY done-marker path must rebuild the
# in-RAM __ttl_index__ from the still-censused all-stamped records before
# discarding the census — else a corroborated cold-adopted memory store replays
# its adopted records unindexed and the sweep never expires them.
# ===========================================================================


class TestMemoryDoneMarkerIndexRebuild:
    """Red-first for finding #1 — the memory twin of RocksDB scenario #8.

    A memory store that was cold-adopted then corroborated produces a replicated
    done-marker. On the next (always-cold) open the header-absent adopted records
    replay into the pending census and the done-marker latches; ``complete_recovery``
    must rebuild the in-RAM ``__ttl_index__`` from the all-stamped census.

    RED on the current tree: the memory ``complete_recovery`` done-marker branch
    pops the census with NO index rebuild (unlike the RocksDB sibling).
    """

    def test_memory_corroborated_cold_restore_rebuilds_index(self, caplog):
        producer = _make_producer()
        # Adopted records stamped in the PAST relative to the sweep clock below,
        # so a rebuilt index expires them and a missing index keeps them forever.
        expiry_past = NOW_MS - DAY_MS
        adopted = [_v3240_msg(f"k{i}", f"v{i}", expiry_past) for i in range(3)]
        # The replicated done-marker replays LAST (produced last on corroboration).
        done_marker = (
            TTL_MIGRATION_DONE_KEY,
            int_to_bytes(STATE_FORMAT_VERSION),
            TTL_SYSTEM_CF_NAME,
            False,
        )
        full = [(k, v, "default", s) for k, v, s in adopted] + [done_marker]

        partition = MemoryStorePartition(changelog_producer=producer)
        with caplog.at_level(logging.INFO):
            _replay_memory(partition, full, now_ms=NOW_MS)
            partition.complete_recovery()

        # Flipped via the done-marker; census discarded; NOT provisional
        # (a corroborated store carries no adopt-pending marker or backup/guard).
        assert partition.uses_ttl_stamps is True
        assert partition._adopt_provisional is False
        assert not partition._state.get(TTL_BACKFILL_PENDING_CF_NAME)

        # The index must be rebuilt from the all-stamped census.
        index = partition._state.get(TTL_INDEX_CF_NAME, {})
        assert len(index) == 3, (
            "memory done-marker path must rebuild __ttl_index__ from the "
            "all-stamped census; without it the adopted records are unindexed"
        )

        # Behavioral proof: the sweep now reclaims the past-dated adopted records.
        partition._high_water_ms = NOW_MS
        partition._run_sweep()
        main = partition._state.get("default", {})
        for i in range(3):
            raw_key = b"pfx|" + json_dumps(f"k{i}")
            assert raw_key not in main, (
                f"corroborated cold-adopted key k{i} must expire once the index "
                "is rebuilt"
            )

        partition.close()


# ===========================================================================
# Finding #5: memory rollback is replay-suppression, not an in-lifetime restore,
# so the ``_adopt_backup`` dict is dead state (populated, never read). After a
# provisional cold-adopt the partition must NOT retain a second in-RAM copy of
# every value.
# ===========================================================================


class TestAdoptBackupFootprint:
    """Red-first footprint guard for finding #5.

    RED on the current tree: ``_adopt_backup`` exists and is populated after a
    provisional cold-adopt. GREEN once the dead dict is removed. The memory
    rollback / corroboration tests are the behavioral safety net proving
    correctness survives its removal.
    """

    def test_memory_provisional_adopt_retains_no_backup_dict(self, caplog):
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(4)]
        full = [(k, v, "default", s) for k, v, s in msgs]

        partition = MemoryStorePartition(changelog_producer=producer)
        with caplog.at_level(logging.WARNING):
            _replay_memory(partition, full, now_ms=NOW_MS)
            partition.complete_recovery()

        # Precondition: the provisional cold-adopt happened.
        assert partition.uses_ttl_stamps is True
        assert partition._adopt_provisional is True

        # No duplicate in-RAM backup of the adopted values is retained.
        assert not hasattr(partition, "_adopt_backup"), (
            "memory rollback is replay-suppression, not restore; the "
            "_adopt_backup dict is dead state and must not be retained"
        )

        partition.close()


# ===========================================================================
# Finding #4: the __ttl_adopt_pending__ marker (and _adopt_provisional) must be
# armed BEFORE the first adoption chunk, so the §5.3 sweep guard covers the whole
# adoption and a crash mid-drain re-arms the guard on restart.
# ===========================================================================


class TestAdoptMarkerArmedEarly:
    """Red-first-style hardening for finding #4.

    RED before the reorder (marker written LAST): during the chunk writes the
    marker is not yet durable and the guard is off. GREEN after arming it first.
    """

    def test_marker_armed_before_first_chunk(self, tmp_path):
        producer = _make_producer()
        expiry = NOW_MS + 7 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(6)]

        opts = RocksDBOptions(
            open_max_retries=0,
            open_retry_backoff=3.0,
            legacy_backfill_chunk_size=2,
        )
        partition = _rocksdb_partition(
            tmp_path, name="armed", options=opts, changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)

        metadata_cf = partition.get_or_create_column_family(METADATA_CF_NAME)
        # For every write that ADDS backup rows (i.e. an adoption chunk), record
        # whether the provisional marker + runtime guard were already armed.
        chunk_observations = []
        real_write = partition._write

        def spy_write(batch):
            backup_cf = partition.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
            before = sum(1 for _ in backup_cf.keys())
            real_write(batch)
            after = sum(1 for _ in backup_cf.keys())
            if after > before:
                marker = metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None)
                chunk_observations.append(
                    marker is not None and partition._adopt_provisional is True
                )

        partition._write = spy_write
        try:
            partition.complete_recovery()
        finally:
            partition._write = real_write

        assert chunk_observations, "expected at least one chunk backup write"
        assert all(chunk_observations), (
            "the __ttl_adopt_pending__ marker + guard must be armed BEFORE the "
            "first adoption chunk (finding #4)"
        )
        # Adoption completed normally.
        assert partition._adopt_provisional is True
        assert metadata_cf.get(TTL_ADOPT_PENDING_KEY, default=None) is not None
        partition.close()

    def test_interrupted_adoption_resumes_without_double_backup(self, tmp_path):
        """A crash mid-adoption (after the marker + first chunk committed) must
        re-arm the guard on restart and resume over the remaining census without
        double-backing-up the already-adopted chunk."""
        producer = _make_producer()
        # Far-future expiry: the resume runs on a reopened partition with no
        # _now_ms override, so the stamps must read as future against the real
        # wallclock for the resume to re-enter provisional adoption (not the
        # all-past legacy-completion fall-through).
        expiry = NOW_MS + 3650 * DAY_MS
        msgs = [_v3240_msg(f"k{i}", f"v{i}", expiry) for i in range(6)]
        opts = RocksDBOptions(
            open_max_retries=0,
            open_retry_backoff=3.0,
            legacy_backfill_chunk_size=2,
        )

        partition = _rocksdb_partition(
            tmp_path, name="resume", options=opts, changelog_producer=producer
        )
        _replay_default(partition, msgs, now_ms=NOW_MS)

        # Interrupt once the first chunk (2 keys) has been backed up.
        real_write = partition._write

        def failing_write(batch):
            real_write(batch)
            backup_cf = partition.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
            if sum(1 for _ in backup_cf.keys()) >= 2:
                raise RuntimeError("simulated crash mid-adoption")

        partition._write = failing_write
        with pytest.raises(RuntimeError):
            partition.complete_recovery()
        partition._write = real_write
        partition.close()

        # Reopen (warm restart): the persisted marker must re-arm the guard.
        producer2 = _make_producer()
        partition2 = _rocksdb_partition(
            tmp_path, name="resume", options=opts, changelog_producer=producer2
        )
        assert partition2.uses_ttl_stamps is True
        assert (
            partition2._adopt_provisional is True
        ), "the sweep guard must re-arm from the persisted marker after a crash"

        # Resume adoption over the remaining census.
        partition2.complete_recovery()

        # All 6 adopted, backed up exactly once (no double-backup), indexed,
        # census drained, guard still armed (provisional, not corroborated).
        backup_cf = partition2.get_or_create_column_family(TTL_ADOPT_BACKUP_CF_NAME)
        assert sum(1 for _ in backup_cf.keys()) == 6
        assert _index_count(partition2) == 6
        assert _pending_keys(partition2) == set()
        assert partition2._adopt_provisional is True
        for i in range(6):
            assert _read_via_tx(partition2, f"k{i}", timestamp=NOW_MS) == f"v{i}"
        partition2.close()
