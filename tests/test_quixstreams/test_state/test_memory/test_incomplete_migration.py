"""
TDD RED tests for review finding MAJOR #4: ``MemoryStorePartition`` lacks the
incomplete-migration (MIXED-changelog) completion that ``RocksDBStorePartition``
implements, leaving legacy records permanently never-expiring after replay.

After a MIXED replay (>=1 ``__ttl_stamped__``-header record + >=1 legacy record
without the header), ``complete_recovery()`` must:

- stamp leftover legacy records with a proper TTL expiry when
  ``legacy_records_ttl`` is configured (parity with
  ``RocksDBStorePartition.complete_recovery``);
- stamp leftover legacy records even when ``legacy_records_ttl`` is absent
  (auto-finish, no rejection -- contract revision 2026-07-02);
- remain a no-op on a pure-legacy replay (no stamped records seen).

Reference: ``tests/test_quixstreams/test_state/test_rocksdb/
test_incomplete_migration_recovery.py``.

**Current gaps discovered during investigation:**

1. **Config plumbing:** ``MemoryStorePartition`` has no ``legacy_records_ttl``
   config surface. ``RocksDBStorePartition`` receives it via
   ``RocksDBOptions.legacy_records_ttl``. ArchDev must add a constructor kwarg
   or equivalent.
2. **Census latch:** ``MemoryStorePartition`` has no ``_recovery_saw_stamped``
   attribute. The MIXED-detection census half is absent.
3. **Completion logic:** ``MemoryStorePartition`` does not override
   ``complete_recovery()`` -- the base-class no-op runs.
4. **Replay routing:** Legacy records replayed on a flipped memory partition go
   through ``_normalize_replay_value`` (which can misinterpret the first 8 bytes
   as a stamp for >= 8-byte payloads, or wrap short payloads as SENTINEL_NEVER),
   instead of landing verbatim as in the RocksDB backend's
   ``not ttl_stamped`` branch (line 316-324 of ``rocksdb/partition.py``).
"""

from datetime import timedelta

import pytest

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    SENTINEL_NEVER,
    decode_ttl_value,
    encode_ttl_value,
)

DAY_MS = 86_400_000


def _replay(partition, msgs, now_ms=None):
    """Replay ``(key, value, ttl_stamped)`` default-CF messages, optionally
    fixing the recovery wallclock so the completion expiry is deterministic."""
    if now_ms is not None:
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


def _mixed_changelog(n_stamped, n_legacy, stamp_expiry):
    """Build a MIXED changelog: ``n_stamped`` header-true stamped records
    (keys ``s0..``) and ``n_legacy`` header-absent legacy records (keys
    ``l0..``) over DISJOINT keys. Returns ``(msgs, legacy_values)``."""
    msgs = []
    legacy_values = {}
    for i in range(n_stamped):
        key = f"pfx|s{i}".encode()
        value = encode_ttl_value(stamp_expiry, f"stamped-{i}".encode())
        msgs.append((key, value, True))
    for i in range(n_legacy):
        key = f"pfx|l{i}".encode()
        raw = f"legacy-payload-{i}".encode()
        legacy_values[key] = raw
        msgs.append((key, raw, False))
    return msgs, legacy_values


def _decode_default(partition):
    """Decode all default-CF entries as ``{key: (expires_at, payload)}``."""
    out = {}
    for key, value in partition._state.get("default", {}).items():
        try:
            out[key] = decode_ttl_value(value)
        except ValueError:
            out[key] = (None, value)
    return out


def _pending_keys(partition):
    """Return the set of keys in ``__ttl_backfill_pending__`` census CF."""
    return set(partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {}).keys())


class TestMixedChangelogCompletion:
    """MIXED replay + completion tests mirroring
    ``TestMixedChangelogCompletion`` in
    ``test_rocksdb/test_incomplete_migration_recovery.py``."""

    def test_leftover_legacy_completed_at_wallclock_expiry(
        self, changelog_producer_mock
    ):
        """Validates MAJOR #4 / spec section 8.8: after a MIXED replay with
        ``legacy_records_ttl`` configured, ``complete_recovery()`` must stamp
        leftover legacy records with ``wallclock_now + legacy_records_ttl``.

        Mirrors ``TestMixedChangelogCompletion.
        test_leftover_legacy_completed_at_wallclock_expiry`` in
        ``test_rocksdb/test_incomplete_migration_recovery.py``.

        The ``legacy_records_ttl`` config surface is now wired into
        ``MemoryStorePartition`` (spec §15.4 item 1), so the config-present
        completion path stamps leftovers at ``wallclock_now + legacy_records_ttl``.
        """
        now_ms = 1_780_000_000_000
        legacy_ttl = timedelta(days=7)
        stamp_expiry = now_ms + 30 * DAY_MS
        n_stamped, n_legacy = 4, 6
        msgs, legacy_values = _mixed_changelog(n_stamped, n_legacy, stamp_expiry)

        recovered = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
            legacy_records_ttl=legacy_ttl,
        )
        _replay(recovered, msgs, now_ms=now_ms)

        # Partition should have flipped (existing behavior).
        assert recovered.uses_ttl_stamps is True

        # The MIXED-detection census latch must exist and be set.
        assert getattr(recovered, "_recovery_saw_stamped", False) is True, (
            "MemoryStorePartition lacks _recovery_saw_stamped latch; "
            "the MIXED-detection census half is absent"
        )

        # Leftover legacy keys should be censused in __ttl_backfill_pending__.
        assert _pending_keys(recovered) == set(legacy_values.keys()), (
            "MemoryStorePartition does not census legacy records into "
            "__ttl_backfill_pending__ during MIXED replay"
        )

        recovered.complete_recovery()

        expected_expiry = now_ms + 7 * DAY_MS
        decoded = _decode_default(recovered)

        # Leftover legacy records must have the correct finite expiry and
        # their full raw payload preserved.
        for key, raw in legacy_values.items():
            assert decoded[key] == (expected_expiry, raw), (
                f"Legacy record {key!r} should be stamped with "
                f"expiry={expected_expiry} and raw payload preserved, "
                f"but got {decoded[key]}"
            )

        # Stamped survivors must be byte-unchanged (not re-stamped).
        for i in range(n_stamped):
            key = f"pfx|s{i}".encode()
            assert decoded[key] == (stamp_expiry, f"stamped-{i}".encode())

        # Pending CF drained after completion.
        assert _pending_keys(recovered) == set()

        # No never-expire (SENTINEL_NEVER) legacy survivors.
        assert all(exp != SENTINEL_NEVER for exp, _ in decoded.values())

        recovered.close()

    def test_config_absent_auto_completes(self, changelog_producer_mock):
        """Validates MAJOR #4 / spec section 8.8 (contract revision
        2026-07-02): after a MIXED replay WITHOUT ``legacy_records_ttl``,
        ``complete_recovery()`` must still run to completion (auto-finish,
        no rejection). Leftover legacy records must end up properly
        TTL-stamped (stamp-encoded values, not raw legacy bytes).

        Originally the RocksDB path rejected loudly here
        (``IncompatibleStateStoreError``); the revised contract requires
        auto-finish instead. The specific expiry value is TBD per spec
        revision; this test only asserts the completion runs and records
        are stamp-encoded.

        Expected RED: ``complete_recovery()`` is a no-op (inherited from
        base ``StorePartition``), so legacy records remain in their
        un-completed replay state.
        """
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        msgs, legacy_values = _mixed_changelog(2, 3, stamp_expiry)

        recovered = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)
        assert recovered.uses_ttl_stamps is True

        # Must NOT raise -- auto-finish contract.
        recovered.complete_recovery()

        # After completion, legacy records must be stamp-encoded (the stored
        # bytes must decode via decode_ttl_value to a valid (expiry, payload)
        # tuple where payload equals the original raw value).
        for key, raw in legacy_values.items():
            stored = recovered._state.get("default", {}).get(key)
            assert (
                stored is not None
            ), f"Legacy record {key!r} missing from default CF after completion"
            try:
                expiry, payload = decode_ttl_value(stored)
            except ValueError:
                pytest.fail(
                    f"Legacy record {key!r} stored value is not stamp-encoded "
                    f"after completion: {stored!r}"
                )
            assert payload == raw, (
                f"Legacy record {key!r}: stamp-encoded payload should be the "
                f"full original value {raw!r}, but got {payload!r}"
            )
            # §15.2: config absent → survivor-derived expiry = the max surviving
            # future stamp (here the stamped survivors' own expiry).
            assert expiry == stamp_expiry, (
                f"Legacy record {key!r} should inherit the survivor-derived "
                f"expiry {stamp_expiry}, but got {expiry}"
            )

        # Store ends recovery in a consistent flipped state.
        assert recovered.uses_ttl_stamps is True

        recovered.close()


class TestUnchangedPaths:
    """Guard tests for paths that should NOT trigger completion."""

    def test_all_legacy_does_not_complete(self, changelog_producer_mock):
        """Validates spec section 8.8 no-op: a pure-legacy replay (no
        ``__ttl_stamped__``-header records) must NOT trigger completion or
        error. Records stay raw and unmodified. The live first-``ttl=``-write
        backfill (section 8.6) owns migration for this case.

        Mirrors ``TestUnchangedPaths.test_all_legacy_does_not_complete`` in
        ``test_rocksdb/test_incomplete_migration_recovery.py``.

        Expected GREEN: ``complete_recovery()`` is already a no-op and the
        partition never flips (no stamped record seen). This test pins the
        no-op contract.
        """
        now_ms = 1_780_000_000_000
        msgs = [(f"pfx|l{i}".encode(), f"legacy-{i}".encode(), False) for i in range(5)]
        recovered = MemoryStorePartition(
            changelog_producer=changelog_producer_mock,
        )
        _replay(recovered, msgs, now_ms=now_ms)

        # Never flipped: completion is a no-op.
        assert recovered.uses_ttl_stamps is False

        recovered.complete_recovery()

        # Records remain raw legacy, reachable verbatim.
        on_state = recovered._state.get("default", {})
        assert len(on_state) == 5
        for i in range(5):
            assert on_state[f"pfx|l{i}".encode()] == f"legacy-{i}".encode()

        recovered.close()
