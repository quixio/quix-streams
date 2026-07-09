"""
Unit tests for §8.6 of the State TTL legacy-backfill spec (shortcut 73191):
**first-ever TTL enablement on a cold-restored store**.

Scenario (live failure, Quix Cloud 2026-06-16): a genuine v3.23.6 seeder wrote
N **un-stamped legacy** records to the store and its changelog. The TTL build is
then deployed against a **wiped** local volume, so recovery replays only
un-stamped legacy messages. The first live ``ttl=`` write must then see a
**populated legacy** store and run the existing backfill (Option 1) — NOT take
the empty-store fast path.

CONFIRMED ROOT CAUSE (OP-3 / spec §8.6.3 Q5): recovery did NOT stay in the
legacy verbatim branch. The flag-discovery heuristic ``_looks_like_stamped_value``
(``partition.py``) false-positives on legacy values whose first 8 bytes happen to
decode as a plausible epoch-ms expiry (the dedup "last-seen timestamp" value is
exactly an 8-byte big-endian epoch-ms). One such record falsely flips the
recovery partition into TTL mode; the Rule 4 wallclock filter then drops every
subsequent legacy record as "already expired", emptying the default CF. The flip
then sees an empty store and takes the empty-store fast path — the legacy records
are lost and never stamped.

The fix (spec §8.7, OP-3 resolution) is an UNAMBIGUOUS out-of-band flip signal: a
per-record ``__ttl_stamped__`` changelog header set on every default-CF record
produced while the partition is in TTL mode. Recovery flip-discovery routes on
that header (``ttl_stamped`` kwarg), never on value content;
``_looks_like_stamped_value`` is off the recovery path. The two
``TestColdRestoreFalseFlipRepro`` tests below — previously ``xfail(strict=True)``
— now PASS: a header-absent (legacy) replay stays legacy and lands verbatim.

The remaining tests assert the parts that ARE correct today: when recovery is NOT
falsely flipped (values that do not resemble stamps), legacy records land
verbatim and the first ``ttl=`` write backfills them (the Option-1 happy path);
and a genuinely-stamped restore flag-discovers and is not re-backfilled.

See ``dev-planning/state-ttl-legacy-backfill/spec.md`` §8.6.
"""

import struct
from datetime import timedelta
from unittest.mock import patch

from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
    METADATA_CF_NAME,
    TTL_BACKFILL_STAMPED_CF_NAME,
)
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.metadata import (
    TTL_ENABLED_KEY,
    TTL_INDEX_CF_NAME,
)
from quixstreams.state.rocksdb.ttl_codec import (
    decode_index_key,
    decode_ttl_value,
    encode_ttl_value,
)

DAY_MS = 86_400_000


def _capture_default_cf_changelog(changelog_producer_mock):
    """Return the list of ``(key, value, ttl_stamped)`` default-CF changelog
    messages, where ``ttl_stamped`` reflects the ``__ttl_stamped__`` header
    (spec §8.7) the producer set on the record."""
    msgs = []
    for call in changelog_producer_mock.produce.call_args_list:
        headers = call.kwargs["headers"]
        if headers[CHANGELOG_CF_MESSAGE_HEADER] == "default":
            ttl_stamped = bool(headers.get(CHANGELOG_TTL_STAMPED_HEADER))
            msgs.append((call.kwargs["key"], call.kwargs["value"], ttl_stamped))
    return msgs


def _replay_default(partition, msgs, now_ms=None):
    """Replay default-CF ``msgs`` into ``partition`` (optionally fixing now).

    ``msgs`` carry the captured ``__ttl_stamped__`` bit so recovery routes on the
    header exactly as the live recovery manager does (spec §8.7)."""
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


def _decode_default_cf(partition):
    cf = partition.get_or_create_column_family("default")
    return {key: decode_ttl_value(value) for key, value in cf.items()}


def _decode_index_cf(partition):
    cf = partition.get_or_create_column_family(TTL_INDEX_CF_NAME)
    out = {}
    for key, _ in cf.items():
        expires_at, user_key = decode_index_key(key)
        out[user_key] = expires_at
    return out


def _seed_unstamped_changelog(
    store_partition_factory, changelog_producer_mock, n, value_fn=None
):
    """
    Produce an all-un-stamped legacy changelog: a genuine v3.23.6-style writer
    (a legacy partition that never sees a ``ttl=`` write) emits plain values.
    Returns the list of default-CF ``(key, value)`` changelog messages.

    ``value_fn(i) -> str`` customizes the per-record value; default is a long
    JSON string that does NOT resemble a stamp.
    """
    src = store_partition_factory(
        name="seed", changelog_producer=changelog_producer_mock
    )
    changelog_producer_mock.produce.reset_mock()
    with src.begin() as tx:
        for i in range(n):
            value = value_fn(i) if value_fn else f"dedup-payload-value-{i}"
            tx.set(key=f"k{i}", value=value, prefix=b"pfx", timestamp=None)
    assert src.uses_ttl_stamps is False
    msgs = _capture_default_cf_changelog(changelog_producer_mock)
    src.close()
    return msgs


# ---------------------------------------------------------------------------
# Confirmed-bug reproductions (OP-3). xfail(strict) until the flip-signal fix.
# These directly drive recover_from_changelog_message with legacy values whose
# 8-byte big-endian prefix decodes as a plausible epoch-ms expiry (the dedup
# "last-seen timestamp" value layout), reproducing the live false-flip.
# ---------------------------------------------------------------------------


class TestColdRestoreFalseFlipRepro:
    def test_legacy_timestamp_values_must_not_flip_recovery(
        self, store_partition_factory
    ):
        # A legacy dedup store whose VALUE is an 8-byte BE epoch-ms timestamp.
        # No stamp was ever written; recovery MUST stay legacy/verbatim.
        recovered = store_partition_factory(name="dst")
        recovered._now_ms = lambda: 1_780_000_000_000  # ~2026 wallclock
        offset = 0
        for i in range(50):
            value = struct.pack(">q", 1_700_000_000_000 + i)  # past epoch-ms
            recovered.recover_from_changelog_message(
                key=f"pfx|k{i}".encode(),
                value=value,
                cf_name="default",
                offset=offset,
            )
            offset += 1

        # Required (Option 1): the partition stays legacy and every record lands
        # verbatim so the first ttl= write can later backfill them.
        assert recovered.uses_ttl_stamps is False
        keys = list(recovered.get_or_create_column_family("default").keys())
        assert len(keys) == 50
        assert recovered.main_cf_has_user_data() is True
        recovered.close()

    def test_first_ttl_write_must_backfill_timestamp_valued_legacy(
        self, store_partition_factory
    ):
        legacy_ttl = timedelta(days=7)
        recovered = store_partition_factory(
            name="dst", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        recovered._now_ms = lambda: 1_780_000_000_000
        offset = 0
        n = 50
        for i in range(n):
            value = struct.pack(">q", 1_700_000_000_000 + i)
            recovered.recover_from_changelog_message(
                key=f"pfx|k{i}".encode(),
                value=value,
                cf_name="default",
                offset=offset,
            )
            offset += 1

        ts = 1_790_000_000_000
        with recovered.begin() as tx:
            tx.set(
                key="klive",
                value="vlive",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )
        # Required: the recovered legacy records are backfilled (n + 1 entries).
        decoded = _decode_default_cf(recovered)
        assert len(decoded) == n + 1
        recovered.close()


# ---------------------------------------------------------------------------
# Option-1 happy path: when recovery is NOT falsely flipped (values that do not
# resemble stamps), the recovered legacy records land verbatim and the first
# ttl= write backfills them. These already pass today and guard the landing.
# ---------------------------------------------------------------------------


class TestFirstEnablementColdRestoreHappyPath:
    # Spec §8.6.7 case 8 — recovered un-stamped legacy records are reachable by
    # main_cf_has_user_data() on the same instance the transaction later uses.
    def test_recovered_legacy_records_are_visible(
        self, store_partition_factory, changelog_producer_mock
    ):
        msgs = _seed_unstamped_changelog(
            store_partition_factory, changelog_producer_mock, n=5
        )
        assert len(msgs) == 5

        recovered = store_partition_factory(name="dst")
        _replay_default(recovered, msgs)

        assert recovered.uses_ttl_stamps is False
        assert recovered.main_cf_has_user_data() is True
        keys = list(recovered.get_or_create_column_family("default").keys())
        assert len(keys) == 5
        recovered.close()

    # Spec §8.6.7 case 9 — first ttl= write after a cold restore of un-stamped
    # legacy records takes the populated/backfill branch, NOT the empty fast path.
    def test_first_ttl_write_backfills_recovered_records(
        self, store_partition_factory, changelog_producer_mock
    ):
        n = 5
        msgs = _seed_unstamped_changelog(
            store_partition_factory, changelog_producer_mock, n=n
        )

        legacy_ttl = timedelta(days=7)
        recovered = store_partition_factory(
            name="dst", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        _replay_default(recovered, msgs)
        assert recovered.uses_ttl_stamps is False  # nothing stamped to discover

        ts = 1_000_000_000_000
        with recovered.begin() as tx:
            tx.set(
                key="klive",
                value="vlive",
                prefix=b"pfx",
                timestamp=ts,
                ttl=timedelta(days=1),
            )

        assert recovered.uses_ttl_stamps is True
        decoded = _decode_default_cf(recovered)
        assert len(decoded) == n + 1  # 5 recovered legacy keys + 1 live key

        expected_legacy_expiry = ts + 7 * DAY_MS
        legacy_keys = [k for k in decoded if b"klive" not in k]
        assert len(legacy_keys) == n
        for key in legacy_keys:
            expires_at, _ = decoded[key]
            assert expires_at == expected_legacy_expiry

        index = _decode_index_cf(recovered)
        for key in legacy_keys:
            assert index[key] == expected_legacy_expiry

        live_key = next(k for k in decoded if b"klive" in k)
        assert decoded[live_key][0] == ts + DAY_MS

        meta = recovered.get_or_create_column_family(METADATA_CF_NAME)
        assert meta.get(TTL_ENABLED_KEY) == b"\x01"
        recovered.close()


class TestStampedRestoreNotMisclassified:
    """Spec §8.6.5 / case 10 — a genuinely-stamped restore must still flag-
    discover and must NOT be re-backfilled or double-stamped. This guards that
    any future flip-signal fix does not regress the stamped-restore path."""

    def _stamped_changelog(self, store_partition_factory, changelog_producer_mock):
        """Build a flipped store via the empty-store fast path and capture its
        stamped default-CF changelog messages, plus the source store snapshot."""
        src = store_partition_factory(
            name="seed", changelog_producer=changelog_producer_mock
        )
        changelog_producer_mock.produce.reset_mock()
        base = 1_000_000_000_000
        tx = src.begin()
        for i in range(3):
            # Distinct, far-future expiries so a rebuild "now=base" keeps all.
            tx.set(
                key=f"k{i}",
                value=f"v{i}",
                prefix=b"pfx",
                timestamp=base,
                ttl=timedelta(days=365 * (i + 1)),
            )
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=0)
        assert src.uses_ttl_stamps is True
        source_default = _decode_default_cf(src)
        msgs = _capture_default_cf_changelog(changelog_producer_mock)
        src.close()
        return msgs, source_default, base

    def test_stamped_restore_flag_discovers_and_does_not_rebackfill(
        self, store_partition_factory, changelog_producer_mock
    ):
        msgs, source_default, base = self._stamped_changelog(
            store_partition_factory, changelog_producer_mock
        )

        legacy_ttl = timedelta(days=99)
        recovered = store_partition_factory(
            name="dst", options=RocksDBOptions(legacy_records_ttl=legacy_ttl)
        )
        # Replay strictly before every (far-future) stamp: nothing is swept.
        _replay_default(recovered, msgs, now_ms=base)

        # Flag-discovery flipped the recovering partition into TTL mode and all
        # stamped records survived (none re-backfilled, none double-stamped).
        assert recovered.uses_ttl_stamps is True
        recovered_default = _decode_default_cf(recovered)
        assert recovered_default == source_default
        recovered.close()

    def test_cold_restore_does_not_ledger_on_replay(
        self, store_partition_factory, changelog_producer_mock
    ):
        """(v) GREEN no-regression (C1 P0, sc-73191): a cold restore on a fresh
        volume opens with the LOCAL_ONLY ``__ttl_backfill_stamped__`` ledger CF
        absent, so the replay-ledger gate ``_ledger_nonempty_at_open`` is False and
        replaying a stamped changelog ledgers NOTHING. This proves the fix cannot
        misfire on the cold-restore adopt / §15.2 / offset-skip paths — those are
        byte-for-byte unchanged (spec §7.1 case 1 / §11 case v)."""
        msgs, source_default, base = self._stamped_changelog(
            store_partition_factory, changelog_producer_mock
        )

        recovered = store_partition_factory(
            name="dst", options=RocksDBOptions(legacy_records_ttl=timedelta(days=99))
        )
        # Fresh volume: the ledger CF does not exist at open → the gate is off.
        assert recovered._ledger_nonempty_at_open is False

        _replay_default(recovered, msgs, now_ms=base)
        assert recovered.uses_ttl_stamps is True

        recovered.complete_recovery()

        # The gate held through the whole stamped replay: not one replayed record
        # was ledgered, so the ledger CF is empty (a cold restore has no live
        # backfill to resume). No spurious interrupted-migration signal.
        ledger_cf = recovered.get_or_create_column_family(TTL_BACKFILL_STAMPED_CF_NAME)
        assert set(ledger_cf.keys()) == set()
        # Stamped records survived unchanged — no double-wrap, no spurious resume.
        assert _decode_default_cf(recovered) == source_default
        recovered.close()


# ---------------------------------------------------------------------------
# §8.7 — per-record __ttl_stamped__ changelog header.
# Produce-side firing matrix and header-only recovery routing.
# ---------------------------------------------------------------------------


def _produced_default_cf_headers(changelog_producer_mock):
    """Return the per-message headers dict for each default-CF produced record."""
    out = []
    for call in changelog_producer_mock.produce.call_args_list:
        headers = call.kwargs["headers"]
        if headers[CHANGELOG_CF_MESSAGE_HEADER] == "default":
            out.append(headers)
    return out


class TestTtlStampedHeaderProduce:
    """§8.7.2 produce-side matrix."""

    def test_header_absent_on_pre_flip_legacy(
        self, store_partition_factory, changelog_producer_mock
    ):
        # Unflipped legacy partition: writes are byte-identical to v3.23.6 and
        # carry NO __ttl_stamped__ header.
        src = store_partition_factory(
            name="seed", changelog_producer=changelog_producer_mock
        )
        changelog_producer_mock.produce.reset_mock()
        with src.begin() as tx:
            tx.set(key="k0", value="v0", prefix=b"pfx", timestamp=None)
        assert src.uses_ttl_stamps is False
        headers = _produced_default_cf_headers(changelog_producer_mock)
        assert headers and all(CHANGELOG_TTL_STAMPED_HEADER not in h for h in headers)
        src.close()

    def test_header_set_on_post_flip_ttl_and_sentinel_writes(
        self, store_partition_factory, changelog_producer_mock
    ):
        # Flip the partition via the empty-store fast path with a ttl= write,
        # then in a later transaction issue a no-ttl= (SENTINEL) write. Every
        # default-CF record produced after the flip must carry the header.
        src = store_partition_factory(
            name="seed", changelog_producer=changelog_producer_mock
        )
        base = 1_000_000_000_000
        tx = src.begin()
        tx.set(
            key="kttl", value="v", prefix=b"pfx", timestamp=base, ttl=timedelta(days=1)
        )
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=0)
        assert src.uses_ttl_stamps is True

        changelog_producer_mock.produce.reset_mock()
        tx = src.begin()
        # no ttl= -> SENTINEL_NEVER stamp, but still 8B-prefixed on the wire.
        tx.set(key="knottl", value="v2", prefix=b"pfx", timestamp=base + 1)
        tx.prepare(processed_offsets={"topic": 2})
        tx.flush(changelog_offset=1)

        headers = _produced_default_cf_headers(changelog_producer_mock)
        assert headers and all(
            h.get(CHANGELOG_TTL_STAMPED_HEADER) == b"\x01" for h in headers
        )
        # And the value is genuinely SENTINEL-stamped (carries the 8B prefix).
        msgs = _capture_default_cf_changelog(changelog_producer_mock)
        _, value, ttl_stamped = msgs[-1]
        assert ttl_stamped is True
        decode_ttl_value(value)  # decodes => 8B prefix present
        src.close()

    def test_header_absent_on_non_default_cf_when_flipped(
        self, store_partition_factory, changelog_producer_mock
    ):
        # The signal is default-CF only; a non-default CF never carries it even
        # on a flipped partition.
        src = store_partition_factory(
            name="seed", changelog_producer=changelog_producer_mock
        )
        base = 1_000_000_000_000
        tx = src.begin()
        tx.set(
            key="kttl", value="v", prefix=b"pfx", timestamp=base, ttl=timedelta(days=1)
        )
        # Write to a non-default CF in the same flip transaction.
        tx.set(key="kother", value="vo", prefix=b"pfx", cf_name="other", timestamp=base)
        tx.prepare(processed_offsets={"topic": 1})
        tx.flush(changelog_offset=0)
        assert src.uses_ttl_stamps is True

        for call in changelog_producer_mock.produce.call_args_list:
            headers = call.kwargs["headers"]
            if headers[CHANGELOG_CF_MESSAGE_HEADER] == "other":
                assert CHANGELOG_TTL_STAMPED_HEADER not in headers
        src.close()


class TestRecoveryRoutesOnHeaderOnly:
    """§8.7.7 cases 5 & 6 — recovery routes purely on the header, never on
    value content; _looks_like_stamped_value is not consulted."""

    def test_header_absent_stamp_shaped_value_stays_legacy(
        self, store_partition_factory
    ):
        # The OP-3 false-positive shape: an 8-byte epoch-ms value with NO header.
        # Recovery must stay legacy and replay verbatim (no strip), and must NOT
        # consult the value-content heuristic.
        recovered = store_partition_factory(name="dst")
        recovered._now_ms = lambda: 1_780_000_000_000  # noqa: E731
        with patch.object(recovered, "_looks_like_stamped_value") as looks_spy:
            offset = 0
            for i in range(10):
                value = struct.pack(">q", 1_700_000_000_000 + i)
                recovered.recover_from_changelog_message(
                    key=f"pfx|k{i}".encode(),
                    value=value,
                    cf_name="default",
                    offset=offset,
                    ttl_stamped=False,
                )
                offset += 1
            looks_spy.assert_not_called()

        assert recovered.uses_ttl_stamps is False
        cf = recovered.get_or_create_column_family("default")
        stored = dict(cf.items())
        assert len(stored) == 10
        # Verbatim: the stored bytes equal the raw 8-byte value (no strip).
        assert stored[b"pfx|k0"] == struct.pack(">q", 1_700_000_000_000)
        recovered.close()

    def test_header_true_flips_and_filters(self, store_partition_factory):
        # header-true stamped value -> partition flips, stamped branch + Rule 4.
        recovered = store_partition_factory(name="dst")
        base = 1_000_000_000_000
        recovered._now_ms = lambda: base  # noqa: E731
        with patch.object(recovered, "_looks_like_stamped_value") as looks_spy:
            value = encode_ttl_value(base + 365 * DAY_MS, b"v")
            recovered.recover_from_changelog_message(
                key=b"pfx|k0",
                value=value,
                cf_name="default",
                offset=0,
                ttl_stamped=True,
            )
            looks_spy.assert_not_called()

        assert recovered.uses_ttl_stamps is True
        decoded = _decode_default_cf(recovered)
        assert len(decoded) == 1
        index = _decode_index_cf(recovered)
        assert index[b"pfx|k0"] == base + 365 * DAY_MS
        recovered.close()
