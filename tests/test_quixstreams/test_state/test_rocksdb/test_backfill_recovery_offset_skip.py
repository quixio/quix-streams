"""
Regression: an interrupted live legacy-TTL migration must not be RE-stamped on a
cold restore just because its re-stamp records were skipped by the changelog
source-offset consistency check.

Root cause. ``backfill_legacy_records`` (the live populated-store backfill) used to
produce its bulk re-stamp records tagged with the TRIGGERING write's
``processed_offsets`` (``partition.py`` header build). On a fresh-volume cold
restore of an INTERRUPTED migration, that source offset is at/ahead of the
committed source offset, so ``RecoveryPartition._should_apply_changelog`` SKIPS
those stamped records: they never reach ``recover_from_changelog_message``, so the
``__ttl_backfill_pending__`` supersession DELETE never runs. The re-stamped keys —
still censused by their applied header-absent legacy records — remain in the
pending census and are re-stamped by ``complete_recovery``, clobbering the
migration's per-record stamps with ``legacy_records_ttl`` and redundantly
re-producing already-stamped keys to the changelog.

Fix. The backfill's bulk re-stamps are idempotent transforms of ALREADY-committed
legacy data; their durability must not hinge on the triggering live write's commit
status. They are produced with ``processed_offsets=None`` (always-apply), exactly
like ``_complete_pending_backfill`` and ``_produce_migration_done_marker`` already
do — so recovery's ``processed_offsets is None`` fast-path applies them on every
cold restore and the supersession DELETE runs.

The tests drive the REAL backfill to capture the exact changelog records it emits
(so they are sensitive to the produce-side header), then replay them through the
real ``RecoveryPartition`` (real header parse + the source-offset skip) with a
``committed_offsets`` that skips the migration offset.
"""

from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
    CHANGELOG_TTL_STAMPED_HEADER,
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions
from quixstreams.state.rocksdb.ttl_codec import decode_ttl_value
from quixstreams.utils.json import dumps as json_dumps
from tests.utils import ConfluentKafkaMessageStub

DAY_MS = 86_400_000
SRC = "src-topic"
NOW_MS = 1_780_000_000_000
LEGACY_TTL = timedelta(days=7)
# Offset the interrupted migration's re-stamps were tagged with, ahead of the
# committed source offset below (so a cold restore skips them on HEAD).
MIGRATION_OFFSET = 1000
# Offset the original legacy writes were committed at (behind ``COMMITTED``).
LEGACY_OFFSET = 5
COMMITTED = 100


def _producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="cl")
    type(producer).partition = PropertyMock(return_value=0)
    # A real int keeps ``_flush_backfill_changelog`` on its progress path.
    producer.flush.return_value = 0
    return producer


def _capture_backfill_restamps(store_partition_factory, done_keys, trigger_key):
    """Drive the REAL live backfill over ``done_keys`` and return the default-CF
    stamped records it produced as ``{raw_key: (value, processed_offsets_header)}``,
    excluding the triggering write's own key.

    ``processed_offsets_header`` is the raw header bytes the backfill attached — the
    exact value the fix changes (``json_dumps(None)`` vs the trigger's offsets).
    """
    src_producer = _producer()
    # 1. Seed the done keys as plain legacy records (no TTL), then close.
    src = store_partition_factory(name="src", changelog_producer=src_producer)
    with src.begin() as tx:
        for i, key in enumerate(done_keys):
            tx.set(key=key, value=f"v-{i}", prefix=b"pfx")
    src.close()

    # 2. Reopen with legacy_records_ttl and trigger the populated-store backfill via
    #    a ttl= write, passing a non-None processed_offsets (the triggering write's
    #    source offset) exactly as the checkpoint pipeline does.
    src = store_partition_factory(
        name="src",
        options=RocksDBOptions(legacy_records_ttl=LEGACY_TTL),
        changelog_producer=src_producer,
    )
    tx = src.begin()
    tx.set(
        key=trigger_key,
        value="trigger",
        prefix=b"pfx",
        timestamp=NOW_MS,
        ttl=LEGACY_TTL,
    )
    tx.prepare(processed_offsets={SRC: MIGRATION_OFFSET})
    tx.flush()

    trigger_raw = None
    restamps = {}
    for call in src_producer.produce.call_args_list:
        headers = call.kwargs.get("headers") or {}
        if headers.get(CHANGELOG_CF_MESSAGE_HEADER) != "default":
            continue
        if not headers.get(CHANGELOG_TTL_STAMPED_HEADER):
            continue
        key = call.kwargs["key"]
        po_header = headers.get(CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER)
        # The triggering write's own stamped record is a genuine live write and is
        # excluded (it correctly keeps the source offset); we only want the bulk
        # re-stamps of pre-existing legacy data.
        if trigger_key.encode() in key:
            trigger_raw = (key, po_header)
            continue
        restamps[key] = (call.kwargs["value"], po_header)
    src.close()
    assert trigger_raw is not None, "triggering write's record not captured"
    return restamps


def _cf_msg(key, value, offset, *, ttl_stamped, processed_offset):
    headers = [(CHANGELOG_CF_MESSAGE_HEADER, b"default")]
    if processed_offset is None:
        headers.append((CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER, json_dumps(None)))
    else:
        headers.append(
            (
                CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER,
                json_dumps({SRC: processed_offset}),
            )
        )
    if ttl_stamped:
        headers.append((CHANGELOG_TTL_STAMPED_HEADER, b"\x01"))
    return ConfluentKafkaMessageStub(
        key=key, value=value, headers=headers, offset=offset
    )


def _pending_keys(partition):
    cf = partition.get_or_create_column_family(TTL_BACKFILL_PENDING_CF_NAME)
    return set(cf.keys())


def _default_cf(partition):
    cf = partition.get_or_create_column_family("default")
    return {key: value for key, value in cf.items()}


def _captured_stamped_produces(producer):
    out = {}
    for call in producer.produce.call_args_list:
        headers = call.kwargs.get("headers") or {}
        if headers.get(CHANGELOG_TTL_STAMPED_HEADER):
            out[call.kwargs["key"]] = call.kwargs["value"]
    return out


def _done_marker_produced(producer):
    for call in producer.produce.call_args_list:
        headers = call.kwargs.get("headers") or {}
        if (
            call.kwargs.get("key") == TTL_MIGRATION_DONE_KEY
            and headers.get(CHANGELOG_CF_MESSAGE_HEADER) == TTL_SYSTEM_CF_NAME
        ):
            return True
    return False


class TestBackfillRestampProcessedOffsets:
    def test_backfill_restamps_are_always_applicable_on_recovery(
        self, store_partition_factory
    ):
        """The bulk re-stamps must carry ``processed_offsets=null`` so a cold
        restore never skips them (they transform already-committed data). RED on
        HEAD, which tags them with the triggering write's source offset."""
        restamps = _capture_backfill_restamps(
            store_partition_factory,
            done_keys=["k0", "k1", "k2"],
            trigger_key="trigger",
        )
        assert restamps, "backfill produced no re-stamp records"
        null_header = json_dumps(None)
        for key, (_value, po_header) in restamps.items():
            assert po_header == null_header, (
                f"re-stamp {key!r} carries processed_offsets {po_header!r}; a cold "
                f"restore whose committed offset is behind the migration offset "
                f"would SKIP it and never run the pending-census supersession delete"
            )


class TestMixedColdRestoreSkippedSupersession:
    def _replay_mixed(
        self, store_partition_factory, recovery_partition_factory, dst_producer
    ):
        """Build a MIXED cold-restore changelog from REAL backfill output and replay
        it through the real RecoveryPartition (with the source-offset skip active).

        Returns the destination partition, the RecoveryPartition, the leftover key
        set, the done-key raw values map, and the captured backfill stamp bytes.
        """
        done_keys = ["k0", "k1", "k2"]
        restamps = _capture_backfill_restamps(
            store_partition_factory, done_keys=done_keys, trigger_key="trigger"
        )
        # Raw changelog keys as the backfill produced them (``pfx|"k0"`` ...).
        done_raw = sorted(restamps.keys())
        # Leftover keys never reached by the interrupted backfill: distinct raw keys.
        leftover_raw = [f'pfx|"m{i}"'.encode() for i in range(3)]
        leftover_values = {
            k: f"leftover-{i}".encode() for i, k in enumerate(leftover_raw)
        }

        dst = store_partition_factory(
            name="dst",
            options=RocksDBOptions(legacy_records_ttl=LEGACY_TTL),
            changelog_producer=dst_producer,
        )
        dst._now_ms = lambda: NOW_MS  # noqa: E731

        msgs = []
        offset = 0
        # (1) Original header-absent legacy records for done + leftover keys, all
        #     committed (source offset behind COMMITTED) → applied → censused.
        for key in done_raw:
            msgs.append(
                _cf_msg(
                    key,
                    b'"seed"',
                    offset,
                    ttl_stamped=False,
                    processed_offset=LEGACY_OFFSET,
                )
            )
            offset += 1
        for key, value in leftover_values.items():
            msgs.append(
                _cf_msg(
                    key,
                    value,
                    offset,
                    ttl_stamped=False,
                    processed_offset=LEGACY_OFFSET,
                )
            )
            offset += 1
        # (2) One committed slice of migration progress fires the recovery flip
        #     (processed_offsets=None → always applied, like a completion record).
        #     Distinct key, so it never collides with the done/leftover census.
        msgs.append(
            _cf_msg(
                b'pfx|"flip"',
                restamps[done_raw[0]][0],
                offset,
                ttl_stamped=True,
                processed_offset=None,
            )
        )
        offset += 1
        # (3) The interrupted migration's re-stamps for the done keys, carrying the
        #     header the REAL backfill emitted. On HEAD that is the (uncommitted)
        #     migration offset → SKIPPED; with the fix it is null → applied.
        for key in done_raw:
            value, po_header = restamps[key]
            processed_offset = (
                MIGRATION_OFFSET if po_header != json_dumps(None) else None
            )
            msgs.append(
                _cf_msg(
                    key,
                    value,
                    offset,
                    ttl_stamped=True,
                    processed_offset=processed_offset,
                )
            )
            offset += 1

        rp = recovery_partition_factory(
            store_partition=dst,
            committed_offsets={SRC: COMMITTED},
            lowwater=0,
            highwater=len(msgs),
        )
        for msg in msgs:
            rp.recover_from_changelog_message(msg)
        return dst, rp, set(leftover_raw), leftover_values, restamps, done_raw

    def test_census_excludes_backfilled_keys(
        self, store_partition_factory, recovery_partition_factory
    ):
        """The pending census after replay must hold ONLY the genuinely-un-backfilled
        leftover keys. RED on HEAD: the skipped re-stamps leave the done keys in the
        census too."""
        dst, _rp, leftover, _vals, _restamps, _done = self._replay_mixed(
            store_partition_factory, recovery_partition_factory, _producer()
        )
        assert _pending_keys(dst) == leftover
        dst.close()

    def test_completion_stamps_only_leftovers_and_preserves_backfill_stamps(
        self, store_partition_factory, recovery_partition_factory
    ):
        """Completion must stamp ONLY the leftovers and leave the already-backfilled
        keys byte-unchanged. RED on HEAD: completion re-stamps the done keys too,
        clobbering their per-record stamp with legacy_records_ttl."""
        dst_producer = _producer()
        dst, rp, leftover, leftover_values, restamps, done_raw = self._replay_mixed(
            store_partition_factory, recovery_partition_factory, dst_producer
        )
        assert rp is not None

        dst_producer.produce.reset_mock()
        rp.complete_recovery()

        # (a)/(b) Completion produced stamped records for EXACTLY the leftovers.
        produced = _captured_stamped_produces(dst_producer)
        assert set(produced.keys()) == leftover

        # (c) The already-backfilled done keys keep their ORIGINAL migration stamp
        #     bytes (applied from the changelog), not a fresh legacy_records_ttl wrap.
        on_disk = _default_cf(dst)
        for key in done_raw:
            assert on_disk[key] == restamps[key][0]

        # Leftovers are stamped once with a valid finite expiry over their raw value.
        expected_expiry = NOW_MS + 7 * DAY_MS
        for key, raw in leftover_values.items():
            assert decode_ttl_value(on_disk[key]) == (expected_expiry, raw)

        # (d) Pending drained and the durable done-flag was produced flag-last.
        assert _pending_keys(dst) == set()
        assert _done_marker_produced(dst_producer)
        dst.close()
