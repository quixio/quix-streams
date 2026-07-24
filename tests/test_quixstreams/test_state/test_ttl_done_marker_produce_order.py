"""
Regression tests for review re-review finding #6 (Fix 6):

- Part 1: the base ``_prepare`` must produce the ``__ttl_system__`` done-marker CF
  STRICTLY LAST, so an at-least-once changelog suffix loss can never leave a
  "migration done" marker ahead of the data it certifies. A plain ``sorted()``
  would put ``__ttl_system__`` BEFORE ``default`` (``"_"`` 0x5f < ``"d"`` 0x64) —
  the exact trap — so the marker-last guarantee is asserted directly.
- Part 2: the ``pending_count == 0`` early-return (a fully-migrated MIXED changelog
  drained to empty) now produces the done-marker best-effort — a failed flush
  WARNs and continues rather than failing recovery.
"""

import logging
from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock

from quixstreams.state.exceptions import ChangelogFlushError
from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import (
    CHANGELOG_CF_MESSAGE_HEADER,
    TTL_SYSTEM_CF_NAME,
)
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value

NOW_MS = 1_780_000_000_000
DAY_MS = 86_400_000


def _producer():
    producer = MagicMock(spec_set=ChangelogProducer)
    type(producer).changelog_name = PropertyMock(return_value="test-changelog-topic")
    type(producer).partition = PropertyMock(return_value=0)
    return producer


def _produced_cf_order(producer):
    """The CF-name of each changelog record produced, in call order."""
    out = []
    for call in producer.produce.call_args_list:
        headers = call.kwargs.get("headers") or {}
        cf = headers.get(CHANGELOG_CF_MESSAGE_HEADER)
        if cf is not None:
            out.append(cf)
    return out


# ===========================================================================
# Part 1 — __ttl_system__ produced LAST.
# ===========================================================================


def test_flip_flush_produces_system_cf_after_default(tmp_path):
    """A live empty-store flip stages both the stamped ``default`` user write and
    the ``__ttl_system__`` done-marker; the base ``_prepare`` must produce the
    system CF AFTER default (never first, as a plain sorted() would)."""
    producer = _producer()
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=producer,
    )
    with part.begin() as tx:
        tx.set(key="k", value="v", prefix=b"pfx", timestamp=NOW_MS, ttl=None)
        # A ttl= write triggers the flip so the done-marker is staged this flush.
        tx.set(
            key="k2", value="v2", prefix=b"pfx", timestamp=NOW_MS, ttl=timedelta(days=1)
        )

    order = _produced_cf_order(producer)
    assert TTL_SYSTEM_CF_NAME in order, "the flip flush must produce the done-marker"
    assert order[-1] == TTL_SYSTEM_CF_NAME, "the system CF must be produced LAST"
    assert order[0] != TTL_SYSTEM_CF_NAME, "sorted() trap: marker must NOT be first"
    assert order.index("default") < order.index(TTL_SYSTEM_CF_NAME)
    part.close()


def test_base_prepare_orders_system_cf_last_among_many(tmp_path):
    """Directly stage several non-system CFs plus ``__ttl_system__`` into a
    transaction cache and prepare: the base ``_prepare`` must emit the non-system
    CFs sorted, then ``__ttl_system__`` last (not sorted inline, where ``_`` would
    sort it first)."""
    producer = _producer()
    partition = MemoryStorePartition(changelog_producer=producer)
    tx = partition.begin()
    # Stage into distinct non-local CFs plus the system marker CF directly.
    for cf in ("zzz_cf", "default", "aaa_cf", TTL_SYSTEM_CF_NAME):
        tx._update_cache.set(key=b"k", value=b"v", prefix=b"", cf_name=cf)
    tx._prepare(processed_offsets=None)

    order = _produced_cf_order(producer)
    assert order[-1] == TTL_SYSTEM_CF_NAME
    non_system = order[:-1]
    assert TTL_SYSTEM_CF_NAME not in non_system
    assert non_system == sorted(non_system), "non-system CFs should be deterministic"
    assert order != sorted(order), "a plain sorted() (marker first) must NOT be used"


# ===========================================================================
# Part 2 — empty-pending path produces the marker best-effort.
# ===========================================================================


def _all_stamped_no_leftovers(part):
    """All-stamped header-true replay (no legacy leftovers) -> Branch A, pending
    drains to empty."""
    part._now_ms = lambda: NOW_MS  # noqa: E731
    for offset in range(3):
        part.recover_from_changelog_message(
            key=f"pfx|s{offset}".encode(),
            value=encode_ttl_value(NOW_MS + 30 * DAY_MS, f"v{offset}".encode()),
            cf_name="default",
            offset=offset,
            ttl_stamped=True,
        )


def test_empty_pending_path_produces_done_marker(tmp_path):
    producer = _producer()
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=producer,
    )
    _all_stamped_no_leftovers(part)
    assert part._count_backfill_pending() == 0
    assert part._has_local_migration_done_marker() is False

    part.complete_recovery()

    # The marker is now recorded locally and produced to the changelog.
    assert part._has_local_migration_done_marker() is True
    assert TTL_SYSTEM_CF_NAME in _produced_cf_order(producer)
    part.close()


def test_empty_pending_marker_flush_failure_warns_and_continues(tmp_path, caplog):
    producer = _producer()
    part = RocksDBStorePartition(
        (tmp_path / "db").as_posix(),
        options=RocksDBOptions(open_max_retries=0, open_retry_backoff=3.0),
        changelog_producer=producer,
    )
    _all_stamped_no_leftovers(part)

    def _boom():
        raise ChangelogFlushError("simulated marker flush failure")

    part._produce_migration_done_marker = _boom

    with caplog.at_level(logging.WARNING):
        part.complete_recovery()  # must NOT raise — best-effort on the empty path

    assert any(
        "done-marker changelog flush failed" in r.getMessage() for r in caplog.records
    )
    # Recovery continued; the store is left unmarked so the next restart retries.
    assert part._has_local_migration_done_marker() is False
    part.close()
