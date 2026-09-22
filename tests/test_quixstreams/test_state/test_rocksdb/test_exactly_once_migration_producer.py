"""
Under exactly-once the legacy-TTL migration / backfill records must be produced
through a dedicated NON-transactional producer, so that a per-chunk ``flush()``
means DURABLE before the local RocksDB write.

Under exactly-once the main changelog producer is transactional: ``flush()``
confirms delivery but records only become durable when the checkpoint transaction
commits (they abort otherwise). The migration paths write local state (stamps +
resume ledger / pending-cursor) immediately after producing+flushing each chunk;
if those records rode the transactional producer, a crash before commit would
leave local stamps ahead of an aborted, never-republished changelog record — the
changelog-first invariant violated. Routing migration records through a
non-transactional producer restores ``flush()==durable``.

These tests are mock-based (no broker): they assert (1) ``ChangelogProducer``
routes ``migration=True`` traffic to the migration producer and normal traffic to
the main producer, and (2) the recovery-completion path produces via the migration
producer with the per-chunk flush ordered BEFORE the local write.
"""

import ssl
from datetime import timedelta
from unittest.mock import MagicMock

from quixstreams import Application
from quixstreams.internal_producer import InternalProducer
from quixstreams.state.recovery import ChangelogProducer
from quixstreams.state.rocksdb import RocksDBOptions, RocksDBStorePartition
from quixstreams.state.rocksdb.ttl_codec import encode_ttl_value

DAY_MS = 86_400_000


class TestChangelogProducerRouting:
    def test_migration_true_routes_to_migration_producer(self):
        main = MagicMock(spec_set=InternalProducer)
        migration = MagicMock(spec_set=InternalProducer)
        cp = ChangelogProducer(
            changelog_name="cl",
            partition=0,
            producer=main,
            migration_producer=migration,
        )

        cp.produce(key=b"k", value=b"v", headers={"h": b"1"}, migration=True)
        migration.produce.assert_called_once()
        main.produce.assert_not_called()

        # Normal (non-migration) production stays on the main (transactional)
        # producer — the fix must not change normal changelog production.
        cp.produce(key=b"k2", value=b"v2")
        main.produce.assert_called_once()
        migration.produce.assert_called_once()  # unchanged

    def test_migration_flush_routes_to_migration_producer(self):
        main = MagicMock(spec_set=InternalProducer)
        migration = MagicMock(spec_set=InternalProducer)
        migration.flush.return_value = 0
        cp = ChangelogProducer(
            changelog_name="cl",
            partition=0,
            producer=main,
            migration_producer=migration,
        )
        cp.flush(migration=True)
        migration.flush.assert_called_once()
        main.flush.assert_not_called()

    def test_falls_back_to_main_when_no_migration_producer(self):
        # Non-exactly-once: no migration producer configured, so migration
        # records fall back to the main producer (already non-transactional).
        main = MagicMock(spec_set=InternalProducer)
        cp = ChangelogProducer(changelog_name="cl", partition=0, producer=main)
        cp.produce(key=b"k", value=b"v", migration=True)
        main.produce.assert_called_once()
        cp.flush(migration=True)
        main.flush.assert_called_once()


class TestCompletionUsesMigrationProducer:
    def test_completion_produces_via_migration_producer_flush_before_write(
        self, tmp_path
    ):
        now_ms = 1_780_000_000_000
        stamp_expiry = now_ms + 30 * DAY_MS
        n_legacy = 3

        main = MagicMock(spec_set=InternalProducer)
        migration = MagicMock(spec_set=InternalProducer)
        order: list[str] = []

        def _flush(*_a, **_k):
            # Fire every recorded ``on_delivery`` callback with a successful
            # delivery before reporting a drained (0) queue, mirroring a real
            # producer serving its callbacks on flush. Without this, the produce
            # phase's acked count never catches up to its produced count
            # and the M3 drained-but-unacked check (partition.py) raises
            # ``ChangelogFlushError`` spuriously.
            for call in migration.produce.call_args_list:
                on_delivery = call.kwargs.get("on_delivery")
                if on_delivery is not None:
                    on_delivery(None, None)
            order.append("flush")
            return 0

        migration.flush.side_effect = _flush
        changelog = ChangelogProducer(
            changelog_name="cl",
            partition=0,
            producer=main,
            migration_producer=migration,
        )

        part = RocksDBStorePartition(
            (tmp_path / "dst").as_posix(),
            changelog_producer=changelog,
            options=RocksDBOptions(legacy_records_ttl=timedelta(days=7)),
        )
        part._now_ms = lambda: now_ms  # noqa: E731

        # MIXED replay: 1 stamped survivor + n_legacy leftovers.
        msgs = [
            (b"pfx|s0", encode_ttl_value(stamp_expiry, b"stamped-0"), True),
        ]
        for i in range(n_legacy):
            msgs.append((f"pfx|l{i}".encode(), f"legacy-{i}".encode(), False))
        offset = 0
        for key, value, ttl_stamped in msgs:
            part.recover_from_changelog_message(
                key=key,
                value=value,
                cf_name="default",
                offset=offset,
                ttl_stamped=ttl_stamped,
            )
            offset += 1

        # Track local-write ordering only for the completion phase. The flip-flag
        # persist is traced SEPARATELY so the local write it contributes is
        # ATTRIBUTED in the sequence below instead of appearing as an anonymous
        # extra "write" the next reader has to guess at.
        original_write = part._write
        original_stamp_flip = part._stamp_flip_metadata
        order.clear()

        def _tracked_write(batch):
            order.append("write")
            return original_write(batch)

        def _tracked_stamp_flip():
            order.append("flip")
            return original_stamp_flip()

        part._write = _tracked_write
        part._stamp_flip_metadata = _tracked_stamp_flip
        part.complete_recovery()

        # All migration records (n_legacy leftover stamps + 1 done-marker) went to
        # the NON-transactional migration producer; the main (transactional)
        # producer was not used for them.
        assert migration.produce.call_count == n_legacy + 1
        assert main.produce.call_count == 0

        # Per-chunk flush precedes the local write (changelog-first): the leftover
        # chunk (flush, write), then the flip flag (flip + the one local write it
        # makes), then the done-marker (flush, write).
        #
        # The flip pair is sc-74843 round 3, and flag-before-marker is the
        # REQUIRED order, not a preference: ``_produce_migration_done_marker``
        # persists ``__ttl_enabled__`` / ``__ttl_format_version__`` in its own
        # durable write BEFORE producing or writing the marker, so the on-disk
        # invariant "done-marker present => flip flag present" holds at every
        # crash point. A marker without the flag reopens the store in legacy mode
        # over values that are now all stamped and dies in the value deserializer
        # on the first ``state.get()``. Writing the flag after the marker, or in
        # the same batch, is precisely the shape that crash-looped in production.
        assert order == ["flush", "write", "flip", "write", "flush", "write"]

        # The changelog-first invariant this test exists for is UNCHANGED by that
        # extra write, and this is the assertion that says so rather than
        # quietly relaxing the one above. The flip flag is local-only metadata --
        # neither key is ever produced to the changelog (the marker is what a
        # cold rebuild learns "TTL enabled" from) -- so there is no changelog
        # record it could run ahead of. Drop the flip and its own write from the
        # trace and the original alternation must remain exactly as it was.
        flip = order.index("flip")
        changelogged = [
            entry for i, entry in enumerate(order) if i not in (flip, flip + 1)
        ]
        assert changelogged == ["flush", "write", "flush", "write"]
        part.close()


class TestEOSInternalProducerStripsTransactionalId:
    """Fix 1 (review re-review #1): under exactly-once the shared
    ``producer_extra_config`` carries a ``transactional.id``. A NON-transactional
    InternalProducer (the migration producer, or a Sources producer) must NOT
    inherit it — that would corrupt the id for itself and any later transactional
    producer. ``_get_internal_producer(transactional=False)`` must strip it on a
    copy without mutating the shared config. Construction-level (no broker)."""

    def _eos_app(self, tmp_path) -> Application:
        # A dummy broker address: Application construction does not connect
        # (librdkafka producers are lazy), so this stays offline.
        return Application(
            broker_address="localhost:9092",
            consumer_group="fix1-eos",
            processing_guarantee="exactly-once",
            state_dir=(tmp_path / "state").as_posix(),
        )

    def test_migration_producer_has_no_transactional_id(self, tmp_path):
        app = self._eos_app(tmp_path)
        # Under EOS the shared config carries a transactional.id (app __init__).
        assert "transactional.id" in app._config.producer_extra_config

        # The dedicated migration producer (created in __init__ under EOS) is
        # NON-transactional and must NOT carry the id.
        migration = app._state_manager._migration_producer
        assert migration is not None
        assert "transactional.id" not in migration._producer._producer_config

        # The same code path via _get_internal_producer(transactional=False)
        # (also the Sources producer path, app.add_source) strips it too.
        non_tx = app._get_internal_producer(transactional=False)
        assert "transactional.id" not in non_tx._producer._producer_config

    def test_transactional_producer_still_has_id(self, tmp_path):
        app = self._eos_app(tmp_path)
        # A transactional producer created AFTER still resolves/keeps its id.
        # Inspect _producer_config only (set at construction); do NOT enter the
        # producer context / flush, which would instantiate the real
        # ConfluentProducer and block on the absent broker.
        tx_internal = app._get_internal_producer(transactional=True)
        assert "transactional.id" in tx_internal._producer._producer_config
        # get_producer(transactional=True) likewise.
        prod = app.get_producer(transactional=True)
        assert "transactional.id" in prod._producer_config

    def test_shared_config_not_mutated(self, tmp_path):
        app = self._eos_app(tmp_path)
        before = dict(app._config.producer_extra_config)
        # Building non-transactional producers must not mutate the shared config
        # (the strip copies the dict, then pops from the copy).
        app._get_internal_producer(transactional=False)
        app._get_internal_producer(transactional=False)
        assert app._config.producer_extra_config == before
        assert "transactional.id" in app._config.producer_extra_config

    def test_unpicklable_extra_config_does_not_break_construction(self, tmp_path):
        """A legitimate NON-picklable value in ``producer_extra_config`` (an
        ``ssl.SSLContext``, a lock, a client handle) must not break the app.

        RED: stripping on a ``copy.deepcopy`` deep-copies every VALUE, so such a
        value raises ``TypeError: cannot pickle ...`` straight out of
        ``Application.__init__`` -- which builds the default, NON-transactional
        app producer via ``_get_internal_producer`` (``app.py:396``). This hits
        every app that is not exactly-once, i.e. the default.

        GREEN: only the top-level dict is copied, which is all the ``pop`` needs,
        and the values are carried through by identity."""
        ssl_ctx = ssl.create_default_context()
        app = Application(
            broker_address="localhost:9092",
            consumer_group="fix1-unpicklable",
            state_dir=(tmp_path / "state").as_posix(),
            producer_extra_config={"ssl.context": ssl_ctx, "linger.ms": 10},
        )
        # Carried through by REFERENCE, not copied: a deep copy of a config value
        # would hand librdkafka a different object than the caller configured.
        non_tx = app._get_internal_producer(transactional=False)
        assert non_tx._producer._producer_config["ssl.context"] is ssl_ctx
