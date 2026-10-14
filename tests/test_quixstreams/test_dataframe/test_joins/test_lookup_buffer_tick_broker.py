"""
Broker-level tests for the deadline tick: K8 and K9.

Spec: `dev-planning/lookup-deadline-tick/spec.md` §6.2 and §8.

These are the two tests that prove the change is not worse than not shipping
it. Everything else about the tick is in-process with a fake clock
(`test_lookup_buffer_tick.py`); these two need a real changelog topic, a real
committed offset and two real `Application.run()` loops, because the property
they assert is *durability of the tick's deletes*.

The failure they guard against, stated once:

    `BaseCheckpoint.empty()` used to be offsets-only. A tick consumes no
    message, so it stores no offsets, so a checkpoint holding nothing but tick
    work reported itself empty, was `close()`d rather than committed, and its
    store transaction -- carrying the `delete_interval` calls for every record
    the tick had just emitted -- was discarded. The emissions had already been
    queued on the producer. The deletes had not. So the records came due again
    on the next tick, and the next, forever while the partition stayed idle.

Both tests therefore have the same shape: app1 buffers a record and is left
idle until its deadline passes, the tick emits it and the checkpoint commits;
app2 then starts cold against the same changelog and must emit **nothing**.
A second emission in `collected2` is the bug.
"""

import uuid
from json import dumps
from typing import Any

from quixstreams.dataframe.joins.lookups import LookupBuffer
from quixstreams.models import TopicConfig
from quixstreams.models.serializers import JSONDeserializer
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    FakeLookup,
    is_resolved,
    make_fields,
)

# Long enough that a slow `app_factory` round trip (topic creation, consumer
# group join, RocksDB open) cannot expire the record before it is even
# consumed, short enough that app1 only has to idle for a few seconds after
# the record lands. The in-process suite's 1s grace is sized for a fake clock
# and is far too tight here -- see the same reasoning in
# `test_lookup_buffer_broker.py`.
TICK_GRACE_MS = 5_000

# How long app1 is allowed to idle waiting for its own tick. The tick fires
# once per `consumer_poll_timeout` (1.0s), so TICK_GRACE_MS/1000 + a few
# seconds of slack is ample; the collector stops the app as soon as the
# emission lands, so this only bounds the failure case.
APP1_IDLE_TIMEOUT = 30.0

# How long app2 idles. It consumes nothing (app1 committed the only offset),
# so this is purely "give the tick several chances to re-emit". Must be
# comfortably longer than TICK_GRACE_MS or a re-emission could be missed and
# the test would pass for the wrong reason.
APP2_IDLE_TIMEOUT = 15.0


def _make_topic(app, name: str):
    return app.topic(
        name,
        key_deserializer="str",
        value_deserializer=JSONDeserializer(),
        config=TopicConfig(num_partitions=1, replication_factor=1),
    )


def _attach_buffer(app, topic, lookup: FakeLookup, buffer: LookupBuffer, collected):
    sdf = app.dataframe(topic)
    sdf = sdf.join_lookup(lookup, make_fields(), buffer=buffer)
    sdf.update(
        lambda value, key, timestamp, headers: collected.append(
            (dict(value), key, timestamp, headers)
        ),
        metadata=True,
    )
    return sdf


def _produce(app, topic, records: list[dict[str, Any]]):
    with app.get_producer() as producer:
        for record in records:
            producer.produce(
                topic=topic.name,
                key=record["key"],
                value=dumps(record.get("value", {})).encode(),
                timestamp=record["timestamp"],
                partition=0,
            )


def _run_deadline_tick_roundtrip(app_factory, tmp_path, processing_guarantee):
    """
    Buffer one record, let its deadline pass with zero traffic, then restart
    cold against the same changelog.

    :param app_factory: the `app_factory` fixture.
    :param tmp_path: the `tmp_path` fixture.
    :param processing_guarantee: `"at-least-once"` (K8) or `"exactly-once"` (K9).
    :return: an `(emitted_by_app1, emitted_by_app2)` pair of collected lists.
    """
    consumer_group = str(uuid.uuid4())
    topic_name = str(uuid.uuid4())

    collected1: list = []

    app1 = app_factory(
        consumer_group=consumer_group,
        state_dir=tmp_path / "state-app1",
        auto_offset_reset="earliest",
        commit_interval=1.0,
        processing_guarantee=processing_guarantee,
    )
    topic1 = _make_topic(app1, topic_name)
    lookup1 = FakeLookup()
    buffer1 = LookupBuffer(grace_ms=TICK_GRACE_MS, is_resolved=is_resolved)
    sdf1 = app1.dataframe(topic1)
    sdf1 = sdf1.join_lookup(lookup1, make_fields(), buffer=buffer1)

    def collect_and_stop(value, key, timestamp, headers):
        collected1.append((dict(value), key, timestamp, headers))
        # The tick has emitted. Returning from `run()` here still runs the
        # loop's final `commit_checkpoint(force=True)`, which is exactly the
        # commit under test.
        app1.stop()

    sdf1.update(collect_and_stop, metadata=True)

    _produce(app1, topic1, [{"key": "D", "timestamp": 1}])
    app1.run(sdf1, timeout=APP1_IDLE_TIMEOUT)

    # app2 shares nothing with app1 but the changelog topic and the committed
    # consumer offset: a fresh state directory forces `do_recovery()` to
    # replay the buffer store from the changelog.
    collected2: list = []
    app2 = app_factory(
        consumer_group=consumer_group,
        state_dir=tmp_path / "state-app2-fresh",
        auto_offset_reset="earliest",
        commit_interval=1.0,
        processing_guarantee=processing_guarantee,
    )
    topic2 = _make_topic(app2, topic_name)
    lookup2 = FakeLookup()
    buffer2 = LookupBuffer(grace_ms=TICK_GRACE_MS, is_resolved=is_resolved)
    sdf2 = _attach_buffer(app2, topic2, lookup2, buffer2, collected2)
    app2.run(sdf2, timeout=APP2_IDLE_TIMEOUT)

    return collected1, collected2


class TestDeadlineTickDurability:
    """
    K8 / K9: the tick's deletes are durable, so a record resolved by the clock
    is never resolved twice.
    """

    def test_k8_tick_emission_is_not_repeated_after_restart(
        self, app_factory, tmp_path
    ):
        """
        K8, at-least-once. The single most important test in the change.

        RED before the fix: app1's tick-only checkpoint reports `empty()`
        (no offsets), is closed instead of committed, and the deletes never
        reach the changelog -- so app2 recovers the record and emits it a
        second time.
        """
        collected1, collected2 = _run_deadline_tick_roundtrip(
            app_factory, tmp_path, "at-least-once"
        )

        assert [timestamp for _, _, timestamp, _ in collected1] == [1], (
            "the record must be emitted exactly once by app1's deadline tick, "
            "with zero input messages after the one that buffered it -- this is "
            "the headline behaviour, and without it the rest of the test proves "
            "nothing"
        )
        assert collected2 == [], (
            "app2 recovered the buffer store from the changelog and must find "
            "it empty: app1's tick deleted the record, and that delete has to "
            "be committed in the same checkpoint that carried the emission. A "
            "record here means the tick-only checkpoint was discarded and the "
            "record would be re-emitted on every commit interval, forever, "
            "while the partition stays idle"
        )

    def test_k9_tick_emission_is_not_repeated_after_restart_exactly_once(
        self, app_factory, tmp_path
    ):
        """
        K9, exactly-once. Same property through the zero-offset EOS commit path.

        RED before the fix in two independent ways: the checkpoint is still
        reported empty (so it aborts rather than commits), and
        `commit_transaction` still calls `send_offsets_to_transaction` with an
        empty position list. An implementation that aborts instead of
        committing never makes progress and the tick livelocks silently, which
        is why `collected1` is asserted too.
        """
        collected1, collected2 = _run_deadline_tick_roundtrip(
            app_factory, tmp_path, "exactly-once"
        )

        assert [timestamp for _, _, timestamp, _ in collected1] == [1], (
            "under exactly-once the tick must still emit the record once; an "
            "implementation that aborts the tick-only transaction would emit "
            "nothing durable and repeat forever"
        )
        assert collected2 == [], (
            "the EOS transaction holding the tick's changelog deletes must be "
            "committed, not aborted -- otherwise app2 recovers the record and "
            "re-emits it"
        )
