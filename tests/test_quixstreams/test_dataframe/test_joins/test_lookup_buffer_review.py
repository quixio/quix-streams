"""
Red-first reproduction tests for the external review of PR #1110.

Each class names the finding it reproduces and states how it fails on the
unfixed code. The numbering is the review's.

1. The deadline tick cached `NO_DEADLINE` for a partition it observed *before*
   changelog recovery filled it, and `_resync`'s identity check could never see
   the contents change under the same `StorePartition` object. Every restart of
   a service holding a backlog therefore stranded it.
2. `QuixConfigurationService.join()` recorded a type as unresolved only when no
   version was found, so a version whose content could not be fetched produced
   an all-defaults record that `is_resolved` called resolved.
3. `prefix_for_key` raises on a non-`bytes`/`str` key, at a point where the
   offset is not yet stored - an uncommitted crash loop on record one, with no
   build-time guard.
4. The record path lowered a key's earliest-arrival marker to `cutoff + 1` even
   when nothing had timed out, re-queueing the key as due earlier than it is.
5. Per-key counts were never pruned, so a re-assigned partition judged overflow
   against a store holding nothing.
6. The record path emitted buffered records under the *triggering* record's
   message context, so a sink keyed on `(topic, partition, offset)` saw several
   records tagged with one origin.
7. `_release()` read a key's whole surviving buffer with no limit and ran
   `lookup.join` and `is_resolved` on every envelope in one callback.
"""

from typing import Any, Optional
from unittest.mock import Mock, patch

import httpx
import pytest

from quixstreams.dataframe.joins.lookups.buffer_bookkeeping import BufferBookkeeping
from quixstreams.dataframe.joins.lookups.buffer_envelope import encode_envelope
from quixstreams.dataframe.joins.lookups.buffer_state import (
    PendingIndex,
    prefix_for_key,
)
from quixstreams.dataframe.joins.lookups.quix_configuration_service.lookup import (
    Lookup as QuixLookup,
)
from quixstreams.dataframe.joins.lookups.quix_configuration_service.models import (
    Configuration,
    ConfigurationVersion,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    GRACE_MS,
    FakeLookup,
    make_buffer,
    make_fields,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer_tick import (
    clock,
    tick_driver,
)

# Re-exported so pytest can resolve these fixtures by name in this module too.
__all__ = ["clock", "tick_driver"]

LOOKUP_MODULE = "quixstreams.dataframe.joins.lookups.quix_configuration_service.lookup"

CONFIG = {"threshold": 5, "region": "eu"}


class FakeRecoveryManager:
    """
    Stands in for the changelog `RecoveryManager`.

    `StateStoreManager.recovery_required` reads nothing but `has_assignments`,
    and `on_partition_assign` calls `assign_partition`.
    """

    def __init__(self) -> None:
        self.has_assignments = True

    def assign_partition(self, **_: Any) -> None:
        return None

    def revoke_partition(self, **_: Any) -> None:
        return None


def now_ms(clock) -> int:
    return int(clock.time() * 1000)


def recover_record(
    driver,
    key: str,
    receive_ms: int,
    timestamp: int,
    value: Optional[dict] = None,
    offset: int = 0,
    partition: int = 0,
) -> None:
    """
    Write one withheld record into the store the way changelog recovery does:
    the envelope plus its index entry, and nothing in any in-memory structure.
    """
    transaction = driver.transaction(partition)
    prefix = prefix_for_key(key)
    transaction.set_for_timestamp(
        timestamp=receive_ms,
        value=encode_envelope(
            value=dict(value or {}),
            timestamp=timestamp,
            receive_ms=receive_ms,
            headers=None,
            topic=driver.topic.name,
            offset=offset,
        ),
        prefix=prefix,
    )
    index = PendingIndex(transaction)
    index.ensure(prefix, key, receive_ms)
    index.flush()


def marker(driver, key: str, partition: int = 0) -> Optional[list]:
    return PendingIndex(driver.transaction(partition)).get(prefix_for_key(key))


def quix_lookup(**kwargs) -> QuixLookup:
    """A `QuixConfigurationService` with its Kafka consumer and HTTP client faked."""
    topic = Mock()
    topic.name = "configurations"
    with (
        patch(f"{LOOKUP_MODULE}.Consumer"),
        patch(f"{LOOKUP_MODULE}.httpx.Client"),
        patch.object(QuixLookup, "_start_consumer_thread"),
    ):
        lookup = QuixLookup(
            topic=topic,
            broker_address="dummy:9092",
            consumer_poll_timeout=1.0,
            **kwargs,
        )
    lookup._configurations = {}
    return lookup


def publish_configuration(lookup: QuixLookup, type_: str, on: str) -> None:
    version = ConfigurationVersion(
        id="configuration-1",
        version=1,
        contentUrl="http://configurations.test/1",
        sha256sum="hash",
        valid_from=0,
    )
    lookup._configurations[lookup._config_id(type_, on)] = Configuration(
        versions={1: version}
    )


class TestFinding1RecoveredBacklog:
    """
    The tick observes a partition that is assigned but not yet recovered, and
    must not conclude from an empty store that the partition has no deadline.

    Without the fix the second tick emits nothing: the first tick cached
    `NO_DEADLINE` and `partitions != self._known` is False forever after, so
    `_resync` never re-seeds the partition it already knows.
    """

    def test_a_backlog_recovered_after_the_first_tick_is_still_settled(
        self, clock, tick_driver
    ):
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        recovery = FakeRecoveryManager()
        driver.state_manager._recovery_manager = recovery

        # The run loop reaches run_periodic_tasks() in the same iteration whose
        # poll() assigned the partition, before do_recovery() has run.
        assert driver.tick() == []

        recovery.has_assignments = False
        recover_record(driver, "D", receive_ms=now_ms(clock), timestamp=7)

        clock.advance_ms(GRACE_MS + 1)
        assert [record[2] for record in driver.tick()] == [7]

    def test_the_tick_is_inert_while_recovery_is_pending(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        assert driver.send("D", timestamp=1) == []

        driver.state_manager._recovery_manager = FakeRecoveryManager()
        clock.advance_ms(GRACE_MS + 1)
        assert driver.tick() == []

        driver.state_manager._recovery_manager = None
        assert [record[2] for record in driver.tick()] == [1]


class TestFinding2ContentFetchFailure:
    """
    A configuration version that exists but whose content cannot be fetched is
    an unresolved type, not a resolved record carrying every default.

    Without the fix `__unresolved__` is `[]` and the documented
    `is_resolved=lambda v: not v["__unresolved__"]` sends the record downstream
    unbuffered.
    """

    def test_a_failed_content_fetch_is_reported_unresolved(self):
        lookup = quix_lookup(
            fallback="default", unresolved_types_field="__unresolved__"
        )
        lookup._client.get.side_effect = httpx.ConnectError("content url is down")
        publish_configuration(lookup, "device", "D")

        fields = {
            "threshold": lookup.json_field("$.threshold", type="device", default=None)
        }
        value: dict[str, Any] = {}
        lookup.join(fields, "D", value, b"D", 1_000, {})

        assert value["threshold"] is None
        assert value["__unresolved__"] == ["device"]

    def test_a_cached_failure_stays_unresolved_until_the_retry_is_due(self):
        lookup = quix_lookup(
            fallback="default", unresolved_types_field="__unresolved__"
        )
        lookup._client.get.side_effect = httpx.ConnectError("content url is down")
        publish_configuration(lookup, "device", "D")

        fields = {
            "threshold": lookup.json_field("$.threshold", type="device", default=None)
        }
        for _ in range(3):
            value: dict[str, Any] = {}
            lookup.join(fields, "D", value, b"D", 1_000, {})
            assert value["__unresolved__"] == ["device"]

    def test_a_resolved_type_is_not_reported_unresolved(self):
        lookup = quix_lookup(
            fallback="default", unresolved_types_field="__unresolved__"
        )
        lookup._client.get.return_value.content = b'{"threshold": 5}'
        publish_configuration(lookup, "device", "D")

        fields = {
            "threshold": lookup.json_field("$.threshold", type="device", default=None)
        }
        value: dict[str, Any] = {}
        lookup.join(fields, "D", value, b"D", 1_000, {})

        assert value["threshold"] == 5
        assert value["__unresolved__"] == []


class TestFinding3KeyDeserializer:
    """
    A key deserializer that cannot produce `bytes`, `str` or `None` is rejected
    where `validate_fields` rejects a field without a default: at build time.

    Without the fix `join_lookup` builds happily and `prefix_for_key` raises on
    the first record, before the offset is stored - a permanent redelivery loop.
    """

    @pytest.mark.parametrize("key_deserializer", ["int", "integer", "double", "json"])
    def test_a_non_string_key_deserializer_is_rejected(
        self, create_sdf, topic_manager_topic_factory, key_deserializer
    ):
        topic = topic_manager_topic_factory(key_deserializer=key_deserializer)
        sdf = create_sdf(topic)

        with pytest.raises(ValueError, match="message keys"):
            sdf.join_lookup(FakeLookup(), make_fields(), buffer=make_buffer())

    @pytest.mark.parametrize("key_deserializer", ["bytes", "str"])
    def test_a_string_key_deserializer_is_accepted(
        self, create_sdf, topic_manager_topic_factory, key_deserializer
    ):
        topic = topic_manager_topic_factory(key_deserializer=key_deserializer)
        sdf = create_sdf(topic)

        sdf.join_lookup(FakeLookup(), make_fields(), buffer=make_buffer())

    def test_an_unbuffered_lookup_accepts_any_key_deserializer(
        self, create_sdf, topic_manager_topic_factory
    ):
        topic = topic_manager_topic_factory(key_deserializer="int")
        sdf = create_sdf(topic)

        sdf.join_lookup(FakeLookup(), make_fields())


class TestFinding4EarliestMarker:
    """
    A second unresolved record for a key whose buffer nothing has drained must
    not lower that key's earliest-arrival marker.

    Without the fix the marker is written as `cutoff + 1`, which is below every
    record still in the buffer, so the key is re-queued as due earlier than it
    is and the next sweep pays a full unbounded re-index for nothing.
    """

    def test_a_second_record_keeps_the_true_earliest_arrival(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        first_receive_ms = now_ms(clock)

        assert driver.send("D", timestamp=1) == []
        clock.advance_ms(10)
        assert driver.send("D", timestamp=2) == []

        assert marker(driver, "D")[0] == first_receive_ms

    def test_a_drained_key_still_takes_the_conservative_bound(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer(on_timeout="drop"))
        assert driver.send("D", timestamp=1) == []

        clock.advance_ms(GRACE_MS + 10)
        cutoff = now_ms(clock) - GRACE_MS
        assert driver.send("D", timestamp=2) == []

        assert marker(driver, "D")[0] == cutoff + 1


class TestFinding5ReassignedPartition:
    """
    Per-key counts belong to an assignment, not to a partition number.

    Without the fix the count survives the revoke, and the first record after
    the re-assignment is judged against a buffer that no longer exists:
    `LookupBufferOverflowError` against an empty store.
    """

    def test_a_reassigned_partition_starts_from_an_empty_count(
        self, clock, tick_driver
    ):
        driver = tick_driver(
            buffer=make_buffer(max_buffered_per_key=2, on_overflow="raise")
        )
        for timestamp in (1, 2):
            assert driver.send("D", timestamp=timestamp) == []
        assert len(driver.stored("D")) == 2

        # The partition moves away, its buffer is drained by the new owner, and
        # it comes back with the changelog's empty state.
        driver.state_manager.destroy_partition_state(driver.sdf.stream_id, 0)
        driver.state_manager.on_partition_assign(
            stream_id=driver.sdf.stream_id,
            partition=0,
            committed_offsets={},
        )
        driver.sdf.processing_context.init_checkpoint()
        driver.tick()

        assert driver.send("D", timestamp=3) == []
        assert len(driver.stored("D")) == 1

    def test_reading_a_count_does_not_remember_the_partition(self):
        bookkeeping = BufferBookkeeping()

        assert bookkeeping.count(7, b"D") == 0
        assert bookkeeping._counts == {}

    def test_forget_drops_every_key_of_a_partition(self):
        bookkeeping = BufferBookkeeping()
        bookkeeping.set_count(7, b"D", 3)
        bookkeeping.set_count(8, b"D", 4)

        bookkeeping.forget(7)

        assert bookkeeping.count(7, b"D") == 0
        assert bookkeeping.count(8, b"D") == 4


class TestFinding6EmissionOrigin:
    """
    A record released by the record path carries its own origin, exactly as the
    tick path already gives it.

    Without the fix every released record is emitted under the triggering
    record's context, so a sink naming files or deduping by
    `(topic, partition, offset)` collides or drops.
    """

    def test_released_records_carry_their_own_offset(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        assert driver.send("D", timestamp=1, offset=10) == []
        assert driver.send("D", timestamp=2, offset=11) == []

        driver.lookup.configs["D"] = CONFIG
        released = driver.send("D", timestamp=3, offset=12)

        assert [record[2] for record in released] == [1, 2, 3]
        assert [context.offset for context in driver.contexts] == [10, 11, 12]
        assert {context.topic for context in driver.contexts} == {driver.topic.name}

    def test_timed_out_records_carry_their_own_offset(self, clock, tick_driver):
        driver = tick_driver(buffer=make_buffer(on_timeout="emit"))
        assert driver.send("D", timestamp=1, offset=10) == []

        clock.advance_ms(GRACE_MS + 1)
        driver.lookup.configs["E"] = CONFIG
        emitted = driver.send("E", timestamp=2, offset=11)

        assert [record[2] for record in emitted] == [1, 2]
        assert [context.offset for context in driver.contexts] == [10, 11]
