"""
Tests for `join_lookup(..., buffer=LookupBuffer(...))`.

The buffer withholds records whose lookup cannot be resolved yet and releases
them when the next record for the same key resolves, or settles them when they
run out of grace. Every test here drives a real `TimestampedStore` through a real
`StreamingDataFrame`, with wall-clock time replaced by a fake clock so the
deadlines are deterministic.

Message keys are `str` wherever the lookup key *is* the message key (`on=None`),
because `FakeLookup.configs` is keyed by the lookup key and the real
`QuixConfigurationService` turns that key into a configuration id with
`sha1(f"{type}-{key}")` (`quix_configuration_service/lookup.py:337-346`) - which
only ever matches a configuration published for a `str` target key. `bytes` keys
appear exactly where the key is nothing but a store prefix: the sweep tests,
whose silent keys are never configured, and `TestLookupKeyResolution`, where
`on=` reads the lookup key out of the value instead.
"""

import dataclasses
from datetime import timedelta
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from quixstreams.dataframe.joins import lookups
from quixstreams.dataframe.joins.lookups import LookupBuffer
from quixstreams.dataframe.joins.lookups.base import BaseLookup
from quixstreams.dataframe.joins.lookups.buffer_operator import (
    LookupBufferOverflowError,
)
from quixstreams.dataframe.joins.lookups.buffer_state import (
    PendingIndex,
    prefix_for_key,
)
from quixstreams.dataframe.joins.lookups.buffer_sweep import (
    MAX_RECEIVE_MS,
    SWEEP_BUDGET,
)
from quixstreams.dataframe.joins.lookups.quix_configuration_service.models import (
    BaseField as ConfigBaseField,
)
from quixstreams.state import StateStoreManager

STORE_NAME = "lookup-buffer"
UNRESOLVED = "__unresolved__"
GRACE_MS = 1_000


class FakeClock:
    """A stand-in for the `time` module inside the buffer operator."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = start

    def time(self) -> float:
        return self._now

    def monotonic(self) -> float:
        return self._now

    def advance_ms(self, milliseconds: int) -> None:
        self._now += milliseconds / 1000


@dataclasses.dataclass(frozen=True)
class ConfigField(ConfigBaseField):
    """
    A Quix Configuration Service field with the real `default` / `missing()`
    semantics, reading its value out of a plain dict instead of JSON content.
    """

    source: str = ""

    def parse(self, id: str, version: int, content: Any) -> Any:
        return content[self.source]


class FakeLookup(BaseLookup):
    """
    A lookup whose "no configuration" path has the same shape as
    `QuixConfigurationService`: every field resolves through `missing()`, and the
    unresolved configuration types are written to the record.

    `configs` is keyed by the **lookup key** - the value `join_lookup` computes
    through `on` and hands to `join()` - and looked up by equality, the same way
    the real service hashes that key into a configuration id without coercing it.
    So a key of the wrong type simply does not resolve, again like the real one.
    """

    def __init__(self) -> None:
        self.configs: dict[str, dict[str, Any]] = {}
        self.joined: list[tuple[Any, int]] = []

    def join(self, fields, on, value, key, timestamp, headers) -> None:
        self.joined.append((on, timestamp))
        content = self.configs.get(on)
        for name, field in fields.items():
            if content is None:
                value[name] = field.missing()
            else:
                value[name] = field.parse("id", 1, content)
        value[UNRESOLVED] = (
            [] if content is not None else sorted({f.type for f in fields.values()})
        )


def is_resolved(value: dict[str, Any]) -> bool:
    return not value[UNRESOLVED]


def make_fields(threshold_default: Any = None) -> dict[str, ConfigField]:
    return {
        "threshold": ConfigField(
            type="device", default=threshold_default, source="threshold"
        ),
        "region": ConfigField(type="device", default="unknown", source="region"),
    }


def make_buffer(**kwargs) -> LookupBuffer:
    kwargs.setdefault("grace_ms", GRACE_MS)
    kwargs.setdefault("is_resolved", is_resolved)
    return LookupBuffer(**kwargs)


@dataclasses.dataclass
class _Driver:
    """A small harness around one buffered `StreamingDataFrame`."""

    sdf: Any
    topic: Any
    lookup: FakeLookup
    publish: Any

    def send(self, key, timestamp=0, value=None, headers=None) -> list:
        return self.publish(
            self.sdf,
            self.topic,
            dict(value) if value else {},
            key,
            timestamp,
            headers,
        )

    def transaction(self):
        return self.sdf.processing_context.checkpoint.get_store_transaction(
            stream_id=self.sdf.stream_id,
            partition=0,
            store_name=STORE_NAME,
        )

    def stored(self, key) -> list:
        return self.transaction().get_interval(
            start=0, end=MAX_RECEIVE_MS, prefix=prefix_for_key(key)
        )

    def pending_keys(self) -> list:
        return list(PendingIndex(self.transaction()).entries())

    def flushed(self, key) -> list:
        """Flush the current transaction and read the key back from the store."""
        transaction = self.transaction()
        transaction.prepare()
        transaction.flush()
        store = self.sdf.processing_context.state_manager.get_store(
            stream_id=self.sdf.stream_id, store_name=STORE_NAME
        )
        with store.start_partition_transaction(partition=0) as fresh:
            return fresh.get_interval(
                start=0, end=MAX_RECEIVE_MS, prefix=prefix_for_key(key)
            )


@pytest.fixture
def clock(monkeypatch):
    fake = FakeClock()
    monkeypatch.setattr(lookups.buffer_operator, "time", fake)
    return fake


@pytest.fixture
def buffered(topic_manager_topic_factory, create_sdf, assign_partition, publish):
    """Build a `StreamingDataFrame` with a lookup buffer attached."""

    def _factory(
        buffer: Optional[LookupBuffer] = None,
        fields: Optional[dict] = None,
        on=None,
    ) -> _Driver:
        topic = topic_manager_topic_factory()
        sdf = create_sdf(topic)
        lookup = FakeLookup()
        sdf = sdf.join_lookup(
            lookup,
            fields if fields is not None else make_fields(),
            on=on,
            buffer=buffer,
        )
        assign_partition(sdf)
        return _Driver(sdf=sdf, topic=topic, lookup=lookup, publish=publish)

    return _factory


class TestLookupBufferConstruction:
    def test_grace_ms_must_be_positive(self):
        with pytest.raises(ValueError, match="grace_ms"):
            LookupBuffer(grace_ms=0, is_resolved=is_resolved)

    def test_timedelta_grace_ms_is_converted_to_milliseconds(self):
        assert make_buffer(grace_ms=timedelta(seconds=2)).grace_ms == 2_000

    def test_invalid_on_timeout(self):
        with pytest.raises(ValueError, match="on_timeout"):
            make_buffer(on_timeout="explode")

    def test_invalid_on_overflow(self):
        with pytest.raises(ValueError, match="on_overflow"):
            make_buffer(on_overflow="explode")

    def test_max_buffered_per_key_must_be_positive(self):
        with pytest.raises(ValueError, match="max_buffered_per_key"):
            make_buffer(max_buffered_per_key=0)


class TestFieldValidation:
    """T7: a field with no `default=` is rejected at build time, not in prod."""

    @pytest.mark.parametrize("on_timeout", ["emit", "drop"])
    def test_field_without_default_is_rejected(self, buffered, on_timeout):
        fields = make_fields()
        # `ConfigField` falls back to RAISE_ON_MISSING when `default` is omitted.
        fields["no_default"] = ConfigField(type="device", source="threshold")

        with pytest.raises(ValueError, match="no_default"):
            buffered(buffer=make_buffer(on_timeout=on_timeout), fields=fields)

    def test_field_without_default_is_allowed_without_a_buffer(self, buffered):
        fields = {"no_default": ConfigField(type="device", source="threshold")}
        driver = buffered(buffer=None, fields=fields)
        driver.lookup.configs["D"] = {"threshold": 7}

        assert driver.send("D")[0][0]["no_default"] == 7


class TestUnbufferedPathIsUnchanged:
    """T12: `buffer=None` is byte-for-byte today's behaviour."""

    def test_no_store_is_registered(
        self, topic_manager_topic_factory, dataframe_factory
    ):
        state_manager = MagicMock(spec=StateStoreManager)
        sdf = dataframe_factory(
            topic_manager_topic_factory(), state_manager=state_manager
        )

        sdf.join_lookup(FakeLookup(), make_fields())

        state_manager.register_timestamped_store.assert_not_called()

    def test_unresolved_record_is_emitted_immediately(self, buffered):
        driver = buffered(buffer=None)

        result = driver.send("D", timestamp=100)

        assert len(result) == 1
        value, key, timestamp, _ = result[0]
        assert key == "D"
        assert timestamp == 100
        assert value["threshold"] is None
        assert value["region"] == "unknown"


class TestBuffering:
    def test_store_is_registered_with_a_changelog_config(
        self, topic_manager_topic_factory, dataframe_factory
    ):
        state_manager = MagicMock(spec=StateStoreManager)
        sdf = dataframe_factory(
            topic_manager_topic_factory(), state_manager=state_manager
        )

        sdf.join_lookup(FakeLookup(), make_fields(), buffer=make_buffer())

        kwargs = state_manager.register_timestamped_store.call_args.kwargs
        assert kwargs["store_name"] == STORE_NAME
        assert kwargs["grace_ms"] == GRACE_MS
        assert kwargs["keep_duplicates"] is True
        assert kwargs["changelog_config"] is not None

    def test_unresolved_record_is_withheld(self, clock, buffered):
        """T1: nothing is emitted and the record is in the store."""
        driver = buffered(buffer=make_buffer())

        assert driver.send("D", timestamp=100) == []

        stored = driver.stored("D")
        assert len(stored) == 1
        assert stored[0]["t"] == 100
        assert driver.pending_keys()

    def test_resolvable_record_passes_straight_through(self, clock, buffered):
        driver = buffered(buffer=make_buffer())
        driver.lookup.configs["A"] = {"threshold": 5, "region": "eu"}

        result = driver.send("A", timestamp=100)

        assert len(result) == 1
        assert result[0][0]["threshold"] == 5
        assert driver.stored("A") == []
        assert driver.pending_keys() == []

    def test_unsupported_key_type_is_rejected(self, clock, buffered):
        driver = buffered(buffer=make_buffer())

        with pytest.raises(ValueError, match="message key"):
            driver.send(12345)


class TestRelease:
    def test_survivors_are_released_enriched_and_in_order(self, clock, buffered):
        """T3: all withheld records emit ahead of the releasing one."""
        driver = buffered(buffer=make_buffer())

        for index in range(5):
            clock.advance_ms(10)
            assert driver.send("D", timestamp=index, headers=[("n", str(index))]) == []

        driver.lookup.configs["D"] = {"threshold": 42, "region": "eu"}
        clock.advance_ms(10)
        result = driver.send("D", timestamp=5, headers=[("n", "5")])

        assert len(result) == 6
        assert [timestamp for _, _, timestamp, _ in result] == [0, 1, 2, 3, 4, 5]
        assert [key for _, key, _, _ in result] == ["D"] * 6
        assert [headers for *_, headers in result] == [
            [("n", str(index))] for index in range(6)
        ]
        assert all(value["threshold"] == 42 for value, *_ in result)
        assert driver.stored("D") == []
        assert driver.pending_keys() == []

    def test_bytes_headers_survive_the_round_trip(self, clock, buffered):
        driver = buffered(buffer=make_buffer())

        clock.advance_ms(10)
        assert driver.send("D", timestamp=1, headers=[("n", b"\x00\xff")]) == []

        driver.lookup.configs["D"] = {"threshold": 1, "region": "eu"}
        clock.advance_ms(10)
        result = driver.send("D", timestamp=2)

        assert result[0][3] == [("n", b"\x00\xff")]

    def test_resolvable_record_queues_behind_withheld_ones(self, clock, buffered):
        """T10: once buffered, always buffered until drained."""
        driver = buffered(buffer=make_buffer())

        clock.advance_ms(10)
        assert driver.send("D", timestamp=1) == []

        driver.lookup.configs["D"] = {"threshold": 1, "region": "eu"}
        clock.advance_ms(10)
        result = driver.send("D", timestamp=2)

        assert [timestamp for _, _, timestamp, _ in result] == [1, 2]


class TestTimeoutEmit:
    def test_timed_out_record_emits_with_its_declared_defaults(self, clock, buffered):
        """T5: `on_timeout="emit"` resolves through `missing()`."""
        driver = buffered(buffer=make_buffer())
        unbuffered = buffered(buffer=None)

        clock.advance_ms(10)
        assert driver.send("D", timestamp=1, headers=[("n", "1")]) == []

        clock.advance_ms(GRACE_MS + 1)
        result = driver.send("D", timestamp=2)

        assert len(result) == 1
        value, key, timestamp, headers = result[0]
        assert key == "D"
        assert timestamp == 1
        assert headers == [("n", "1")]
        assert value["threshold"] is None
        assert value["region"] == "unknown"

        # The same record through the unbuffered path must look identical.
        expected = unbuffered.send("D", timestamp=1, headers=[("n", "1")])[0][0]
        assert value == expected

    def test_timed_out_record_is_not_enriched_afterwards(self, clock, buffered):
        """T6: a configuration arriving after the deadline changes nothing."""
        driver = buffered(buffer=make_buffer())

        clock.advance_ms(10)
        assert driver.send("D", timestamp=1) == []

        clock.advance_ms(GRACE_MS + 1)
        driver.lookup.configs["D"] = {"threshold": 99, "region": "eu"}
        result = driver.send("D", timestamp=2)

        assert len(result) == 2
        timed_out, current = result
        assert timed_out[2] == 1
        assert timed_out[0]["threshold"] is None
        assert current[0]["threshold"] == 99

    def test_timed_out_records_emit_before_survivors(self, clock, buffered):
        driver = buffered(buffer=make_buffer())

        clock.advance_ms(10)
        assert driver.send("D", timestamp=1) == []
        clock.advance_ms(10)
        assert driver.send("D", timestamp=2) == []

        driver.lookup.configs["D"] = {"threshold": 7, "region": "eu"}
        # Past the first record's deadline, inside the second's.
        clock.advance_ms(GRACE_MS - 5)
        result = driver.send("D", timestamp=3)

        assert [timestamp for _, _, timestamp, _ in result] == [1, 2, 3]
        assert [value["threshold"] for value, *_ in result] == [None, 7, 7]


class TestTimeoutDrop:
    def test_timed_out_record_is_dropped_permanently(self, clock, buffered):
        """T4: nothing is emitted for it, then or ever."""
        driver = buffered(buffer=make_buffer(on_timeout="drop"))

        clock.advance_ms(10)
        assert driver.send("D", timestamp=1) == []

        clock.advance_ms(GRACE_MS + 1)
        assert driver.send("D", timestamp=2) == []
        assert [envelope["t"] for envelope in driver.stored("D")] == [2]

        driver.lookup.configs["D"] = {"threshold": 3, "region": "eu"}
        clock.advance_ms(10)
        result = driver.send("D", timestamp=3)

        assert [timestamp for _, _, timestamp, _ in result] == [2, 3]


class TestSweep:
    def test_silent_key_is_drained_by_other_keys_traffic(self, clock, buffered):
        """T8: the sweep emits a silent key's records under `"emit"`."""
        driver = buffered(buffer=make_buffer())
        driver.lookup.configs["A"] = {"threshold": 1, "region": "eu"}

        # The silent key stays `bytes`: it is never configured, so it only ever
        # serves as a store prefix, and emitting it back exercises the `bytes`
        # branch of the sweep's `key_from_prefix()` reconstruction.
        clock.advance_ms(10)
        for timestamp in (1, 2, 3):
            assert driver.send(b"D", timestamp=timestamp) == []

        clock.advance_ms(GRACE_MS + 1)
        result = driver.send("A", timestamp=10)

        swept = [record for record in result if record[1] == b"D"]
        assert [timestamp for _, _, timestamp, _ in swept] == [1, 2, 3]
        assert all(value["threshold"] is None for value, *_ in swept)
        assert driver.stored(b"D") == []
        assert driver.pending_keys() == []

    def test_silent_key_is_dropped_by_other_keys_traffic(self, clock, buffered):
        driver = buffered(buffer=make_buffer(on_timeout="drop"))
        driver.lookup.configs["A"] = {"threshold": 1, "region": "eu"}

        clock.advance_ms(10)
        for timestamp in (1, 2, 3):
            assert driver.send(b"D", timestamp=timestamp) == []

        clock.advance_ms(GRACE_MS + 1)
        result = driver.send("A", timestamp=10)

        assert [key for _, key, _, _ in result] == ["A"]
        assert driver.stored(b"D") == []
        assert driver.pending_keys() == []

    def test_sweep_cursor_starves_nothing(self, clock, buffered):
        """Every pending prefix is eventually swept, `SWEEP_BUDGET` at a time."""
        driver = buffered(buffer=make_buffer())
        driver.lookup.configs["A"] = {"threshold": 1, "region": "eu"}
        # Unconfigured `bytes` keys: nothing about them is meant to resolve, they
        # only have to come back out of the sweep under their own key.
        pending = [f"key-{index}".encode() for index in range(50)]

        clock.advance_ms(10)
        for key in pending:
            assert driver.send(key, timestamp=1) == []
        assert len(driver.pending_keys()) == len(pending)

        clock.advance_ms(GRACE_MS + 1)
        swept_keys = set()
        for _ in range(len(pending) // SWEEP_BUDGET + 2):
            for _, key, _, _ in driver.send("A", timestamp=2):
                if key != "A":
                    swept_keys.add(key)

        assert swept_keys == set(pending)
        assert driver.pending_keys() == []


class TestOverflow:
    def test_drop_newest_bounds_the_buffer(self, clock, buffered):
        """T11: overflowed records are dropped and never emitted."""
        driver = buffered(
            buffer=make_buffer(max_buffered_per_key=3, on_overflow="drop-newest")
        )

        for timestamp in range(1, 7):
            clock.advance_ms(1)
            assert driver.send("D", timestamp=timestamp) == []

        assert [envelope["t"] for envelope in driver.stored("D")] == [1, 2, 3]

        driver.lookup.configs["D"] = {"threshold": 1, "region": "eu"}
        clock.advance_ms(1)
        result = driver.send("D", timestamp=7)

        assert [timestamp for _, _, timestamp, _ in result] == [1, 2, 3, 7]

    def test_raise_fails_loudly(self, clock, buffered):
        driver = buffered(
            buffer=make_buffer(max_buffered_per_key=1, on_overflow="raise")
        )

        clock.advance_ms(1)
        assert driver.send("D", timestamp=1) == []

        clock.advance_ms(1)
        with pytest.raises(LookupBufferOverflowError, match="max_buffered_per_key"):
            driver.send("D", timestamp=2)


class TestDurability:
    def test_buffer_outlives_the_transaction_it_was_written_in(self, clock, buffered):
        """
        T9, the part that needs no broker: the withheld records live in the state
        store, not in memory, so a fresh transaction still sees them with their
        own arrival and event times.
        """
        driver = buffered(buffer=make_buffer())

        clock.advance_ms(10)
        for timestamp in (1, 2, 3):
            assert driver.send("D", timestamp=timestamp) == []

        assert [envelope["t"] for envelope in driver.flushed("D")] == [1, 2, 3]

    def test_passive_expiry_never_deletes_an_unemitted_record(self, clock, buffered):
        """
        The store's own `_expire()` is a safety net over the same receive-time
        stamps. It must never delete a record the operator has not settled: the
        operator always drains `[0, cutoff + 1)` before writing, and the floor
        that `set_for_timestamp()` raises is exactly that cutoff.
        """
        driver = buffered(buffer=make_buffer())

        clock.advance_ms(10)
        assert driver.send("D", timestamp=1) == []
        clock.advance_ms(GRACE_MS // 2)
        assert driver.send("D", timestamp=2) == []

        clock.advance_ms(GRACE_MS // 2 + 1)
        emitted = driver.send("D", timestamp=3)

        assert [timestamp for _, _, timestamp, _ in emitted] == [1]
        assert [envelope["t"] for envelope in driver.flushed("D")] == [2, 3]


class TestLookupKeyResolution:
    def test_each_survivor_is_rejoined_under_its_own_lookup_key(self, clock, buffered):
        """
        With `on="device"` the lookup key comes from the value, so two records
        sharing a message key can need different configurations. The message key
        is `bytes` here precisely because it is not the lookup key: it only
        selects the store prefix the two records queue under.
        """
        driver = buffered(buffer=make_buffer(), on="device")

        clock.advance_ms(10)
        assert driver.send(b"D", timestamp=1, value={"device": "one"}) == []

        driver.lookup.configs["one"] = {"threshold": 1, "region": "eu"}
        driver.lookup.configs["two"] = {"threshold": 2, "region": "us"}
        clock.advance_ms(10)
        result = driver.send(b"D", timestamp=2, value={"device": "two"})

        assert [value["threshold"] for value, *_ in result] == [1, 2]
