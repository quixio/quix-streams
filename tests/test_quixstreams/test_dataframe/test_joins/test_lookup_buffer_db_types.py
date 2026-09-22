"""
The types a database lookup writes into a record value, through the buffer.

`PostgresLookup.join()` assigns column values straight into the record
(`postgresql.py:472,484`), and psycopg2 hands it a `datetime` for a
`timestamptz`, a `date` for a `date` and a `Decimal` for a `numeric`. They are
created inside the join, after any upstream normalization has run, so nothing
before the buffer can turn them into something the store's orjson can hold.

Before the envelope lifted them, the first such record to be withheld raised
`StateSerializationError` inside `PartitionTransaction.set()`
(`state/serialization.py:32-36`), which flips the transaction to `FAILED` and
aborts the whole checkpoint - every offset in it, and every other store's
writes. The tests below are red on that code: the envelope-level ones on
orjson's `TypeError`, `TestADatabaseValuedFieldReachesTheStore` on the
`StateSerializationError` the record path itself raises.

Named time zones are out of scope by construction: `isoformat()` carries the
offset, not the zone, so a `ZoneInfo` datetime comes back on a fixed offset.
Documented in `buffer_envelope.py`'s module docstring.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from quixstreams.dataframe.joins.lookups.buffer_envelope import (
    ENVELOPE_SCALARS,
    encode_envelope,
    envelope_value,
)
from quixstreams.state.exceptions import StateSerializationError
from quixstreams.utils.json import dumps as orjson_dumps
from quixstreams.utils.json import loads as orjson_loads
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    GRACE_MS,
    ConfigField,
    make_buffer,
    make_fields,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    buffered as buffered,
)
from tests.test_quixstreams.test_dataframe.test_joins.test_lookup_buffer import (
    clock as clock,
)

UTC_NOW = datetime(2026, 9, 17, 11, 2, 28, 123456, tzinfo=timezone.utc)
OFFSET_NOW = datetime(
    2026, 9, 17, 11, 2, 28, 123456, tzinfo=timezone(timedelta(hours=-5, minutes=-30))
)
NAIVE_NOW = datetime(2026, 9, 17, 11, 2, 28, 123456)


def _round_trip(value: Any) -> Any:
    """
    Put a value through the envelope and the store's real serializer.

    `orjson_dumps` is exactly what `PartitionTransaction.set()` calls, so a
    value that cannot survive this cannot be buffered at all.
    """
    envelope = encode_envelope(value=value, timestamp=10, receive_ms=20, headers=None)
    try:
        stored = orjson_dumps(envelope)
    except TypeError as exc:
        pytest.fail(
            f"The envelope for {value!r} is not serializable by the store's "
            f"own dumps: {exc}. On the record path this is a "
            f"StateSerializationError raised before the offset is committed, "
            f"taking the whole checkpoint with it."
        )
    return envelope_value(orjson_loads(stored))


class TestDatabaseScalarsSurviveTheEnvelope:
    @pytest.mark.parametrize(
        "value",
        [
            pytest.param({"seen": UTC_NOW}, id="datetime-utc"),
            pytest.param({"seen": OFFSET_NOW}, id="datetime-offset"),
            pytest.param({"seen": NAIVE_NOW}, id="datetime-naive"),
            pytest.param({"day": date(2026, 9, 17)}, id="date"),
            pytest.param({"amount": Decimal("1.10")}, id="decimal"),
            pytest.param({"amount": Decimal("-0.000001")}, id="decimal-small"),
            pytest.param({"amount": Decimal("1E+9")}, id="decimal-exponent"),
            pytest.param({"rows": [{"at": UTC_NOW}, {"at": NAIVE_NOW}]}, id="in-list"),
            pytest.param({"window": (UTC_NOW, NAIVE_NOW)}, id="in-tuple"),
            pytest.param({"days": {date(2026, 1, 2), date(2026, 1, 3)}}, id="in-set"),
            pytest.param(
                {"a": {"b": [{"c": (Decimal("3.00"), date(2026, 1, 2))}]}},
                id="four-levels-deep",
            ),
            pytest.param(
                {"cert": b"x", "at": UTC_NOW, "amount": Decimal("1.10")},
                id="alongside-bytes",
            ),
        ],
    )
    def test_types_survive_the_store_round_trip(self, value: Any) -> None:
        assert _round_trip(value) == value

    def test_a_bare_scalar_value_round_trips(self) -> None:
        """The empty path, the branch every restore helper has a case of its own for."""
        assert _round_trip(UTC_NOW) == UTC_NOW

    @pytest.mark.parametrize(
        "moment",
        [
            pytest.param(UTC_NOW, id="utc"),
            pytest.param(OFFSET_NOW, id="offset"),
        ],
    )
    def test_an_aware_datetime_keeps_its_offset(self, moment: datetime) -> None:
        restored = _round_trip({"seen": moment})["seen"]
        assert restored == moment
        assert restored.utcoffset() == moment.utcoffset()
        assert restored.microsecond == moment.microsecond

    def test_a_naive_datetime_stays_naive(self) -> None:
        """
        An offset invented on the way back would move the record in time and
        make it uncomparable with the naive datetimes around it.
        """
        restored = _round_trip({"seen": NAIVE_NOW})["seen"]
        assert restored == NAIVE_NOW
        assert restored.tzinfo is None

    def test_a_date_does_not_come_back_a_datetime(self) -> None:
        """
        `datetime` is a subclass of `date`, so the two have to be tagged apart.
        `date(...) == datetime(...)` is `False`, and the ordering comparisons
        between them raise.
        """
        restored = _round_trip({"day": date(2026, 9, 17)})["day"]
        assert restored == date(2026, 9, 17)
        assert type(restored) is date

    @pytest.mark.parametrize(
        "amount",
        [
            pytest.param(Decimal("1.10"), id="trailing-zero"),
            pytest.param(Decimal("0.000"), id="zero-with-scale"),
            pytest.param(Decimal("1E+9"), id="exponent"),
        ],
    )
    def test_a_decimal_keeps_its_scale(self, amount: Decimal) -> None:
        """
        `Decimal("1.10") == Decimal("1.1")`, so equality alone would pass a value
        that lost its scale - and scale is what a `numeric(10,2)` column carries.
        `as_tuple()` compares digits and exponent.
        """
        restored = _round_trip({"amount": amount})["amount"]
        assert type(restored) is Decimal
        assert restored.as_tuple() == amount.as_tuple()
        assert str(restored) == str(amount)

    def test_a_decimal_does_not_pass_through_float(self) -> None:
        """28 significant digits: a float holds 17."""
        amount = Decimal("0.1234567890123456789012345678")
        assert _round_trip({"amount": amount})["amount"] == amount

    def test_a_value_without_these_types_records_no_scalar_paths(self) -> None:
        """The lift stays inert for the ordinary record, JSON scalars included."""
        envelope = encode_envelope(
            value={"a": [1, "2026-09-17", None, True, 1.5]},
            timestamp=10,
            receive_ms=20,
            headers=None,
        )

        assert envelope[ENVELOPE_SCALARS] is None


class TestADatabaseValuedFieldReachesTheStore:
    """
    The gap through the real record path, which is where it costs the checkpoint.

    A field's `default` is what `missing()` returns for an unresolved record, so
    it is written into the value *before* the record is withheld - the same
    shape as a Postgres column value landing in the record inside `join()`.
    """

    def test_a_datetime_and_a_decimal_are_buffered_and_emitted_intact(
        self,
        clock: Any,  # noqa: F811 - re-exported fixture, see the import above
        buffered: Any,  # noqa: F811 - re-exported fixture, see the import above
    ) -> None:
        fields = {
            **make_fields(),
            "valid_from": ConfigField(
                type="device", default=UTC_NOW, source="valid_from"
            ),
            "limit": ConfigField(
                type="device", default=Decimal("1.10"), source="limit"
            ),
        }
        driver = buffered(buffer=make_buffer(), fields=fields)

        # Unresolvable, so it is withheld - and withholding it serializes it.
        try:
            assert driver.send("D", timestamp=1) == []
        except StateSerializationError as exc:
            pytest.fail(
                f"Withholding a record carrying a datetime and a Decimal failed "
                f"the transaction: {exc}. The offset stays uncommitted and the "
                f"checkpoint aborts with it."
            )

        # Nothing configures it, so it leaves through the timeout path, which
        # emits the stored value verbatim. Another key's record drives the sweep.
        clock.advance_ms(GRACE_MS + 1)
        emitted = driver.send("other", timestamp=2)

        assert len(emitted) == 1, f"expected the timed-out record only: {emitted}"
        value, key, timestamp, _ = emitted[0]
        assert key == "D"
        assert timestamp == 1
        assert value["valid_from"] == UTC_NOW
        assert value["valid_from"].utcoffset() == UTC_NOW.utcoffset()
        assert value["limit"].as_tuple() == Decimal("1.10").as_tuple()
