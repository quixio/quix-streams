import base64
import json
import logging
import ssl
import time
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")

from pymysql.err import InterfaceError, OperationalError

from quixstreams.models.messages import KafkaMessage
from quixstreams.sources.community.mysql_cdc import mysql_helper
from quixstreams.sources.community.mysql_cdc.config import TlsConfig
from quixstreams.sources.community.mysql_cdc.mysql_cdc import (
    MySqlCdcError,
    MySqlCdcSource,
    _derive_server_id,
)
from quixstreams.sources.community.mysql_cdc.mysql_helper import is_connection_error
from quixstreams.sources.community.mysql_cdc.snapshot import (
    build_snapshot_query,
    is_checkpointable_key,
    quote_identifier,
)
from quixstreams.sources.community.mysql_cdc.values import (
    ColumnType,
    serialize_binlog_values,
    serialize_row,
    serialize_value,
)


def _source_kwargs(**overrides):
    kwargs = {
        "host": "localhost",
        "user": "cdc_user",
        "password": "cdc_password",
        "database": "db",
        "table": "tbl",
    }
    kwargs.update(overrides)
    return kwargs


def _helper_kwargs(**overrides):
    kwargs = {
        "host": "localhost",
        "port": 3306,
        "user": "cdc_user",
        "password": "cdc_password",
        "database": "db",
        "table": "tbl",
        "snapshot_host": "localhost",
        "tls": TlsConfig(),
    }
    kwargs.update(overrides)
    return kwargs


class _FakeShowVariableCursor:
    """
    Stand-in for a DB-API cursor that only ever answers `SHOW VARIABLES LIKE %s`.

    Used to drive `MySqlHelper._require_row_metadata`/`_require_distinct_server_id`
    without a MySQL server: those methods only ever `execute()` once and read
    `fetchone()` once, so recording the last statement is enough.
    """

    def __init__(self, value):
        self._value = value
        self._last_params = None

    def execute(self, _sql, params=None):
        self._last_params = params

    def fetchone(self):
        if self._value is None:
            return None
        name = self._last_params[0] if self._last_params else None
        return (name, self._value)


class _FakeServerIdCursor:
    """Stand-in for a cursor answering `SELECT @@server_id`."""

    def __init__(self, value):
        self._value = value

    def execute(self, _sql, _params=None):
        pass

    def fetchone(self):
        return (self._value,)


class _InfiniteOtherTableStream:
    """
    An infinite binlog stream whose events never match the table being read.

    `read_changes()` must not drain this - the only things that can stop it are
    `should_continue()` returning False or `max_seconds` elapsing, since `changes`
    never grows for a table this stream never reports.
    """

    log_file = "mysql-bin.000001"
    log_pos = 1000

    def __iter__(self):
        while True:
            yield SimpleNamespace(schema="other_db", table="other_tbl")


class _DecodeErrorStream:
    """A stream that fails the way a MINIMAL-metadata binlog packet does."""

    log_file = "mysql-bin.000001"
    log_pos = 555

    def __iter__(self):
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")


class _RecordedState:
    """Stand-in for `StatefulSource.state` that records every write."""

    def __init__(self, calls):
        self._calls = calls
        self._values = {}

    def get(self, key, default=None):
        return self._values.get(key, default)

    def set(self, key, value):
        self._calls.append(("set", key))
        self._values[key] = value

    def delete(self, key):
        self._calls.append(("delete", key))
        self._values.pop(key, None)


class _RecordingSource(MySqlCdcSource):
    """
    `MySqlCdcSource` with `produce`, `flush` and `state` replaced by recorders.

    Lets the commit ordering be asserted without a broker, a state store or MySQL.
    """

    def __init__(self, fail_on_flush=None, **kwargs):
        super().__init__(**kwargs)
        self.calls = []
        self.flush_count = 0
        self._fail_on_flush = fail_on_flush
        self._recorded_state = _RecordedState(self.calls)

    @property
    def state(self):
        return self._recorded_state

    def serialize(self, key=None, value=None, headers=None, timestamp_ms=None):
        return KafkaMessage(
            key=str(key).encode(), value=repr(value).encode(), headers=None
        )

    def produce(self, value=None, key=None, **kwargs):
        self.calls.append(("produce", key))

    def flush(self, timeout=None):
        self.flush_count += 1
        self.calls.append(("flush", self.flush_count))
        if self._fail_on_flush == self.flush_count:
            raise RuntimeError("simulated producer flush failure")


def test_derive_server_id_is_deterministic():
    first = _derive_server_id("src", "db", "tbl")
    second = _derive_server_id("src", "db", "tbl")
    assert first == second


@pytest.mark.parametrize("table", ["a", "orders", "a_really_long_table_name_here"])
def test_derive_server_id_within_range(table):
    assert 1000 <= _derive_server_id("src", "db", table) <= 2**31 - 1


def test_derive_server_id_differs_per_table():
    assert _derive_server_id("src", "db", "a") != _derive_server_id("src", "db", "b")


@pytest.mark.parametrize("server_id", [0, -1, 2**32])
def test_explicit_server_id_out_of_range_raises(server_id):
    with pytest.raises(MySqlCdcError):
        MySqlCdcSource(**_source_kwargs(server_id=server_id))


def test_quote_identifier_escapes_backticks():
    assert quote_identifier("plain") == "`plain`"
    assert quote_identifier("a`b") == "`a``b`"


def test_build_snapshot_query_first_batch():
    query = build_snapshot_query("db", "tbl", ["id"], resuming=False)
    assert query == "SELECT * FROM `db`.`tbl` ORDER BY `id` LIMIT %s"
    assert "WHERE" not in query
    assert "OFFSET" not in query


def test_build_snapshot_query_resume_single_pk():
    assert build_snapshot_query("db", "tbl", ["id"], resuming=True) == (
        "SELECT * FROM `db`.`tbl` WHERE `id` > %s ORDER BY `id` LIMIT %s"
    )


def test_build_snapshot_query_resume_composite_pk():
    assert build_snapshot_query("db", "tbl", ["a", "b"], resuming=True) == (
        "SELECT * FROM `db`.`tbl` WHERE (`a`, `b`) > (%s, %s) ORDER BY `a`, `b` LIMIT %s"
    )


def test_serialize_value():
    assert serialize_value(None) is None
    assert serialize_value(b"abc") == base64.b64encode(b"abc").decode()
    assert serialize_value(datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02T03:04:05"
    assert serialize_value(Decimal("1.50")) == "1.50"
    assert serialize_value(7) == 7
    assert serialize_value("x") == "x"


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([1], True),
        (["a"], True),
        ([1, "a"], True),
        ([b"x"], False),
        ([datetime(2024, 1, 1)], False),
        ([Decimal("1")], False),
        ([True], False),
        ([], False),
    ],
)
def test_is_checkpointable_key(values, expected):
    assert is_checkpointable_key(values) is expected


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        # Retryable: the link broke and a new connection can plausibly replace it.
        (OperationalError(2013, "Lost connection to MySQL server during query"), True),
        (OperationalError(2006, "MySQL server has gone away"), True),
        (InterfaceError("(0, '')"), True),
        (BrokenPipeError(), True),
        (ConnectionResetError(), True),
        # 1236 is retryable on purpose: MySQL reuses it both for a purged position and
        # for a transient same-server_id collision, and only a reconnect can tell them
        # apart. The purged case still fails loudly once the attempts are exhausted.
        (
            OperationalError(
                1236, "A replica with the same server_uuid/server_id has connected"
            ),
            True,
        ),
        # 1236 "Could not find first log file..." means the committed position was
        # purged: D2/D5 classify this PURGED, which is fatal, not retryable, so a
        # reconnect must not be attempted.
        (
            OperationalError(
                1236, "Could not find first log file name in binary log index file"
            ),
            False,
        ),
        # Fatal: a reconnect presents the same rejected credentials or the same missing
        # grant forever, so retrying only hides the failure.
        (OperationalError(1045, "Access denied for user 'cdc_user'@'%'"), False),
        (OperationalError(1044, "Access denied to database 'db'"), False),
        (OperationalError(1227, "Access denied; you need REPLICATION SLAVE"), False),
        # Not a connection failure at all.
        (ValueError("nope"), False),
    ],
)
def test_is_connection_error(exc, expected):
    assert is_connection_error(exc) is expected


def test_create_binlog_stream_enables_column_name_cache(monkeypatch):
    """
    The stream must ask for the INFORMATION_SCHEMA column-name fallback.

    Without it, `pymysqlreplication` names every column `UNKNOWN_COL0..n` on any server
    that does not set `binlog_row_metadata=FULL` - which is the default on MySQL 8.x and
    unavailable on 5.7. This asserts the call shape only; that the names actually come
    back real is proved against a live MINIMAL-metadata server, not here.
    """
    captured = {}

    def fake_reader(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(mysql_helper, "BinLogStreamReader", fake_reader)
    helper = mysql_helper.MySqlHelper(
        host="localhost",
        port=3306,
        user="cdc_user",
        password="cdc_password",
        database="db",
        table="tbl",
        snapshot_host="localhost",
        tls=TlsConfig(),
    )

    helper.create_binlog_stream(server_id=1234, log_file="mysql-bin.000001", log_pos=4)

    assert captured["use_column_name_cache"] is True
    assert captured["server_id"] == 1234
    assert captured["log_file"] == "mysql-bin.000001"
    assert captured["log_pos"] == 4

    connection_settings = captured["connection_settings"]
    # D3/D5: `passwd` is deprecated by pymysql and TLS defaults to required, so the
    # connection settings handed to the control connection must carry the renamed
    # key, the UTC session pin, and an ssl context - not the old `passwd=`.
    assert "passwd" not in connection_settings
    assert connection_settings["password"] == "cdc_password"
    assert connection_settings["init_command"] == "SET time_zone = '+00:00'"
    assert "ssl" in connection_settings


def test_commit_batch_order():
    source = _RecordingSource(**_source_kwargs())
    source._buffer = [{"kind": "insert"}, {"kind": "update"}]
    source._pending_position = ("mysql-bin.000001", 4096)

    source._commit_batch()

    assert [call[0] for call in source.calls] == [
        "produce",
        "produce",
        "flush",
        "set",
        "flush",
    ]
    assert source._buffer == []
    assert source._pending_position is None
    assert source._committed_position == ("mysql-bin.000001", 4096)

    stored = source.state.get("binlog_position_db_tbl")
    assert stored["log_file"] == "mysql-bin.000001"
    assert stored["log_pos"] == 4096


def test_commit_batch_does_not_advance_position_when_data_flush_fails():
    source = _RecordingSource(fail_on_flush=1, **_source_kwargs())
    source._buffer = [{"kind": "insert"}]
    source._pending_position = ("mysql-bin.000001", 4096)

    with pytest.raises(RuntimeError):
        source._commit_batch()

    assert [call for call in source.calls if call[0] == "set"] == []
    assert source.state.get("binlog_position_db_tbl") is None
    assert source._committed_position is None


def test_stop_does_not_produce():
    source = _RecordingSource(**_source_kwargs())
    source._running = True
    source._buffer = [{"kind": "insert"}]

    source.stop()

    assert source.running is False
    assert source.calls == []
    assert source._buffer == [{"kind": "insert"}]


# --------------------------------------------------------------------------------
# Round 2 red-first tests (spec: dev-planning/mysql-cdc-round2/spec.md, section 3)
# --------------------------------------------------------------------------------


def test_read_changes_wraps_decode_error():
    """
    Validates spec section 2.1 / D1: a decode failure the transition window can still
    hit must surface as an actionable `MySqlCdcError` naming `binlog_row_metadata`,
    never a raw `UnicodeDecodeError`.
    """
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())

    with pytest.raises(MySqlCdcError, match="binlog_row_metadata"):
        helper.read_changes(
            _DecodeErrorStream(),
            max_rows=10,
            max_seconds=5,
            should_continue=lambda: True,
        )


def test_validate_server_config_requires_full_row_metadata():
    """
    Validates spec D1/2.2: `_require_row_metadata` refuses anything but FULL, for both
    a MySQL 5.7-style server (variable absent) and a stock 8.x server (MINIMAL).
    """
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())

    with pytest.raises(MySqlCdcError, match="'FULL'"):
        helper._require_row_metadata(_FakeShowVariableCursor("MINIMAL"))

    with pytest.raises(MySqlCdcError, match="has no binlog_row_metadata variable"):
        helper._require_row_metadata(_FakeShowVariableCursor(None))


def test_allow_minimal_row_metadata_refuses_by_default_and_warns_when_enabled(caplog):
    """
    Validates spec section 2.2 / the `allow_minimal_row_metadata` parameter: refused
    by default, downgraded to a warning naming the parameter when opted in.
    """
    strict_helper = mysql_helper.MySqlHelper(**_helper_kwargs())
    with pytest.raises(MySqlCdcError, match="allow_minimal_row_metadata"):
        strict_helper._require_row_metadata(_FakeShowVariableCursor("MINIMAL"))

    lenient_helper = mysql_helper.MySqlHelper(
        **_helper_kwargs(allow_minimal_row_metadata=True)
    )
    with caplog.at_level(logging.WARNING):
        lenient_helper._require_row_metadata(_FakeShowVariableCursor("MINIMAL"))

    assert any(
        "allow_minimal_row_metadata=True" in record.message for record in caplog.records
    )


def test_serialize_value_json_dict_with_bytes_keys():
    """
    Validates spec D4: a JSON column decoded from the binlog (`dict` with `bytes`
    keys/values) must serialize to valid, round-trippable JSON text.
    """
    encoded = serialize_value({b"a": 2})
    assert json.loads(encoded) == {"a": 2}


def test_serialize_value_set_is_sorted_and_stable():
    """Validates spec D4: a SET column's unordered `set`/`frozenset` sorts, every time."""
    assert serialize_value({"b", "a"}) == "a,b"
    assert serialize_value(frozenset({"z", "m", "a"})) == "a,m,z"


def test_serialize_row_matches_binlog_for_json_set_bit():
    """
    Validates spec D4's cross-path contract: SET, JSON and BIT columns must serialize
    identically whether the raw value came from the snapshot (`pymysql`-shaped) or the
    binlog (`pymysqlreplication`-shaped) side.
    """
    column_types = {
        "tags": ColumnType(data_type="set"),
        "doc": ColumnType(data_type="json"),
        "bits": ColumnType(data_type="bit", bit_width=8),
    }

    snapshot_row = serialize_row(
        values=["b,a", '{"a": 1, "b": 2}', b"\x05"],
        columns=["tags", "doc", "bits"],
        column_types=column_types,
    )
    binlog_row = serialize_binlog_values(
        values={"tags": {"a", "b"}, "doc": {b"a": 1, b"b": 2}, "bits": "00000101"},
        none_sources=None,
        json_columns=["doc"],
        float_columns=[],
    )

    assert snapshot_row == binlog_row == ["a,b", '{"a":1,"b":2}', "00000101"]


def test_force_snapshot_reanchors_position():
    """
    Validates spec D5: a forced snapshot must discard the stored binlog position and
    re-anchor from a freshly fetched one, not resume the stale position.
    """
    source = _RecordingSource(
        **_source_kwargs(initial_snapshot=True, force_snapshot=True)
    )
    source.state.set(
        source._position_key, {"log_file": "stale-bin.000001", "log_pos": 999}
    )
    source._helper.fetch_snapshot_start_position = lambda: ("fresh-bin.000009", 4242)

    snapshot_needed = source._is_snapshot_needed()
    assert snapshot_needed is True

    position = source._resolve_start_position(snapshot_needed)

    assert position == ("fresh-bin.000009", 4242)
    assert source._committed_position == ("fresh-bin.000009", 4242)
    stored = source.state.get(source._position_key)
    assert stored["log_file"] == "fresh-bin.000009"
    assert stored["log_pos"] == 4242


def test_force_snapshot_without_initial_snapshot_raises():
    """Validates spec D5: `force_snapshot=True` needs `initial_snapshot=True`."""
    with pytest.raises(MySqlCdcError, match="force_snapshot"):
        MySqlCdcSource(**_source_kwargs(force_snapshot=True, initial_snapshot=False))


def test_classify_error_purged_is_fatal():
    """
    Validates spec D2/D5: a purged binlog position classifies as PURGED (fatal), never
    RETRYABLE - the same 1236 code that a `server_id` collision also raises.
    """
    exc = OperationalError(
        1236, "Could not find first log file name in binary log index file"
    )
    assert mysql_helper.classify_error(exc) is mysql_helper.BinlogErrorKind.PURGED
    assert is_connection_error(exc) is False


def test_connect_mysql_passes_tls_context(monkeypatch):
    """Validates spec D3: `connect_mysql()` passes a real `ssl.SSLContext` by default."""
    captured = {}

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(mysql_helper.pymysql, "connect", fake_connect)
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())

    helper.connect_mysql()

    assert isinstance(captured["ssl"], ssl.SSLContext)
    assert captured["init_command"] == "SET time_zone = '+00:00'"


def test_create_binlog_stream_passes_tls_context(monkeypatch):
    """
    Validates spec D3: the control connection `create_binlog_stream()` builds also
    carries a real `ssl.SSLContext`, not just an `ssl` key of some kind.
    """
    captured = {}

    def fake_reader(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(mysql_helper, "BinLogStreamReader", fake_reader)
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())

    helper.create_binlog_stream(server_id=1234, log_file="mysql-bin.000001", log_pos=4)

    assert isinstance(captured["connection_settings"]["ssl"], ssl.SSLContext)


@pytest.mark.parametrize(
    "overrides",
    [
        {"tls_verify_cert": True},  # no tls_ca to verify against
        {"tls_key": "/path/key.pem"},  # no tls_cert this key belongs to
        {"tls_enabled": False, "tls_ca": "/path/ca.pem"},  # nothing to secure
    ],
)
def test_tls_parameter_contradictions_raise(overrides):
    """Validates spec D3: every contradictory tls_* combination is rejected loudly."""
    with pytest.raises(MySqlCdcError):
        MySqlCdcSource(**_source_kwargs(**overrides))


def test_repeated_collisions_exit_despite_successful_polls():
    """
    Validates spec D2/2.6: a `server_id` collision counter must NOT reset on a
    successful poll - only a non-collision failure resets it (`self._collisions = 0`
    in the RETRYABLE branch). Alternating collision/success must still trip
    `_MAX_COLLISIONS` and raise, proving the old `failures = 0` reset alone (which this
    test would otherwise loop against forever) is not what governs collisions anymore.
    """
    source = _RecordingSource(**_source_kwargs())
    source._running = True
    source._sleep = lambda duration: None
    source._reconnect_stream = lambda: None

    collision_exc = OperationalError(
        1236, "A replica with the same server_uuid/server_id has connected"
    )
    calls = {"n": 0}

    def fake_poll_once():
        calls["n"] += 1
        if calls["n"] % 2 == 1:
            raise collision_exc

    source._poll_once = fake_poll_once

    with pytest.raises(MySqlCdcError, match="collided"):
        source._stream_changes()

    # Collisions land on calls 1, 3, 5 (odd); a successful poll (even calls) does not
    # reset the counter, so the 3rd collision (call 5) trips _MAX_COLLISIONS=3.
    assert calls["n"] == 5


def test_validate_server_config_rejects_server_own_id():
    """Validates spec D2/2.6: announcing the server's own server_id is refused."""
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())

    with pytest.raises(MySqlCdcError, match="server_id=1234"):
        helper._require_distinct_server_id(_FakeServerIdCursor(1234), server_id=1234)


def test_read_changes_stops_when_asked():
    """
    Validates spec D6/2.7: `read_changes()` must break on `should_continue()` going
    False, even mid-way through an effectively infinite stream for another table.
    """
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())
    calls = {"n": 0}

    def should_continue():
        calls["n"] += 1
        return calls["n"] < 3

    started = time.monotonic()
    changes, position = helper.read_changes(
        _InfiniteOtherTableStream(),
        max_rows=1_000_000,
        max_seconds=1000,
        should_continue=should_continue,
    )
    elapsed = time.monotonic() - started

    assert changes == []
    assert position == ("mysql-bin.000001", 1000)
    assert elapsed < 2.0
    assert calls["n"] >= 2


def test_read_changes_honours_time_bound():
    """Validates spec D6/2.7: `read_changes()` must break once `max_seconds` elapses."""
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())

    started = time.monotonic()
    changes, position = helper.read_changes(
        _InfiniteOtherTableStream(),
        max_rows=1_000_000,
        max_seconds=0.05,
        should_continue=lambda: True,
    )
    elapsed = time.monotonic() - started

    assert changes == []
    assert position == ("mysql-bin.000001", 1000)
    assert elapsed < 2.0


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("poll_interval", 0),
        ("commit_interval", 0),
        ("retry_backoff_secs", -1),
        ("shutdown_timeout", 0),
    ],
)
def test_interval_parameters_must_be_positive(field, bad_value):
    """Validates spec D6/2.9: all four interval parameters must be strictly positive."""
    with pytest.raises(MySqlCdcError):
        MySqlCdcSource(**_source_kwargs(**{field: bad_value}))
