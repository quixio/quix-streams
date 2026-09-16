import base64
import json
import logging
import ssl
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")

from pymysql.err import InterfaceError, OperationalError

from quixstreams.models.messages import KafkaMessage
from quixstreams.sources.community.mysql_cdc import (
    failures,
    mysql_helper,
    server_config,
)
from quixstreams.sources.community.mysql_cdc.config import ConnectionTimeouts, TlsConfig
from quixstreams.sources.community.mysql_cdc.events import event_to_changes
from quixstreams.sources.community.mysql_cdc.failures import (
    _MAX_RECONNECT_ATTEMPTS,
    BinlogErrorKind,
    classify_error,
)
from quixstreams.sources.community.mysql_cdc.mysql_cdc import (
    MySqlCdcError,
    MySqlCdcSource,
    _derive_server_id,
)
from quixstreams.sources.community.mysql_cdc.reader import BinlogReader
from quixstreams.sources.community.mysql_cdc.retention import SnapshotAnchor
from quixstreams.sources.community.mysql_cdc.snapshot import (
    SnapshotPlan,
    build_snapshot_query,
    decode_key,
    encode_key,
    iter_snapshot_batches,
    quote_identifier,
)
from quixstreams.sources.community.mysql_cdc.values import (
    ColumnType,
    serialize_binlog_values,
    serialize_row,
    serialize_value,
)

_POSITION_KEY = "binlog_position_db_tbl"
_SNAPSHOT_COMPLETED_KEY = "snapshot_completed_db_tbl"
_SNAPSHOT_PROGRESS_KEY = "snapshot_progress_db_tbl"


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
        "timeouts": ConnectionTimeouts.derive(
            commit_interval=5.0, retry_backoff_secs=5.0, shutdown_timeout=10.0
        ),
    }
    kwargs.update(overrides)
    return kwargs


class _FakeServerCursor:
    """
    Stand-in for a DB-API cursor that answers `SHOW GLOBAL VARIABLES` from a dict.

    Every other statement is recorded in `statements` and answers nothing, which is
    enough to drive `server_config.ensure_row_settings()` without a MySQL server: the
    variables it reads come back from `variables`, and the `SET GLOBAL`/`FLUSH` it runs
    are what the test asserts on. A `SET GLOBAL` that is not refused changes what the
    next `SHOW GLOBAL VARIABLES` answers, so repeated calls see the repaired server.
    `refuse` makes one statement prefix raise, standing in for a server that will not
    run it for this account.
    """

    def __init__(self, variables, refuse=None, refusal=None):
        self.variables = dict(variables)
        self.statements = []
        self._refuse = refuse
        self._refusal = refusal
        self._row = None

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if self._refuse is not None and sql.startswith(self._refuse):
            raise self._refusal
        if sql.startswith("SHOW GLOBAL VARIABLES"):
            name = params[0]
            value = self.variables.get(name)
            self._row = None if value is None else (name, value)
            return
        if sql.startswith("SET GLOBAL "):
            name, _, value = sql[len("SET GLOBAL ") :].partition(" = ")
            self.variables[name] = value
        self._row = None

    def fetchone(self):
        return self._row


class _FakeServerIdCursor:
    """Stand-in for a cursor answering `SELECT @@server_id`."""

    def __init__(self, value):
        self._value = value

    def execute(self, _sql, _params=None):
        pass

    def fetchone(self):
        return (self._value,)


class _InfiniteEmptyEventStream:
    """An infinite binlog stream of events that carry no rows for this source."""

    log_file = "mysql-bin.000001"
    log_pos = 1000

    def __iter__(self):
        while True:
            yield SimpleNamespace(
                schema="db",
                table="tbl",
                table_id=1,
                table_map={1: SimpleNamespace(column_name_flag=True)},
                columns=[],
            )


class _RaisingStream:
    """A stream whose `fetchone` raises, where the library's own decoding raises."""

    log_file = "mysql-bin.000001"
    log_pos = 555

    def __init__(self, error):
        self._error = error

    def __iter__(self):
        return self

    def __next__(self):
        raise self._error


class _ForeignTrafficStream:
    """
    Models `BinLogStreamReader.fetchone` on a server where another table is hot.

    Its loop is the library's (`binlogstream.py:598-728`): every event is discarded by
    the `only_tables` filter and `continue`s, so the call only returns when the
    `end_log_pos` bound the caller installed says it is past the end (`:672`, `:600`).
    `_CEILING` stands in for the millions of events a real server has ready; reaching
    it means nothing bounded the walk.
    """

    _CEILING = 2_000_000

    def __init__(self):
        self.log_file = "mysql-bin.000001"
        self.log_pos = 4
        self.end_log_pos = None
        self.is_past_end_log_pos = False
        self.events_walked = 0

    def __iter__(self):
        return iter(self.fetchone, None)

    def fetchone(self):
        while self.events_walked < self._CEILING:
            if self.end_log_pos and self.is_past_end_log_pos:
                return None
            self.events_walked += 1
            self.log_pos += 100
            if self.end_log_pos and self.log_pos >= self.end_log_pos:
                self.is_past_end_log_pos = True
        return None


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
    """`MySqlCdcSource` with `produce`, `flush` and `state` replaced by recorders."""

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
    query = build_snapshot_query("db", "tbl", ["id", "name"], ["id"], resuming=False)
    assert query == "SELECT `id`, `name` FROM `db`.`tbl` ORDER BY `id` LIMIT %s"
    assert "WHERE" not in query
    assert "OFFSET" not in query


def test_build_snapshot_query_resume_single_pk():
    assert build_snapshot_query("db", "tbl", ["id"], ["id"], resuming=True) == (
        "SELECT `id` FROM `db`.`tbl` WHERE `id` > %s ORDER BY `id` LIMIT %s"
    )


def test_build_snapshot_query_resume_composite_pk():
    assert build_snapshot_query("db", "tbl", ["a", "b"], ["a", "b"], resuming=True) == (
        "SELECT `a`, `b` FROM `db`.`tbl` WHERE (`a`, `b`) > (%s, %s) "
        "ORDER BY `a`, `b` LIMIT %s"
    )


def test_serialize_value():
    assert serialize_value(None) is None
    assert serialize_value(b"abc") == base64.b64encode(b"abc").decode()
    assert serialize_value(datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02T03:04:05"
    assert serialize_value(Decimal("1.50")) == "1.50"
    assert serialize_value(7) == 7
    assert serialize_value("x") == "x"


@pytest.mark.parametrize(
    "values",
    [
        [1],
        ["a"],
        [1, "a"],
        [b"x"],
        [datetime(2024, 1, 1)],
        [Decimal("1")],
        [True],
        [timedelta(hours=26, minutes=3, seconds=4)],
        [bytes([0, 255]), Decimal("12.30"), date(2024, 3, 1)],
    ],
)
def test_checkpoint_key_round_trips(values):
    encoded, tags = encode_key(values)
    assert json.loads(json.dumps(encoded)) == encoded
    assert decode_key(encoded, tags) == values


def test_an_empty_key_cannot_be_checkpointed():
    assert encode_key([]) is None


def test_a_key_stored_without_its_types_cannot_be_decoded():
    """Every stored key carries one tag per value; anything else is not a key."""
    assert decode_key([1, "a"], []) is None
    assert decode_key([1, "a"], ["int"]) is None
    assert decode_key([1], ["nosuchtag"]) is None


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        # Retryable: the link broke and a new connection can plausibly replace it.
        (
            OperationalError(2013, "Lost connection to MySQL server during query"),
            BinlogErrorKind.RETRYABLE,
        ),
        (
            OperationalError(2006, "MySQL server has gone away"),
            BinlogErrorKind.RETRYABLE,
        ),
        (InterfaceError("(0, '')"), BinlogErrorKind.RETRYABLE),
        (BrokenPipeError(), BinlogErrorKind.RETRYABLE),
        (ConnectionResetError(), BinlogErrorKind.RETRYABLE),
        # MySQL reuses 1236 for a purged position and for a same-server_id collision,
        # and only the message tells them apart. The collision is retried a bounded
        # number of times; the purged position must not be retried at all.
        (
            OperationalError(
                1236, "A replica with the same server_uuid/server_id has connected"
            ),
            BinlogErrorKind.COLLISION,
        ),
        (
            OperationalError(
                1236, "Could not find first log file name in binary log index file"
            ),
            BinlogErrorKind.PURGED,
        ),
        # Fatal: a reconnect presents the same rejected credentials or the same missing
        # grant forever, so retrying only hides the failure.
        (
            OperationalError(1045, "Access denied for user 'cdc_user'@'%'"),
            BinlogErrorKind.FATAL,
        ),
        (
            OperationalError(1044, "Access denied to database 'db'"),
            BinlogErrorKind.FATAL,
        ),
        (
            OperationalError(1227, "Access denied; you need REPLICATION SLAVE"),
            BinlogErrorKind.FATAL,
        ),
        # Not a connection failure at all.
        (ValueError("nope"), BinlogErrorKind.FATAL),
    ],
)
def test_classify_error(exc, expected):
    assert classify_error(exc) is expected


def test_create_binlog_stream_enables_column_name_cache(monkeypatch):
    """The stream must ask for the INFORMATION_SCHEMA column-name fallback."""
    captured = {}

    def fake_reader(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(mysql_helper, "BinLogStreamReader", fake_reader)
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())

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
    """A decode failure surfaces as `MySqlCdcError`, not a `UnicodeDecodeError`."""
    error = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")
    reader = BinlogReader(_RaisingStream(error), database="db", table="tbl")

    with pytest.raises(MySqlCdcError, match="binlog_row_metadata"):
        reader.read_changes(
            max_rows=10,
            max_seconds=5,
            should_continue=lambda: True,
        )


@pytest.mark.parametrize(
    "error",
    [
        KeyError(42),
        IndexError("list index out of range"),
    ],
)
def test_read_changes_does_not_call_a_lookup_error_a_decode_failure(error):
    """
    Round 6 #4: `KeyError` and `IndexError` are `LookupError`s, and pymysqlreplication
    raises them from `table_map[table_id]`, from the parallel cursors of
    `_sync_column_info` and from `column.enum_values[...]`. Reporting those as an
    undecodable event sends the operator to re-read the whole table for nothing.
    """
    reader = BinlogReader(_RaisingStream(error), database="db", table="tbl")

    with pytest.raises(type(error)):
        reader.read_changes(max_rows=10, max_seconds=5, should_continue=lambda: True)


def test_read_changes_bounds_the_stream_walk_over_foreign_events():
    """
    Round 6 #3: `fetchone` skips every event of every other table without returning, so
    both bounds have to be installed inside its loop, not only around it.
    """
    stream = _ForeignTrafficStream()
    reader = BinlogReader(stream, database="db", table="tbl")

    started = time.monotonic()
    changes, position = reader.read_changes(
        max_rows=1000, max_seconds=1000, should_continue=lambda: False
    )
    elapsed = time.monotonic() - started

    assert changes == []
    assert position == ("mysql-bin.000001", 104)
    assert stream.events_walked == 1, (
        f"the stream walked {stream.events_walked} foreign events before either bound "
        "was evaluated"
    )
    assert elapsed < 2.0


def test_read_changes_bounds_the_stream_walk_by_time():
    """Round 6 #3: `max_seconds` has to reach inside `fetchone` for the same reason."""
    stream = _ForeignTrafficStream()
    reader = BinlogReader(stream, database="db", table="tbl")

    started = time.monotonic()
    reader.read_changes(max_rows=1000, max_seconds=0.05, should_continue=lambda: True)
    elapsed = time.monotonic() - started

    assert stream.events_walked < stream._CEILING
    assert elapsed < 2.0

    # A second read continues: the bound that tripped is not the end of the stream.
    walked = stream.events_walked
    reader.read_changes(max_rows=1000, max_seconds=0.05, should_continue=lambda: True)
    assert stream.events_walked > walked


def test_ensure_row_settings_sets_both_globals_on_a_stock_server(caplog):
    """A server on MySQL's defaults is repaired: globals set, log rotated, logged."""
    cursor = _FakeServerCursor(
        {"binlog_row_metadata": "MINIMAL", "binlog_row_image": "MINIMAL"}
    )

    with caplog.at_level(logging.INFO):
        server_config.ensure_row_settings(cursor, host="localhost", user="cdc_user")

    assert "SET GLOBAL binlog_row_metadata = FULL" in cursor.statements
    assert "SET GLOBAL binlog_row_image = FULL" in cursor.statements
    assert "FLUSH BINARY LOGS" in cursor.statements
    assert any("= FULL on localhost" in record.message for record in caplog.records)


def test_ensure_row_settings_is_a_no_op_on_a_configured_server():
    """A server that already has both FULL is not written to at all."""
    cursor = _FakeServerCursor(
        {"binlog_row_metadata": "FULL", "binlog_row_image": "FULL"}
    )

    server_config.ensure_row_settings(cursor, host="localhost", user="cdc_user")

    assert [s for s in cursor.statements if not s.startswith("SHOW GLOBAL")] == []


def test_ensure_row_settings_without_the_privilege_names_the_grant():
    """The one case that still fails names the single GRANT that fixes it."""
    cursor = _FakeServerCursor(
        {"binlog_row_metadata": "MINIMAL", "binlog_row_image": "FULL"},
        refuse="SET GLOBAL",
        refusal=OperationalError(
            1227, "Access denied; you need SUPER or SYSTEM_VARIABLES_ADMIN"
        ),
    )

    with pytest.raises(MySqlCdcError, match="GRANT SYSTEM_VARIABLES_ADMIN"):
        server_config.ensure_row_settings(cursor, host="localhost", user="cdc_user")


def test_ensure_row_settings_survives_a_refused_flush(caplog):
    """A refused FLUSH BINARY LOGS is reported, not fatal: it needs RELOAD."""
    cursor = _FakeServerCursor(
        {"binlog_row_metadata": "MINIMAL", "binlog_row_image": "MINIMAL"},
        refuse="FLUSH",
        refusal=OperationalError(1227, "Access denied; you need RELOAD"),
    )

    with caplog.at_level(logging.INFO):
        server_config.ensure_row_settings(cursor, host="localhost", user="cdc_user")

    assert "SET GLOBAL binlog_row_metadata = FULL" in cursor.statements
    assert any("Did not rotate the binary logs" in r.message for r in caplog.records)


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
    """SET, JSON and BIT serialize identically on the snapshot and binlog paths."""
    column_types = {
        "tags": ColumnType(data_type="set"),
        "doc": ColumnType(data_type="json"),
        "bits": ColumnType(data_type="bit", bit_width=8),
    }

    snapshot_row = serialize_row(
        values=["b,a", '{"a": 1, "b": 2}', b"\x05"],
        columns=["tags", "doc", "bits"],
        column_types=column_types,
        table="db.tbl",
    )
    binlog_names, binlog_row = serialize_binlog_values(
        values={"tags": {"a", "b"}, "doc": {b"a": 1, b"b": 2}, "bits": "00000101"},
        table="db.tbl",
        none_sources=None,
        json_columns=["doc"],
        float_columns=[],
    )

    assert binlog_names == ["tags", "doc", "bits"]
    assert snapshot_row == binlog_row == ["a,b", '{"a":1,"b":2}', "00000101"]


def test_force_snapshot_reanchors_position():
    """A forced snapshot discards the stored position and re-anchors it."""
    source = _RecordingSource(
        **_source_kwargs(initial_snapshot=True, force_snapshot=True)
    )
    source.state.set(_POSITION_KEY, {"log_file": "stale-bin.000001", "log_pos": 999})
    source._helper.fetch_snapshot_start_position = lambda: ("fresh-bin.000009", 4242)

    snapshot_needed = source._is_snapshot_needed()
    assert snapshot_needed is True

    position = source._resolve_start_position(snapshot_needed)

    assert position == ("fresh-bin.000009", 4242)
    assert source._committed_position == ("fresh-bin.000009", 4242)
    stored = source.state.get(_POSITION_KEY)
    assert stored["log_file"] == "fresh-bin.000009"
    assert stored["log_pos"] == 4242


def test_force_snapshot_without_initial_snapshot_raises():
    """Validates spec D5: `force_snapshot=True` needs `initial_snapshot=True`."""
    with pytest.raises(MySqlCdcError, match="force_snapshot"):
        MySqlCdcSource(**_source_kwargs(force_snapshot=True, initial_snapshot=False))


def test_classify_error_purged_is_not_retryable():
    """
    Validates spec D2/D5: a purged binlog position classifies as PURGED, never
    RETRYABLE - the same 1236 code that a `server_id` collision also raises.
    """
    exc = OperationalError(
        1236, "Could not find first log file name in binary log index file"
    )
    assert classify_error(exc) is BinlogErrorKind.PURGED


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


def test_tls_disabled_with_a_ca_is_rejected():
    """The one impossible TLS combination is rejected, not reinterpreted."""
    with pytest.raises(MySqlCdcError, match="tls_enabled=False"):
        MySqlCdcSource(**_source_kwargs(tls_enabled=False, tls_ca="/path/ca.pem"))


def test_tls_ca_turns_verification_on():
    """A CA is the switch: without one the connection is encrypted, not authenticated."""
    assert TlsConfig().verifies is False
    assert TlsConfig(ca="/path/ca.pem").verifies is True


def test_repeated_collisions_exit_despite_successful_polls():
    """A successful poll does not clear the collision count."""
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

    # Collisions land on the odd calls, so the third one is call 5.
    assert calls["n"] == 5


def test_validate_server_config_rejects_server_own_id():
    """Validates spec D2/2.6: announcing the server's own server_id is refused."""
    with pytest.raises(MySqlCdcError, match="server_id=1234"):
        server_config.require_distinct_server_id(
            _FakeServerIdCursor(1234), host="localhost", server_id=1234
        )


def test_read_changes_stops_when_asked():
    """`read_changes()` breaks on `should_continue()` going False."""
    reader = BinlogReader(_InfiniteEmptyEventStream(), database="db", table="tbl")
    calls = {"n": 0}

    def should_continue():
        calls["n"] += 1
        return calls["n"] < 3

    started = time.monotonic()
    changes, position = reader.read_changes(
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
    reader = BinlogReader(_InfiniteEmptyEventStream(), database="db", table="tbl")

    started = time.monotonic()
    changes, position = reader.read_changes(
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


# --------------------------------------------------------------------------------
# Round 3 red-first tests (spec: dev-planning/mysql-cdc-round3, blockers 1-7)
# --------------------------------------------------------------------------------


def test_failed_reconnect_counts_as_a_failure_and_retries():
    """Blocker 1: a rebuild that fails counts as a failure and the loop retries."""
    source = _RecordingSource(**_source_kwargs())
    source._running = True
    source._sleep = lambda duration: None
    attempts = {"poll": 0, "reconnect": 0}

    def poll_once():
        attempts["poll"] += 1
        raise OperationalError(2013, "Lost connection to MySQL server during query")

    def reconnect_stream():
        attempts["reconnect"] += 1
        raise OperationalError(2003, "Can't connect to MySQL server on 'localhost'")

    source._poll_once = poll_once
    source._reconnect_stream = reconnect_stream

    with pytest.raises(OperationalError):
        source._stream_changes()

    assert attempts == {
        "poll": 1,
        "reconnect": _MAX_RECONNECT_ATTEMPTS - 1,
    }


def test_reconnect_attempts_stop_when_the_source_is_stopped():
    """A stop() during the backoff ends the loop instead of rebuilding the stream."""
    source = _RecordingSource(**_source_kwargs())
    source._running = True
    source._sleep = lambda duration: source.stop()
    reconnects = {"n": 0}

    def poll_once():
        raise OperationalError(2013, "Lost connection to MySQL server during query")

    source._poll_once = poll_once
    source._reconnect_stream = lambda: reconnects.__setitem__("n", reconnects["n"] + 1)

    source._stream_changes()

    assert reconnects["n"] == 0


def test_binlog_values_keep_sql_null_and_the_empty_set_apart():
    """Blocker 2: `null` means SQL NULL, and an empty SET is an empty string."""
    names, values = serialize_binlog_values(
        values={"id": 1, "a": 2, "b": None, "tags": None},
        table="db.tbl",
        none_sources={"b": "null", "tags": "empty set"},
    )

    assert names == ["id", "a", "b", "tags"]
    assert values == [1, 2, None, ""]


def test_default_topic_keys_are_strings():
    """Blocker 4: the key is the string "<database>.<table>", not b"db.tbl"."""
    topic = MySqlCdcSource(**_source_kwargs()).default_topic()

    assert type(topic._key_serializer).__name__ == "StringSerializer"
    assert type(topic._key_deserializer).__name__ == "StringDeserializer"
    assert type(topic._value_serializer).__name__ == "JSONSerializer"
    assert type(topic._value_deserializer).__name__ == "JSONDeserializer"


def test_every_connection_carries_timeouts(monkeypatch):
    """Blocker 5: plain connections and the reader's both carry socket timeouts."""
    captured = {}
    monkeypatch.setattr(
        mysql_helper.pymysql, "connect", lambda **kwargs: captured.update(kwargs)
    )
    monkeypatch.setattr(
        mysql_helper, "BinLogStreamReader", lambda **kwargs: captured.update(kwargs)
    )
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())

    helper.connect_mysql()
    assert captured["connect_timeout"] > 0
    assert captured["read_timeout"] > 0
    assert captured["write_timeout"] > 0

    captured.clear()
    helper.create_binlog_stream(server_id=1234, log_file="mysql-bin.000001", log_pos=4)
    settings = captured["connection_settings"]
    assert settings["connect_timeout"] > 0
    assert settings["read_timeout"] > 0
    assert settings["write_timeout"] > 0


def test_timeouts_are_derived_from_the_existing_intervals():
    """Longer cadences widen the bounds; neither can push them below its floor."""
    default = ConnectionTimeouts.derive(
        commit_interval=5.0, retry_backoff_secs=5.0, shutdown_timeout=10.0
    )
    patient = ConnectionTimeouts.derive(
        commit_interval=60.0, retry_backoff_secs=30.0, shutdown_timeout=120.0
    )

    assert default.read >= 30.0
    assert default.connect >= 10.0
    assert patient.read > default.read
    assert patient.connect > default.connect


def test_collisions_decay_out_of_the_window(monkeypatch):
    """Blocker 7: collisions decay out of the window instead of accumulating forever."""
    clock = {"t": 0.0}
    monkeypatch.setattr(failures.time, "monotonic", lambda: clock["t"])
    policy = failures.ReconnectPolicy("db.tbl", server_id=1234, max_backoff=5.0)
    exc = OperationalError(
        1236, "A replica with the same server_uuid/server_id has connected"
    )

    for month in range(4):
        clock["t"] = month * 30 * 24 * 3600.0
        policy.note_collision(exc)


def test_collisions_inside_one_window_still_trip(monkeypatch):
    """The decay must not blunt the bound: three collisions in seconds still exit."""
    clock = {"t": 0.0}
    monkeypatch.setattr(failures.time, "monotonic", lambda: clock["t"])
    policy = failures.ReconnectPolicy("db.tbl", server_id=1234, max_backoff=5.0)
    exc = OperationalError(
        1236, "A replica with the same server_uuid/server_id has connected"
    )

    policy.note_collision(exc)
    clock["t"] = 1.0
    policy.note_collision(exc)
    clock["t"] = 2.0
    with pytest.raises(MySqlCdcError, match="collided 3 times"):
        policy.note_collision(exc)


def test_reconnect_window_trips_at_the_number_it_names():
    """The 20-reconnect bound fires on the 20th, not the 21st."""
    policy = failures.ReconnectPolicy("db.tbl", server_id=1234, max_backoff=5.0)
    exc = OperationalError(2013, "Lost connection to MySQL server during query")

    for _ in range(19):
        policy.note_failure(exc)
        policy.note_success()

    with pytest.raises(MySqlCdcError, match="rebuilt 20 times"):
        policy.note_failure(exc)


def test_partial_row_image_is_refused_by_name():
    """F1: a column MySQL did not send stops the source instead of vanishing."""
    with pytest.raises(MySqlCdcError, match="binlog_row_image"):
        serialize_binlog_values(
            values={"id": 1, "note": "touched", "tags": None},
            table="db.tbl",
            none_sources={"tags": "cols bitmap"},
        )


def test_partial_json_update_is_refused_by_name():
    """F1: a JSON column carrying only a diff cannot be published either."""
    with pytest.raises(MySqlCdcError, match="binlog_row_value_options"):
        serialize_binlog_values(
            values={"id": 1, "doc": None},
            table="db.tbl",
            none_sources={"doc": "same with before values"},
        )


def test_snapshot_query_names_every_column_including_invisible_ones():
    """F2: `SELECT *` omits INVISIBLE columns; the binlog row images carry them."""
    query = build_snapshot_query(
        "db", "parts", ["my_row_id", "name"], ["my_row_id"], resuming=False
    )
    assert query.startswith("SELECT `my_row_id`, `name` FROM `db`.`parts`")


def test_snapshot_pages_on_an_invisible_primary_key():
    """F2: the PK index comes from the plan, not from the result-set metadata."""
    plan = SnapshotPlan(
        pk_columns=["my_row_id"],
        columns=["my_row_id", "name"],
        column_types={
            "my_row_id": ColumnType(data_type="bigint"),
            "name": ColumnType(data_type="varchar"),
        },
        estimated_rows=2,
    )

    class Cursor:
        description = (("my_row_id",), ("name",))

        def execute(self, query, params=None):
            self.query = query

        def fetchall(self):
            return [(1, "bolt"), (2, "nut")]

    changes, last_key = next(
        iter_snapshot_batches(
            Cursor(), database="db", table="parts", plan=plan, batch_size=2
        )
    )

    assert changes[0]["columnnames"] == ["my_row_id", "name"]
    assert last_key == [2]


def test_snapshot_anchor_fails_once_the_anchored_file_is_purged():
    """F3: a snapshot that outlives its anchor stops instead of ending on a 1236."""

    class Helper:
        host = "mysql-1"

        def __init__(self, present):
            self.present = present
            self.queries = 0

        def binlog_status(self, log_file):
            self.queries += 1
            return self.present, 3600

    helper = Helper(present=False)
    anchor = SnapshotAnchor(
        helper, "db.tbl", ("mysql-bin.000007", 4), anchored_at=time.time() - 7200
    )

    # Throttled: a snapshot that finishes inside a minute asks the server nothing.
    anchor.check(rows_done=10, estimated_rows=100)
    assert helper.queries == 0

    anchor._checked_at -= 120
    with pytest.raises(MySqlCdcError, match="binlog_expire_logs_seconds"):
        anchor.check(rows_done=10, estimated_rows=100)


def test_purged_anchor_discards_the_state_it_points_at():
    """F3: the next start re-anchors by itself, with no state store to wipe."""

    class Helper:
        host = "mysql-1"

        def binlog_status(self, log_file):
            return False, 60

    source = _RecordingSource(**_source_kwargs(initial_snapshot=True))
    source.state.set(_POSITION_KEY, {"log_file": "a", "log_pos": 1})
    source.state.set(_SNAPSHOT_PROGRESS_KEY, {"last_key": [1]})
    source.state.set(_SNAPSHOT_COMPLETED_KEY, {"rows": 1})

    anchor = SnapshotAnchor(
        Helper(), "db.tbl", ("mysql-bin.000007", 4), anchored_at=time.time() - 600
    )
    anchor._checked_at -= 120

    with pytest.raises(MySqlCdcError):
        source._check_snapshot_anchor(anchor, rows_produced=10, estimated_rows=100)

    assert source.state.get(_POSITION_KEY) is None
    assert source.state.get(_SNAPSHOT_PROGRESS_KEY) is None
    assert source.state.get(_SNAPSHOT_COMPLETED_KEY) is None


def test_one_event_cannot_exceed_the_buffer_bound(monkeypatch):
    """F4: an event with more rows than the bound is split across reads."""
    rows = [{"kind": "insert", "n": n} for n in range(5_000)]

    class Stream:
        log_file = "mysql-bin.000001"
        log_pos = 5000

        def __init__(self):
            self.served = 0

        def __iter__(self):
            if self.served:
                return
            self.served += 1
            yield SimpleNamespace(schema="db", table="tbl", rows=rows)

    monkeypatch.setattr(
        "quixstreams.sources.community.mysql_cdc.reader.event_to_changes",
        lambda event: list(event.rows),
    )
    reader = BinlogReader(Stream(), database="db", table="tbl")

    first, first_position = reader.read_changes(
        max_rows=1000, max_seconds=5, should_continue=lambda: True
    )
    second, second_position = reader.read_changes(
        max_rows=1000, max_seconds=5, should_continue=lambda: True
    )

    assert len(first) == 1000
    assert len(second) == 1000
    # The event's position is only reported once every one of its rows has been.
    assert first_position is None
    assert second_position is None
    assert first[0]["n"] == 0
    assert second[0]["n"] == 1000


def test_poll_asks_for_the_room_left_in_the_buffer():
    """F4: `max_buffer_size` bounds the buffer, not each read in isolation."""
    source = _RecordingSource(**_source_kwargs(max_buffer_size=1000))
    source._buffer = [{"kind": "insert"}] * 900
    asked = {}

    class Stream:
        def read_changes(self, max_rows, max_seconds, should_continue):
            asked["max_rows"] = max_rows
            return [], None

    source._stream = Stream()
    source._sleep = lambda duration: None
    source._poll_once()

    assert asked["max_rows"] == 100


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (timedelta(hours=26, minutes=3, seconds=4), "26:03:04"),
        (timedelta(hours=-1), "-01:00:00"),
        (timedelta(hours=838, minutes=59, seconds=59), "838:59:59"),
        (-timedelta(hours=838, minutes=59, seconds=59), "-838:59:59"),
        (timedelta(seconds=1, microseconds=500000), "00:00:01.500000"),
        (timedelta(0), "00:00:00"),
    ],
)
def test_time_columns_are_mysql_time_strings(value, expected):
    """F5: MySQL's TIME range does not survive `str(timedelta)`."""
    assert serialize_value(value, "span") == expected


def test_tls_context_clears_x509_strict(monkeypatch):
    """F6: MySQL's own generated certificates do not pass VERIFY_X509_STRICT."""
    real = ssl.create_default_context

    def strict_context(*args, **kwargs):
        context = real(*args, **kwargs)
        context.verify_flags |= ssl.VERIFY_X509_STRICT
        return context

    monkeypatch.setattr(ssl, "create_default_context", strict_context)
    context = TlsConfig().connect_kwargs()["ssl"]

    assert not context.verify_flags & ssl.VERIFY_X509_STRICT


class _ReconnectingHelper:
    """A helper whose server settings are the `_FakeServerCursor` handed to it."""

    def __init__(self, cursor):
        self.cursor = cursor
        self.streams = 0

    def ensure_row_settings(self):
        server_config.ensure_row_settings(
            self.cursor, host="localhost", user="cdc_user"
        )

    def create_binlog_stream(self, server_id, log_file, log_pos):
        self.streams += 1
        return SimpleNamespace(close=lambda: None)


def test_reconnect_does_not_rewrite_the_settings_of_a_server_that_has_them():
    """F7: a flapping link must not issue SET GLOBAL and rotate the binlog per retry."""
    cursor = _FakeServerCursor(
        {"binlog_row_metadata": "FULL", "binlog_row_image": "FULL"}
    )
    source = _RecordingSource(**_source_kwargs())
    source._helper = _ReconnectingHelper(cursor)
    source._committed_position = ("mysql-bin.000004", 120)

    for _ in range(20):
        source._reconnect_stream()

    assert source._helper.streams == 20
    assert [s for s in cursor.statements if not s.startswith("SHOW GLOBAL")] == []
    assert len(cursor.statements) == 40


def test_reconnect_repairs_a_server_that_reverted_and_rotates_once():
    """
    Round 6 #1: a MySQL restart puts both settings back to my.cnf, and every writer
    that reconnects then strips the events this source has to refuse. The rebuild is
    where that is caught, and it costs one rotation, not one per reconnect.
    """
    cursor = _FakeServerCursor(
        {"binlog_row_metadata": "MINIMAL", "binlog_row_image": "MINIMAL"}
    )
    source = _RecordingSource(**_source_kwargs())
    source._helper = _ReconnectingHelper(cursor)
    source._committed_position = ("mysql-bin.000004", 120)

    for _ in range(20):
        source._reconnect_stream()

    assert cursor.variables["binlog_row_metadata"] == "FULL"
    assert cursor.variables["binlog_row_image"] == "FULL"
    assert cursor.statements.count("SET GLOBAL binlog_row_metadata = FULL") == 1
    assert cursor.statements.count("SET GLOBAL binlog_row_image = FULL") == 1
    assert cursor.statements.count("FLUSH BINARY LOGS") == 1


def test_a_drained_poll_waits_for_the_commit_it_is_holding_data_for():
    """F8: buffered changes and no commit due must not spin the dump connection."""
    source = _RecordingSource(**_source_kwargs(commit_interval=5.0))
    waits = []
    source._sleep = waits.append

    class Stream:
        def read_changes(self, max_rows, max_seconds, should_continue):
            return [{"kind": "insert"}], ("mysql-bin.000001", 99)

    source._stream = Stream()
    source._last_commit_at = time.monotonic()
    source._poll_once()

    assert waits
    assert waits[0] > 1.0


def test_idle_backoff_reaches_the_commit_interval():
    """F8: an idle table costs one dump registration per `commit_interval`."""
    source = _RecordingSource(**_source_kwargs(commit_interval=5.0, poll_interval=0.1))
    waits = []
    source._sleep = waits.append

    class Stream:
        def read_changes(self, max_rows, max_seconds, should_continue):
            return [], None

    source._stream = Stream()
    for _ in range(12):
        source._poll_once()

    assert waits[-1] == 5.0


def test_absent_binlog_format_is_not_a_misconfiguration(caplog):
    """F9: `binlog_format` is deprecated; a server without it only writes ROW."""
    with caplog.at_level(logging.INFO):
        server_config.require_row_format(_FakeServerCursor({}), host="mysql-9")

    assert any("no binlog_format variable" in r.message for r in caplog.records)

    with pytest.raises(MySqlCdcError, match="STATEMENT"):
        server_config.require_row_format(
            _FakeServerCursor({"binlog_format": "STATEMENT"}), host="mysql-8"
        )


def test_purged_position_message_has_a_branch_for_a_pk_less_table():
    """F10: the recovery offered must not be the one the no-PK check refuses."""
    source = MySqlCdcSource(**_source_kwargs(initial_snapshot=False))

    message = str(source._purged_position_failure())

    assert "no snapshot to take" in message
    assert "PRIMARY KEY" in message
    assert "restart the source with initial_snapshot=False" not in message


def test_snapshot_progress_is_discarded_when_the_primary_key_changed():
    """F11: a stored key cannot be replayed into a query built from a different PK."""
    source = _RecordingSource(**_source_kwargs(initial_snapshot=True))
    progress = {
        "last_key": [42],
        "key_types": ["int"],
        "pk_columns": ["id"],
        "rows": 42,
    }
    source.state.set(_SNAPSHOT_PROGRESS_KEY, dict(progress))

    assert source._progress.resume_point(["tenant", "id"]) == (None, 0)
    assert source.state.get(_SNAPSHOT_PROGRESS_KEY) is None

    source.state.set(_SNAPSHOT_PROGRESS_KEY, dict(progress))
    assert source._progress.resume_point(["id"]) == ([42], 42)


def test_snapshot_progress_survives_a_primary_key_spelt_in_another_case():
    """
    Round 6: MySQL column names are case-insensitive and the snapshot query lowercases
    them, so a case difference between runs must not restart a large snapshot at row 0.
    """
    source = _RecordingSource(**_source_kwargs(initial_snapshot=True))
    source.state.set(
        _SNAPSHOT_PROGRESS_KEY,
        {"last_key": [42], "key_types": ["int"], "pk_columns": ["Id"], "rows": 42},
    )

    assert source._progress.resume_point(["id"]) == ([42], 42)
    assert source.state.get(_SNAPSHOT_PROGRESS_KEY) is not None


# --------------------------------------------------------------------------------
# Round 6 red-first tests
# --------------------------------------------------------------------------------


# Each pair is what the two paths really produced for one row: the 4-byte value the
# binlog carries, and the value pymysql returns for the `SELECT` that renders it.
# Measured on MySQL 8.0.46, 8.4.2 and 9.4.0 - all three agree.
_MEASURED_FLOATS = [
    (0.3333333432674408, 0.333333),
    (0.10000000149011612, 0.1),
    (123.45600128173828, 123.456),
    (3.4028234663852886e38, 3.40282e38),
    (1.1754943508222875e-38, 1.17549e-38),
    (-7.699999809265137, -7.7),
    (16777216.0, 16777200.0),
    (19.989999771118164, 19.99),
]


@pytest.mark.parametrize(("binlog_value", "select_value"), _MEASURED_FLOATS)
def test_a_float_column_reaches_the_topic_the_same_way_on_both_paths(
    binlog_value, select_value
):
    """
    Round 6 #2: a FLOAT column must carry one value, not one per path.

    The binlog carries the 4-byte float itself; `SELECT` renders it with six
    significant digits, which is what the snapshot path ships.
    """
    _, binlog_row = serialize_binlog_values(
        values={"f": binlog_value}, table="db.tbl", float_columns=["f"]
    )
    snapshot_row = serialize_row(
        values=[select_value],
        columns=["f"],
        column_types={"f": ColumnType(data_type="float")},
        table="db.tbl",
    )

    assert binlog_row == snapshot_row == [select_value]


@pytest.mark.parametrize("value", [0.3333333333333333, 1.175494351e-38, -7.7, 19.99])
def test_a_double_column_is_shipped_unrounded_on_both_paths(value):
    """Round 6 #2: DOUBLE renders shortest-round-trip, so no rule may touch it."""
    _, binlog_row = serialize_binlog_values(
        values={"d": value}, table="db.tbl", float_columns=["f"]
    )
    snapshot_row = serialize_row(
        values=[value],
        columns=["d"],
        column_types={"d": ColumnType(data_type="double")},
        table="db.tbl",
    )

    assert binlog_row == snapshot_row == [value]


def test_an_event_whose_table_is_not_in_the_map_is_refused():
    """
    Round 6 #6: with no entry in the table map, nothing is known about the event's
    columns, character sets or ENUM/SET values, so it cannot be published.
    """
    event = SimpleNamespace(
        schema="db", table="tbl", table_id=7, table_map={}, columns=[]
    )

    with pytest.raises(MySqlCdcError, match="no table metadata"):
        event_to_changes(event)


def test_deciding_to_resume_a_forced_snapshot_writes_nothing():
    """
    Round 6 #5: the marker is dropped by the caller, once the position it applies to is
    settled - not inside the boolean expression that decides whether to drop it.
    """
    source = _RecordingSource(
        **_source_kwargs(initial_snapshot=True, force_snapshot=True)
    )
    source.state.set(_POSITION_KEY, {"log_file": "mysql-bin.000004", "log_pos": 120})
    source.state.set(
        _SNAPSHOT_PROGRESS_KEY,
        {"last_key": [1], "key_types": ["int"], "pk_columns": ["id"], "rows": 1},
    )
    source.state.set(_SNAPSHOT_COMPLETED_KEY, {"rows": 1})
    source.calls.clear()

    assert source._resuming_forced_snapshot(("mysql-bin.000004", 120)) is True
    assert source.calls == [], "deciding whether to resume wrote to the state store"
    assert source.state.get(_SNAPSHOT_COMPLETED_KEY) is not None

    assert source._resolve_start_position(snapshot_needed=True) == (
        "mysql-bin.000004",
        120,
    )
    assert source.state.get(_SNAPSHOT_COMPLETED_KEY) is None
    assert source.state.get(_SNAPSHOT_PROGRESS_KEY) is not None


def test_an_uncheckpointable_key_is_decided_once():
    """
    Round 6: a primary key with no JSON encoding does not acquire one partway through,
    so the snapshot must not re-encode it on every page of a large table.
    """
    source = _RecordingSource(**_source_kwargs(initial_snapshot=True))
    attempts = []

    class Progress:
        def resume_point(self, pk_columns):
            return None, 0

        def checkpoint(self, last_key, pk_columns, rows):
            attempts.append(rows)
            return False

        def mark_snapshot_completed(self, rows):
            pass

    class Helper:
        def plan_snapshot(self):
            return SnapshotPlan(
                pk_columns=["id"], columns=["id"], column_types={}, estimated_rows=40
            )

        def perform_initial_snapshot(self, plan, batch_size, start_after):
            for page in range(20):
                yield [{"kind": "snapshot_insert", "page": page}], [object()]

    source._progress = Progress()
    source._helper = Helper()
    source._committed_position = ("mysql-bin.000001", 4)
    source._check_snapshot_anchor = lambda *args: None
    source._running = True

    source._run_initial_snapshot()

    assert attempts == [1]


def test_the_snapshot_anchor_asks_both_questions_on_one_connection(monkeypatch):
    """Round 6: one check a minute, one connection - not one connection per question."""
    helper = mysql_helper.MySqlHelper(**_helper_kwargs())
    connections = []

    class Cursor:
        def __init__(self):
            self.rows = []

        def execute(self, sql, params=None):
            if sql.startswith("SHOW BINARY LOGS"):
                self.rows = [("mysql-bin.000007",)]
            else:
                self.rows = [("binlog_expire_logs_seconds", "3600")]

        def fetchall(self):
            return self.rows

        def fetchone(self):
            return self.rows[0]

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class Connection:
        def cursor(self, *args):
            return Cursor()

        def close(self):
            pass

    def connect(override_host=None):
        connections.append(override_host)
        return Connection()

    monkeypatch.setattr(helper, "connect_mysql", connect)

    assert helper.binlog_status("mysql-bin.000007") == (True, 3600)
    assert len(connections) == 1
