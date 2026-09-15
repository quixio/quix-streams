import base64
from datetime import datetime
from decimal import Decimal

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")

from pymysql.err import InterfaceError, OperationalError

from quixstreams.models.messages import KafkaMessage
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
        (
            OperationalError(
                1236, "Could not find first log file name in binary log index file"
            ),
            True,
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
