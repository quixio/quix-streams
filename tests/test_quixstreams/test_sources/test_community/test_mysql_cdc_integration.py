"""
Integration tests for `MySqlCdcSource` against a real MySQL server in Docker.

Kafka is stubbed - the producer is a list and the state store is a dict - so these
tests exercise the real MySQL connection, the real binlog decoding and the real
snapshot pagination without needing a broker.

Requires Docker. Skipped automatically when Docker or the optional MySQL extras
are unavailable.
"""

import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")
pytest.importorskip("testcontainers.mysql")

import pymysql
from testcontainers.mysql import MySqlContainer

from quixstreams.sources.community.mysql_cdc.mysql_cdc import (
    MySqlCdcError,
    MySqlCdcSource,
)

MYSQL_IMAGE = "mysql:8.0"
CDC_USER = "cdc"
CDC_PASSWORD = "cdcpw"
ROOT_PASSWORD = "rootpw"
DATABASE = "testdb"

# `binlog_row_metadata=MINIMAL` is the MySQL 8.x default and is set explicitly here:
# it is the configuration under which the binlog carries no column names at all, so
# every column-name assertion below is really testing the INFORMATION_SCHEMA fallback.
MYSQL_COMMAND = (
    "--server-id=1 --log-bin=mysql-bin --binlog-format=ROW "
    "--binlog-row-image=FULL --binlog-row-metadata=MINIMAL"
)

WAIT_TIMEOUT = 60.0
POLL_SLEEP = 0.1


# --------------------------------------------------------------------- Kafka stubs


class _DictState:
    """`StatefulSource.state` backed by a plain dict that survives a restart."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def delete(self, key: str) -> None:
        self._data.pop(key, None)


class _CapturingProducer:
    """Stands in for `InternalProducer`, recording every produced message."""

    def __init__(self) -> None:
        self.messages: List[Tuple[Any, Any]] = []
        self._lock = threading.Lock()

    def produce(self, topic: str, value: Any = None, key: Any = None, **kwargs) -> None:
        with self._lock:
            self.messages.append((key, value))

    def flush(self, timeout: Optional[float] = None) -> int:
        return 0

    def snapshot(self) -> List[Tuple[Any, Any]]:
        with self._lock:
            return list(self.messages)


class HarnessSource(MySqlCdcSource):
    """
    `MySqlCdcSource` with only the Kafka side replaced.

    Everything MySQL-facing - `setup`, `run`, `_poll_once`, `_commit_batch`,
    `_run_initial_snapshot`, `_resolve_start_position` - is the real implementation.
    Serialization goes through the real source topic, so the change dicts must really
    be JSON-serializable.
    """

    def __init__(self, state_data: Dict[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._state = _DictState(state_data)
        self._capturing_producer = _CapturingProducer()
        self.configure(topic=self.default_topic(), producer=self._capturing_producer)

    @property
    def state(self) -> _DictState:  # type: ignore[override]
        return self._state

    def flush(self, timeout: Optional[float] = None) -> None:
        # Skip the store transaction (there is none); still flush the stub producer so
        # the real commit ordering in `_commit_batch` is exercised.
        if self.producer.flush(timeout) > 0:
            raise AssertionError("stub producer failed to flush")

    def received(self) -> List[Dict[str, Any]]:
        """Produced messages, decoded from the topic's JSON serializer."""
        return [
            dict(json.loads(value), _key=key)
            for key, value in self._capturing_producer.snapshot()
        ]


class RunningSource:
    """Drives `source.start()` on a background thread."""

    def __init__(self, source: HarnessSource):
        self.source = source
        self.error: Optional[BaseException] = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            self.source.start()
        except BaseException as exc:  # noqa: BLE001 - surfaced by the test
            self.error = exc

    def __enter__(self) -> "RunningSource":
        self._thread.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()

    def stop(self) -> None:
        self.source.stop()
        self._thread.join(timeout=WAIT_TIMEOUT)
        if self._thread.is_alive():
            raise AssertionError("the source thread did not stop")
        if self.error is not None:
            raise AssertionError(f"the source raised: {self.error!r}") from self.error

    def wait_for(
        self,
        predicate: Callable[[List[Dict[str, Any]]], bool],
        what: str,
        timeout: float = WAIT_TIMEOUT,
    ) -> List[Dict[str, Any]]:
        deadline = time.monotonic() + timeout
        while True:
            messages = self.source.received()
            if predicate(messages):
                return messages
            if self.error is not None:
                raise AssertionError(
                    f"the source raised while waiting for {what}: {self.error!r}"
                )
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"timed out after {timeout}s waiting for {what}; received "
                    f"{len(messages)} message(s):\n{json.dumps(messages, indent=2)}"
                )
            time.sleep(POLL_SLEEP)


# -------------------------------------------------------------------- MySQL helpers


def start_mysql_server():
    """
    Start a CDC-configured MySQL container, yielding its `{host, port}`.

    A plain generator rather than a fixture so sibling test modules can build their
    own module-scoped fixture on it without importing (and shadowing) this one's.
    """
    container = MySqlContainer(
        MYSQL_IMAGE,
        username=CDC_USER,
        password=CDC_PASSWORD,
        dbname=DATABASE,
        root_password=ROOT_PASSWORD,
    ).with_command(MYSQL_COMMAND)
    with container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(3306))
        root = pymysql.connect(
            host=host, port=port, user="root", password=ROOT_PASSWORD, database=DATABASE
        )
        try:
            with root.cursor() as cursor:
                cursor.execute(
                    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, RELOAD, "
                    f"SYSTEM_VARIABLES_ADMIN ON *.* TO '{CDC_USER}'@'%'"
                )
                cursor.execute("FLUSH PRIVILEGES")
            root.commit()
        finally:
            root.close()
        yield {"host": host, "port": port}


def open_connection(server: Dict[str, Any]):
    """Open an autocommitting connection as the CDC user, yielding it."""
    conn = pymysql.connect(
        host=server["host"],
        port=server["port"],
        user=CDC_USER,
        password=CDC_PASSWORD,
        database=DATABASE,
        autocommit=True,
    )
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture(scope="module")
def mysql_server():
    yield from start_mysql_server()


@pytest.fixture()
def mysql(mysql_server):
    yield from open_connection(mysql_server)


def execute(conn: Any, sql: str, args: Any = None) -> None:
    with conn.cursor() as cursor:
        cursor.execute(sql, args)


def executemany(conn: Any, sql: str, rows: Any) -> None:
    with conn.cursor() as cursor:
        cursor.executemany(sql, rows)


def make_source(mysql_server, table: str, state: Dict[str, Any], **kwargs: Any):
    params: Dict[str, Any] = {
        "host": mysql_server["host"],
        "port": mysql_server["port"],
        "user": CDC_USER,
        "password": CDC_PASSWORD,
        "database": DATABASE,
        "table": table,
        "commit_interval": 0.2,
        "poll_interval": 0.05,
        "shutdown_timeout": 5,
    }
    params.update(kwargs)
    return HarnessSource(state_data=state, **params)


# ------------------------------------------------------------------- shape helpers


def row_of(message: Dict[str, Any]) -> Dict[str, Any]:
    """The `columnnames`/`columnvalues` pair of a change event as a dict."""
    return dict(zip(message["columnnames"], message["columnvalues"]))


def oldkeys_of(message: Dict[str, Any]) -> Dict[str, Any]:
    oldkeys = message["oldkeys"]
    return dict(zip(oldkeys["keynames"], oldkeys["keyvalues"]))


def of_kind(messages: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
    return [message for message in messages if message["kind"] == kind]


def count_kinds(messages: List[Dict[str, Any]], **expected: int) -> bool:
    return all(len(of_kind(messages, kind)) >= n for kind, n in expected.items())


# -------------------------------------------------------------------------- tests


def test_snapshot_then_binlog_handover_loses_no_row(mysql, mysql_server):
    """Pre-existing rows arrive once via the snapshot, then live DML as change events."""
    table = "handover"
    execute(mysql, "DROP TABLE IF EXISTS handover")
    execute(
        mysql,
        "CREATE TABLE handover (id INT PRIMARY KEY, customer VARCHAR(50), amount INT)",
    )
    executemany(
        mysql,
        "INSERT INTO handover (id, customer, amount) VALUES (%s, %s, %s)",
        [(1, "ada", 100), (2, "grace", 200), (3, "linus", 300)],
    )

    source = make_source(mysql_server, table, {}, initial_snapshot=True)
    with RunningSource(source) as running:
        running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=3),
            "the 3 pre-existing rows to be snapshotted",
        )

        execute(
            mysql,
            "INSERT INTO handover (id, customer, amount) VALUES (4, 'edsger', 400)",
        )
        execute(mysql, "UPDATE handover SET amount = 250 WHERE id = 2")
        execute(mysql, "DELETE FROM handover WHERE id = 3")

        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1, update=1, delete=1),
            "the insert, update and delete change events",
        )

    snapshots = of_kind(messages, "snapshot_insert")
    assert [row_of(m) for m in snapshots] == [
        {"id": 1, "customer": "ada", "amount": 100},
        {"id": 2, "customer": "grace", "amount": 200},
        {"id": 3, "customer": "linus", "amount": 300},
    ]

    # The snapshot must come first: every snapshot row precedes every change event.
    kinds = [m["kind"] for m in messages]
    assert kinds.index("snapshot_insert") < kinds.index("insert")
    assert max(i for i, k in enumerate(kinds) if k == "snapshot_insert") < kinds.index(
        "insert"
    )

    inserted = of_kind(messages, "insert")
    assert len(inserted) == 1
    assert row_of(inserted[0]) == {"id": 4, "customer": "edsger", "amount": 400}

    updated = of_kind(messages, "update")
    assert len(updated) == 1
    assert row_of(updated[0]) == {"id": 2, "customer": "grace", "amount": 250}
    assert oldkeys_of(updated[0]) == {"id": 2, "customer": "grace", "amount": 200}

    deleted = of_kind(messages, "delete")
    assert len(deleted) == 1
    assert oldkeys_of(deleted[0]) == {"id": 3, "customer": "linus", "amount": 300}

    # No row of the original table is missing from the snapshot, and none is duplicated.
    assert sorted(row_of(m)["id"] for m in snapshots) == [1, 2, 3]


def test_snapshot_and_binlog_events_share_one_schema(mysql, mysql_server):
    """
    Snapshot and binlog events carry the same shape.

    The regression guarded here is `UNKNOWN_COL0..n`: on a server with the MySQL 8.x
    default `binlog_row_metadata=MINIMAL` the binlog carries no column names, so
    without the INFORMATION_SCHEMA fallback the change events would name their columns
    `UNKNOWN_COL0`, `UNKNOWN_COL1`, ... while snapshot rows kept real names - one topic
    with two incompatible schemas.
    """
    table = "schema_shape"
    execute(mysql, "DROP TABLE IF EXISTS schema_shape")
    execute(
        mysql,
        "CREATE TABLE schema_shape "
        "(id INT PRIMARY KEY, first_name VARCHAR(50), city VARCHAR(50), score INT)",
    )
    execute(
        mysql,
        "INSERT INTO schema_shape VALUES (1, 'ada', 'london', 10)",
    )

    expected_columns = ["id", "first_name", "city", "score"]

    source = make_source(mysql_server, table, {}, initial_snapshot=True)
    with RunningSource(source) as running:
        running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=1), "the snapshot row"
        )
        execute(mysql, "INSERT INTO schema_shape VALUES (2, 'grace', 'paris', 20)")
        execute(mysql, "UPDATE schema_shape SET score = 25 WHERE id = 2")
        execute(mysql, "DELETE FROM schema_shape WHERE id = 1")
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1, update=1, delete=1),
            "one change event of each kind",
        )

    snapshot = of_kind(messages, "snapshot_insert")[0]
    inserted = of_kind(messages, "insert")[0]
    updated = of_kind(messages, "update")[0]
    deleted = of_kind(messages, "delete")[0]

    # Same column-name keys on the snapshot path and the binlog path.
    assert snapshot["columnnames"] == expected_columns
    assert inserted["columnnames"] == expected_columns
    assert updated["columnnames"] == expected_columns
    assert updated["oldkeys"]["keynames"] == expected_columns
    assert deleted["oldkeys"]["keynames"] == expected_columns

    # Nothing anywhere degraded to positional placeholders.
    for message in messages:
        names = list(message["columnnames"]) + list(
            message["oldkeys"].get("keynames", [])
        )
        assert names, f"no column names at all on {message}"
        assert not [n for n in names if n.startswith("UNKNOWN_COL")], message

    # Same envelope: every event has the same top-level fields and message key.
    envelope = {"kind", "schema", "table", "columnnames", "columnvalues", "oldkeys"}
    for message in messages:
        assert set(message) - {"_key"} == envelope
        assert message["schema"] == DATABASE
        assert message["table"] == table
        key = message["_key"]
        key = key.decode() if isinstance(key, bytes) else key
        assert key == f"{DATABASE}.{table}"

    # ... and the real values travel with the real names.
    assert row_of(snapshot) == {
        "id": 1,
        "first_name": "ada",
        "city": "london",
        "score": 10,
    }
    assert row_of(inserted) == {
        "id": 2,
        "first_name": "grace",
        "city": "paris",
        "score": 20,
    }
    assert row_of(updated)["score"] == 25
    assert oldkeys_of(updated)["score"] == 20
    assert oldkeys_of(deleted) == {
        "id": 1,
        "first_name": "ada",
        "city": "london",
        "score": 10,
    }


def test_restart_resumes_from_state_without_losing_the_downtime_window(
    mysql, mysql_server
):
    """Changes made while the source is stopped must arrive after it restarts."""
    table = "downtime"
    execute(mysql, "DROP TABLE IF EXISTS downtime")
    execute(mysql, "CREATE TABLE downtime (id INT PRIMARY KEY, note VARCHAR(50))")
    execute(mysql, "INSERT INTO downtime VALUES (1, 'before')")

    state: Dict[str, Any] = {}

    first = make_source(mysql_server, table, state, initial_snapshot=True)
    with RunningSource(first) as running:
        running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=1), "the snapshot row"
        )
        execute(mysql, "INSERT INTO downtime VALUES (2, 'while-running')")
        running.wait_for(lambda m: count_kinds(m, insert=1), "the live insert")

    assert state.get(f"binlog_position_{DATABASE}_{table}") is not None

    # Downtime: the source is stopped, the table keeps changing.
    execute(mysql, "INSERT INTO downtime VALUES (3, 'during-downtime')")
    execute(mysql, "UPDATE downtime SET note = 'edited-during-downtime' WHERE id = 1")
    execute(mysql, "DELETE FROM downtime WHERE id = 2")

    second = make_source(mysql_server, table, state, initial_snapshot=True)
    with RunningSource(second) as running:
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1, update=1, delete=1),
            "the change events made during downtime",
        )

    # The snapshot is not re-run: the state says it already completed.
    assert of_kind(messages, "snapshot_insert") == []

    assert [row_of(m) for m in of_kind(messages, "insert")] == [
        {"id": 3, "note": "during-downtime"}
    ]
    updated = of_kind(messages, "update")
    assert len(updated) == 1
    assert row_of(updated[0]) == {"id": 1, "note": "edited-during-downtime"}
    assert oldkeys_of(updated[0]) == {"id": 1, "note": "before"}
    assert [oldkeys_of(m) for m in of_kind(messages, "delete")] == [
        {"id": 2, "note": "while-running"}
    ]


def test_snapshot_pagination_emits_every_row_exactly_once(mysql, mysql_server):
    """35 rows over a batch size of 10: no duplicate and no skip at the page seams."""
    table = "paginated"
    row_count = 35
    execute(mysql, "DROP TABLE IF EXISTS paginated")
    execute(mysql, "CREATE TABLE paginated (id INT PRIMARY KEY, label VARCHAR(50))")
    executemany(
        mysql,
        "INSERT INTO paginated (id, label) VALUES (%s, %s)",
        [(i, f"row-{i}") for i in range(1, row_count + 1)],
    )

    source = make_source(
        mysql_server, table, {}, initial_snapshot=True, snapshot_batch_size=10
    )
    with RunningSource(source) as running:
        messages = running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=row_count),
            f"all {row_count} snapshot rows",
        )

    snapshots = of_kind(messages, "snapshot_insert")
    rows = [row_of(m) for m in snapshots]
    assert len(rows) == row_count, f"expected {row_count} rows, got {len(rows)}"
    assert rows == [{"id": i, "label": f"row-{i}"} for i in range(1, row_count + 1)]
    assert len({row["id"] for row in rows}) == row_count


def test_composite_primary_key_snapshot_and_change_events(mysql, mysql_server):
    """A two-column primary key paginates and streams with real column names."""
    table = "composite"
    execute(mysql, "DROP TABLE IF EXISTS composite")
    execute(
        mysql,
        "CREATE TABLE composite "
        "(region VARCHAR(10), id INT, label VARCHAR(50), PRIMARY KEY (region, id))",
    )
    expected = [
        (region, id_, f"{region}-{id_}")
        for region in ("emea", "namer")
        for id_ in range(1, 7)
    ]
    executemany(
        mysql, "INSERT INTO composite (region, id, label) VALUES (%s, %s, %s)", expected
    )

    source = make_source(
        mysql_server, table, {}, initial_snapshot=True, snapshot_batch_size=5
    )
    with RunningSource(source) as running:
        running.wait_for(
            lambda m: count_kinds(m, snapshot_insert=len(expected)),
            f"all {len(expected)} snapshot rows",
        )
        execute(mysql, "INSERT INTO composite VALUES ('apac', 9, 'apac-9')")
        messages = running.wait_for(
            lambda m: count_kinds(m, insert=1), "the live insert"
        )

    rows = [row_of(m) for m in of_kind(messages, "snapshot_insert")]
    assert rows == [
        {"region": region, "id": id_, "label": label} for region, id_, label in expected
    ]
    assert row_of(of_kind(messages, "insert")[0]) == {
        "region": "apac",
        "id": 9,
        "label": "apac-9",
    }


def test_table_without_primary_key_fails_at_setup(mysql, mysql_server):
    """`initial_snapshot=True` on a PK-less table fails loudly before streaming."""
    table = "no_pk"
    execute(mysql, "DROP TABLE IF EXISTS no_pk")
    execute(mysql, "CREATE TABLE no_pk (id INT, label VARCHAR(50))")
    execute(mysql, "INSERT INTO no_pk VALUES (1, 'x')")

    source = make_source(mysql_server, table, {}, initial_snapshot=True)
    with pytest.raises(MySqlCdcError, match="has no PRIMARY KEY"):
        source.setup()

    # ... and streaming-only mode accepts the same table.
    streaming = make_source(mysql_server, table, {}, initial_snapshot=False)
    streaming.setup()
