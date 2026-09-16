"""
Integration tests for the server-repair behaviour added in `ensure_row_settings`.

The source issues `SET GLOBAL binlog_row_metadata = FULL` and
`SET GLOBAL binlog_row_image = FULL` against the server it connects to, at start-up
and again whenever it rebuilds the stream. These tests cover what that does to the
server, what happens to writers that were already connected when it happened -
`binlog_row_image` is a SESSION variable seeded from the global at connect time, so an
existing connection keeps writing partial row images - and what happens when a restart
puts my.cnf back in charge.

All of them share one container and reset its two row settings before each test.

Requires Docker.
"""

import threading
import time
from typing import Any, Dict

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")
pytest.importorskip("testcontainers.mysql")

import pymysql
from testcontainers.mysql import MySqlContainer

from tests.test_quixstreams.test_sources.test_community.test_mysql_cdc_integration import (  # noqa: E501
    CDC_PASSWORD,
    CDC_USER,
    DATABASE,
    MYSQL_IMAGE,
    ROOT_PASSWORD,
    HarnessSource,
    count_kinds,
    execute,
    of_kind,
)

# The stock MySQL 8.x defaults: both row settings below FULL.
DEFAULTS_COMMAND = (
    "--server-id=1 --log-bin=mysql-bin --binlog-format=ROW "
    "--binlog-row-image=MINIMAL --binlog-row-metadata=MINIMAL"
)


@pytest.fixture(scope="module")
def stock_container():
    container = MySqlContainer(
        MYSQL_IMAGE,
        username=CDC_USER,
        password=CDC_PASSWORD,
        dbname=DATABASE,
        root_password=ROOT_PASSWORD,
    ).with_command(DEFAULTS_COMMAND)
    with container:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(3306))
        root = pymysql.connect(
            host=host,
            port=port,
            user="root",
            password=ROOT_PASSWORD,
            database=DATABASE,
            autocommit=True,
        )
        try:
            with root.cursor() as cursor:
                cursor.execute(
                    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, RELOAD, "
                    f"SYSTEM_VARIABLES_ADMIN ON *.* TO '{CDC_USER}'@'%'"
                )
                cursor.execute("FLUSH PRIVILEGES")
        finally:
            root.close()
        yield {"host": host, "port": port, "container": container}


@pytest.fixture()
def stock_server(stock_container):
    """
    The module's one container, with both row settings put back to MySQL's defaults.

    Every test here needs a server the source has not repaired yet - the writers they
    connect take their session `binlog_row_image` from the global - and each one leaves
    it repaired, so the reset is what makes one container enough for all of them.
    """
    root = root_connection(stock_container)
    try:
        with root.cursor() as cursor:
            cursor.execute("SET GLOBAL binlog_row_image = MINIMAL")
            cursor.execute("SET GLOBAL binlog_row_metadata = MINIMAL")
    finally:
        root.close()
    return stock_container


def root_connection(server: Dict[str, Any]):
    return pymysql.connect(
        host=server["host"],
        port=server["port"],
        user="root",
        password=ROOT_PASSWORD,
        database=DATABASE,
        autocommit=True,
    )


def connect(server: Dict[str, Any]):
    return pymysql.connect(
        host=server["host"],
        port=server["port"],
        user=CDC_USER,
        password=CDC_PASSWORD,
        database=DATABASE,
        autocommit=True,
    )


def global_var(conn: Any, name: str) -> str:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT @@GLOBAL.{name}")
        return str(cursor.fetchone()[0])


def session_var(conn: Any, name: str) -> str:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT @@SESSION.{name}")
        return str(cursor.fetchone()[0])


def build(server, table, state, **kwargs):
    params = {
        "host": server["host"],
        "port": server["port"],
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


class Running:
    """Runs the source on a thread and keeps whatever it raised."""

    def __init__(self, source):
        self.source = source
        self.error = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            self.source.start()
        except BaseException as exc:  # noqa: BLE001
            self.error = exc

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self.source.stop()
        self._thread.join(timeout=30)

    def wait_until(self, predicate, what, timeout=30.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {what}")


def test_source_rewrites_the_servers_global_row_settings(stock_server):
    """The source reconfigures the whole server, not just its own session."""
    observer = connect(stock_server)
    try:
        assert global_var(observer, "binlog_row_image") == "MINIMAL"
        assert global_var(observer, "binlog_row_metadata") == "MINIMAL"

        execute(observer, "DROP TABLE IF EXISTS repair_globals")
        execute(
            observer, "CREATE TABLE repair_globals (id INT PRIMARY KEY, a INT, b INT)"
        )

        source = build(stock_server, "repair_globals", {})
        source.setup()

        assert global_var(observer, "binlog_row_image") == "FULL"
        assert global_var(observer, "binlog_row_metadata") == "FULL"
    finally:
        observer.close()


def test_writer_connected_before_the_source_still_writes_partial_images(stock_server):
    """
    A writer that was already connected keeps its session `binlog_row_image=MINIMAL`,
    so its UPDATEs carry only the changed columns and the source cannot publish them.
    """
    writer = connect(stock_server)  # connected BEFORE the source repairs
    fresh = None
    try:
        execute(writer, "DROP TABLE IF EXISTS repair_writer")
        execute(writer, "CREATE TABLE repair_writer (id INT PRIMARY KEY, a INT, b INT)")
        execute(writer, "INSERT INTO repair_writer VALUES (1, 10, 100)")

        source = build(stock_server, "repair_writer", {})
        with Running(source) as running:
            time.sleep(1.5)  # let the stream anchor
            assert session_var(writer, "binlog_row_image") == "MINIMAL"
            execute(writer, "UPDATE repair_writer SET a = 11 WHERE id = 1")
            running.wait_until(
                lambda: running.error is not None
                or count_kinds(source.received(), update=1),
                "an update event or a failure",
            )
        old_writer_error = running.error
        old_writer_messages = source.received()

        # Control: a writer that connects AFTER the repair inherits FULL.
        fresh = connect(stock_server)
        assert session_var(fresh, "binlog_row_image") == "FULL"
        source2 = build(stock_server, "repair_writer", {})
        with Running(source2) as running2:
            time.sleep(1.5)
            execute(fresh, "UPDATE repair_writer SET b = 222 WHERE id = 1")
            running2.wait_until(
                lambda: running2.error is not None
                or count_kinds(source2.received(), update=1),
                "an update event or a failure",
            )
        assert running2.error is None, f"fresh writer failed too: {running2.error!r}"
        updates = of_kind(source2.received(), "update")
        assert updates, "fresh writer produced no update event"

        # The pre-existing writer is the one under test.
        assert old_writer_error is not None, (
            "expected the source to fail on the partial image; it produced "
            f"{old_writer_messages}"
        )
        assert "partial row image" in str(old_writer_error), repr(old_writer_error)
    finally:
        writer.close()
        if fresh is not None:
            fresh.close()


def test_force_snapshot_does_not_recover_while_the_old_writer_holds_its_session(
    stock_server,
):
    """
    The error the source raises tells the operator to restart with force_snapshot=True.
    That does not help while the writer that produced the partial image is still
    connected: the very next UPDATE from it fails the source again.
    """
    writer = connect(stock_server)  # connected BEFORE the source repairs the server
    try:
        execute(writer, "DROP TABLE IF EXISTS repair_recover")
        execute(
            writer, "CREATE TABLE repair_recover (id INT PRIMARY KEY, a INT, b INT)"
        )
        execute(writer, "INSERT INTO repair_recover VALUES (1, 10, 100)")

        state = {}
        source = build(stock_server, "repair_recover", state, initial_snapshot=True)
        with Running(source) as running:
            time.sleep(1.5)
            execute(writer, "UPDATE repair_recover SET a = 11 WHERE id = 1")
            running.wait_until(
                lambda: running.error is not None, "the first partial-image failure"
            )
        assert "partial row image" in str(running.error), repr(running.error)

        # The documented recovery, with the same writer still connected.
        recovered = build(
            stock_server,
            "repair_recover",
            state,
            initial_snapshot=True,
            force_snapshot=True,
        )
        with Running(recovered) as running2:
            time.sleep(1.5)
            execute(writer, "UPDATE repair_recover SET a = 12 WHERE id = 1")
            running2.wait_until(
                lambda: running2.error is not None
                or count_kinds(recovered.received(), update=1),
                "the documented recovery to either work or fail again",
            )
        assert running2.error is not None, (
            "force_snapshot recovered despite the old writer session; "
            f"received {recovered.received()}"
        )
        assert "partial row image" in str(running2.error), repr(running2.error)
    finally:
        writer.close()


def binary_logs(conn: Any) -> int:
    with conn.cursor() as cursor:
        cursor.execute("SHOW BINARY LOGS")
        return len(cursor.fetchall())


def test_settings_that_reverted_are_repaired_when_the_stream_is_rebuilt(stock_server):
    """
    Round 6 #1: a MySQL restart puts both row settings back to my.cnf, and every writer
    that connects after it strips the events this source cannot publish. The rebuild
    that follows the lost connection is the source's chance to notice, so that is what
    this drives - directly, rather than through whichever failure broke the link.

    The revert is done by hand because it is what a restart does to the two globals.
    """
    observer = connect(stock_server)
    writer = None
    try:
        execute(observer, "DROP TABLE IF EXISTS reverted")
        execute(observer, "CREATE TABLE reverted (id INT PRIMARY KEY, a INT)")

        source = build(stock_server, "reverted", {})
        source.setup()
        assert global_var(observer, "binlog_row_metadata") == "FULL"
        assert global_var(observer, "binlog_row_image") == "FULL"

        execute(observer, "SET GLOBAL binlog_row_metadata = MINIMAL")
        execute(observer, "SET GLOBAL binlog_row_image = MINIMAL")

        source._committed_position = source._helper.fetch_start_position()
        source._reconnect_stream()

        assert global_var(observer, "binlog_row_metadata") == "FULL"
        assert global_var(observer, "binlog_row_image") == "FULL"

        # A repaired server costs no rotation, however often the stream is rebuilt.
        logs = binary_logs(observer)
        for _ in range(5):
            source._reconnect_stream()
        assert binary_logs(observer) == logs

        writer = connect(stock_server)  # a fresh session inherits the repair
        execute(writer, "INSERT INTO reverted VALUES (1, 1)")
        changes, _ = source._stream.read_changes(
            max_rows=10, max_seconds=10, should_continue=lambda: True
        )

        row = dict(zip(changes[0]["columnnames"], changes[0]["columnvalues"]))

        assert [change["kind"] for change in changes] == ["insert"]
        assert row == {"id": 1, "a": 1}
    finally:
        source._close_stream()
        if writer is not None:
            writer.close()
        observer.close()
