"""Empirical checks of the two reviewer blockers that need a live server."""

import threading
import time

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")
pytest.importorskip("testcontainers.mysql")

from tests.test_quixstreams.test_sources.test_community.test_mysql_cdc_integration import (  # noqa: E501
    DATABASE,
    count_kinds,
    execute,
    make_source,
    open_connection,
    start_mysql_server,
)


@pytest.fixture(scope="module")
def mysql_server():
    yield from start_mysql_server()


@pytest.fixture()
def mysql(mysql_server):
    yield from open_connection(mysql_server)


class Bg:
    def __init__(self, source):
        self.source = source
        self.error = None
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            self.source.start()
        except BaseException as exc:  # noqa: BLE001
            self.error = exc

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *e):
        self.source.stop()
        self._t.join(timeout=30)

    def until(self, pred, what, timeout=30.0):
        """Wait for `pred`, reporting a source that died instead of satisfying it."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if pred():
                return
            if self.error is not None:
                raise AssertionError(
                    f"the source raised while waiting for {what}: {self.error!r}"
                )
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {what}")

    def until_failed(self, what, timeout=30.0):
        """Wait for the source to raise, which is what this one test wants."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if self.error is not None:
                return
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {what}")


def test_position_advances_over_other_tables_traffic(mysql, mysql_server):
    """
    Fixed behaviour: the committed position advances over foreign-table events too, so a
    quiet table's stored position does not age towards the server's binlog retention
    while other tables in the same database are busy.
    """
    execute(mysql, "DROP TABLE IF EXISTS quiet")
    execute(mysql, "DROP TABLE IF EXISTS noisy")
    execute(mysql, "CREATE TABLE quiet (id INT PRIMARY KEY, a INT)")
    execute(mysql, "CREATE TABLE noisy (id INT PRIMARY KEY, a INT)")

    state = {}
    source = make_source(mysql_server, "quiet", state, commit_interval=0.3)
    key = f"binlog_position_{DATABASE}_quiet"
    with Bg(source) as bg:
        bg.until(lambda: state.get(key) is not None, "the anchored position")
        anchored = dict(state[key])

        for i in range(200):  # heavy traffic on a DIFFERENT table
            execute(mysql, "INSERT INTO noisy VALUES (%s, %s)", (i, i))
        time.sleep(3.0)
        after_other_table = dict(state[key])

        execute(mysql, "INSERT INTO quiet VALUES (1, 1)")  # one event of our own
        bg.until(lambda: count_kinds(source.received(), insert=1), "our own insert")
        time.sleep(1.0)
        after_own = dict(state[key])

    assert bg.error is None, repr(bg.error)
    print(f"\nanchored          : {anchored['log_file']}:{anchored['log_pos']}")
    print(
        f"after 200 foreign : {after_other_table['log_file']}:"
        f"{after_other_table['log_pos']}"
    )
    print(f"after 1 own row   : {after_own['log_file']}:{after_own['log_pos']}")

    assert after_own["log_pos"] > anchored["log_pos"], "own event did not advance it"
    assert after_other_table["log_pos"] > anchored["log_pos"], (
        "foreign traffic did not advance the committed position - a quiet table's "
        "position would age towards the server's binlog retention"
    )


def test_replay_of_pre_full_metadata_events_is_refused(mysql, mysql_server):
    """
    Fixed behaviour: events written before the source raised binlog_row_metadata carry
    no ENUM/SET metadata, so replaying them is refused with a `MySqlCdcError` rather
    than shipping ENUM as null and SET as "" with no error.
    """
    execute(mysql, "DROP TABLE IF EXISTS replay_enum")
    execute(
        mysql,
        "CREATE TABLE replay_enum (id INT PRIMARY KEY, "
        "status ENUM('new','paid','shipped'), tags SET('a','b','c'))",
    )
    execute(mysql, "INSERT INTO replay_enum VALUES (1, 'new', 'a')")

    state = {}
    # Run once so the source anchors a position, then stop it.
    first = make_source(mysql_server, "replay_enum", state, commit_interval=0.3)
    with Bg(first) as bg:
        bg.until(
            lambda: state.get(f"binlog_position_{DATABASE}_replay_enum") is not None,
            "the anchored position",
        )
    assert bg.error is None, repr(bg.error)

    # The server reverts to the MySQL 8.x default, and writes happen while it is there.
    execute(mysql, "SET GLOBAL binlog_row_metadata = MINIMAL")
    execute(mysql, "INSERT INTO replay_enum VALUES (2, 'paid', 'a,b')")
    execute(mysql, "UPDATE replay_enum SET status = 'shipped' WHERE id = 2")

    # The source restarts: it sets metadata back to FULL, then replays the window.
    second = make_source(mysql_server, "replay_enum", state, commit_interval=0.3)
    with Bg(second) as bg2:
        bg2.until_failed("the source to refuse the replay")

    print(f"\nsource error: {bg2.error!r}")
    assert bg2.error is not None, (
        "the source replayed the pre-FULL window instead of refusing it - ENUM would "
        "ship as null and SET as an empty string with no error"
    )
    message = str(bg2.error)
    assert "binlog_row_metadata was below FULL" in message
    assert "replay_enum" in message
    assert not count_kinds(
        second.received(), insert=1, update=1
    ), "no rows from the pre-FULL window should have been published"


def test_force_snapshot_discards_progress_on_every_restart(mysql, mysql_server):
    """
    Reviewer claim: force_snapshot is static config, and every start wipes the snapshot
    progress key, so an interrupted forced snapshot restarts from row 0 forever.
    """
    execute(mysql, "DROP TABLE IF EXISTS forced")
    execute(mysql, "CREATE TABLE forced (id INT PRIMARY KEY, a INT)")
    for i in range(1, 21):
        execute(mysql, "INSERT INTO forced VALUES (%s, %s)", (i, i))

    state = {}
    progress_key = f"snapshot_progress_{DATABASE}_forced"

    # Simulate a forced snapshot interrupted at row 8.
    source = make_source(
        mysql_server,
        "forced",
        state,
        initial_snapshot=True,
        force_snapshot=True,
        snapshot_batch_size=4,
    )
    source.setup()
    state[progress_key] = {
        "last_key": [8],
        "key_types": ["int"],
        "pk_columns": ["id"],
        "rows": 8,
        "updated_at": time.time(),
    }
    state[f"binlog_position_{DATABASE}_forced"] = {
        "log_file": "mysql-bin.000001",
        "log_pos": 4,
        "committed_at": time.time(),
    }
    print(f"\nprogress before restart: {state.get(progress_key)}")

    # The next start of the same (still force_snapshot=True) deployment.
    restarted = make_source(
        mysql_server,
        "forced",
        state,
        initial_snapshot=True,
        force_snapshot=True,
        snapshot_batch_size=4,
    )
    restarted.setup()
    needed = restarted._is_snapshot_needed()
    restarted._resolve_start_position(needed)
    print(f"progress after restart : {state.get(progress_key)}")

    assert state.get(progress_key) is not None, (
        "force_snapshot discarded the interrupted snapshot's progress: the forced "
        "snapshot restarts from row 0 on every process restart"
    )
