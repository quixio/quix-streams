"""Measures how many MySQL connections an idle source opens per second."""

import threading
import time

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")
pytest.importorskip("testcontainers.mysql")

from tests.test_quixstreams.test_sources.test_community.test_mysql_cdc_integration import (  # noqa: E501
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


def connections_total(conn) -> int:
    with conn.cursor() as cursor:
        cursor.execute("SHOW GLOBAL STATUS LIKE 'Connections'")
        return int(cursor.fetchone()[1])


def test_idle_source_connection_rate(mysql, mysql_server):
    execute(mysql, "DROP TABLE IF EXISTS churn")
    execute(mysql, "CREATE TABLE churn (id INT PRIMARY KEY, a INT)")

    source = make_source(
        mysql_server, "churn", {}, commit_interval=5.0, poll_interval=0.1
    )
    error = []

    def run():
        try:
            source.start()
        except BaseException as exc:  # noqa: BLE001
            error.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        time.sleep(3.0)  # let it anchor and settle into the idle path
        before = connections_total(mysql)
        start = time.monotonic()
        time.sleep(10.0)
        elapsed = time.monotonic() - start
        after = connections_total(mysql)
    finally:
        source.stop()
        thread.join(timeout=30)

    assert not error, repr(error[0])
    rate = (after - before) / elapsed
    print(
        f"\nIDLE CONNECTION RATE: {after - before} connections in "
        f"{elapsed:.1f}s = {rate:.2f}/s"
    )
    assert rate < 2.0, f"idle source opens {rate:.2f} MySQL connections/second"
