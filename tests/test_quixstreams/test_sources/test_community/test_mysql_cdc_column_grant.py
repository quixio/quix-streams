"""Does a column-level SELECT grant get refused instead of truncating the snapshot?"""

import threading
import time

import pytest

pytest.importorskip("pymysql")
pytest.importorskip("pymysqlreplication")
pytest.importorskip("testcontainers.mysql")

import pymysql
from testcontainers.mysql import MySqlContainer

from tests.test_quixstreams.test_sources.test_community.test_mysql_cdc_integration import (  # noqa: E501
    DATABASE,
    MYSQL_IMAGE,
    ROOT_PASSWORD,
    HarnessSource,
    count_kinds,
    of_kind,
)

GRANTED = "colu"
GRANTED_PW = "colupw"
COMMAND = (
    "--server-id=1 --log-bin=mysql-bin --binlog-format=ROW "
    "--binlog-row-image=FULL --binlog-row-metadata=FULL"
)


@pytest.fixture(scope="module")
def server():
    container = MySqlContainer(
        MYSQL_IMAGE,
        username="setup",
        password="setup",
        dbname=DATABASE,
        root_password=ROOT_PASSWORD,
    ).with_command(COMMAND)
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
            with root.cursor() as cur:
                cur.execute(
                    "CREATE TABLE partial_grant "
                    "(id INT PRIMARY KEY, name VARCHAR(30), secret VARCHAR(30), "
                    "amount INT, note VARCHAR(30))"
                )
                cur.execute(
                    "INSERT INTO partial_grant VALUES "
                    "(1,'ada','ssn-1',10,'n1'), (2,'grace','ssn-2',20,'n2')"
                )
                cur.execute(f"CREATE USER '{GRANTED}'@'%' IDENTIFIED BY '{GRANTED_PW}'")
                # Column-level SELECT only - no table-level SELECT on the table.
                cur.execute(
                    f"GRANT SELECT (id, name) ON {DATABASE}.partial_grant "
                    f"TO '{GRANTED}'@'%'"
                )
                cur.execute(
                    f"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* "
                    f"TO '{GRANTED}'@'%'"
                )
                cur.execute("FLUSH PRIVILEGES")
        finally:
            root.close()
        yield {"host": host, "port": port}


def test_column_level_grant_is_refused_at_setup(server):
    granted = pymysql.connect(
        host=server["host"],
        port=server["port"],
        user=GRANTED,
        password=GRANTED_PW,
        database=DATABASE,
        autocommit=True,
    )
    root = pymysql.connect(
        host=server["host"],
        port=server["port"],
        user="root",
        password=ROOT_PASSWORD,
        database=DATABASE,
        autocommit=True,
    )
    try:
        with granted.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='partial_grant' "
                "ORDER BY ORDINAL_POSITION",
                (DATABASE,),
            )
            visible = [r[0] for r in cur.fetchall()]
        print(f"\ninformation_schema.COLUMNS as {GRANTED}: {visible}")

        source = HarnessSource(
            state_data={},
            host=server["host"],
            port=server["port"],
            user=GRANTED,
            password=GRANTED_PW,
            database=DATABASE,
            table="partial_grant",
            initial_snapshot=True,
            commit_interval=0.2,
            poll_interval=0.05,
            shutdown_timeout=5,
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
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if error or count_kinds(source.received(), snapshot_insert=2):
                    break
                time.sleep(0.05)
        finally:
            source.stop()
            thread.join(timeout=30)

        assert error, (
            "the source accepted a column-level grant instead of refusing it at "
            "setup - snapshot and binlog would silently disagree on the schema"
        )
        print(f"SOURCE FAILED: {error[0]!r}")
        message = str(error[0])
        assert "may not read every column" in message
        assert "partial_grant" in message
        assert not of_kind(
            source.received(), "snapshot_insert"
        ), "no snapshot row should have been produced once setup refused"
    finally:
        granted.close()
        root.close()
