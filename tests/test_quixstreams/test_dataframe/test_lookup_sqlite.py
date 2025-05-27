import os
import sqlite3
import tempfile
import time

import pytest

from quixstreams.dataframe.joins.lookups.sqlite import (
    SQLiteLookup,
    SQLiteLookupField,
    SQLiteLookupQueryField,
)


@pytest.fixture()
def sqlite_db():
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE test (id TEXT, col1 TEXT, col2 INTEGER)")
    conn.execute("INSERT INTO test (id, col1, col2) VALUES ('k1', 'foo', 1)")
    conn.execute("INSERT INTO test (id, col1, col2) VALUES ('k1', 'foo0', 0)")
    conn.execute("INSERT INTO test (id, col1, col2) VALUES ('k1', 'foo5', 5)")
    conn.execute("INSERT INTO test (id, col1, col2) VALUES ('k2', 'bar', 2)")
    conn.commit()
    conn.close()
    yield path
    os.remove(path)


def test_sqlite_lookup_basic(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)
    fields = {"f": SQLiteLookupField(table="test", columns=["col1", "col2"], on="id")}

    value = {}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo", "col2": 1}

    value = {}
    lookup.join(fields, "k2", value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "bar", "col2": 2}


def test_sqlite_lookup_cache(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)
    fields = {"f": SQLiteLookupField(table="test", columns=["col1"], on="id", ttl=0.1)}

    value = {}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    # Update DB directly
    with sqlite3.connect(sqlite_db) as conn:
        conn.execute("UPDATE test SET col1='baz' WHERE id='k1'")
        conn.commit()

    # Should return cached value
    value = {}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    # Wait for cache to expire
    time.sleep(0.11)
    value = {}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "baz"}


def test_sqlite_lookup_no_ttl(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)
    fields = {"f": SQLiteLookupField(table="test", columns=["col1"], on="id", ttl=0)}

    value = {}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "foo"}

    # Update DB directly
    with sqlite3.connect(sqlite_db) as conn:
        conn.execute("UPDATE test SET col1='baz' WHERE id='k1'")
        conn.commit()

    # Should return new value
    value = {}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == {"col1": "baz"}


def test_sqlite_lookup_orderby(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)
    fields = {
        "normal": SQLiteLookupField(table="test", columns=["col1"], on="id"),
        "orderby": SQLiteLookupField(
            table="test", columns=["col1"], on="id", order_by="col2"
        ),
        "asc": SQLiteLookupField(
            table="test",
            columns=["col1"],
            on="id",
            order_by="col2",
            order_by_direction="ASC",
        ),
        "desc": SQLiteLookupField(
            table="test",
            columns=["col1"],
            on="id",
            order_by="col2",
            order_by_direction="DESC",
        ),
    }

    value = {}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)
    assert value == {
        "normal": {"col1": "foo"},
        "orderby": {"col1": "foo0"},
        "asc": {"col1": "foo0"},
        "desc": {"col1": "foo5"},
    }


def test_sqlite_lookup_invalid_identifier(sqlite_db):
    with pytest.raises(ValueError):
        SQLiteLookupField(table="test;DROP TABLE test", columns=["col1"], on="id")

    with pytest.raises(ValueError):
        SQLiteLookupField(table="test", columns=["col1;DROP"], on="id")


def test_sqlite_lookup_default(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)
    fields = {
        "f": SQLiteLookupField(
            table="test", columns=["col1"], on="id", default="notfound"
        )
    }
    value = {}
    lookup.join(fields, "missing", value, key=None, timestamp=0, headers=None)
    assert value["f"] == "notfound"


def test_sqlite_lookup_error(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)
    # close the connection to simulate an error
    lookup._conn.close()

    fields = {
        "f": SQLiteLookupField(
            table="test", columns=["col1"], on="id", default="notfound"
        )
    }

    with pytest.raises(sqlite3.ProgrammingError):
        lookup.join(fields, "missing", {}, key=None, timestamp=0, headers=None)


def test_sqlite_lookup_query_field(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)
    fields = {
        "f": SQLiteLookupQueryField(
            query="SELECT col2 FROM test WHERE col1 = :field1", first_match_only=True
        )
    }

    value = {"field1": "foo"}
    lookup.join(fields, "key1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (1,)

    # Using the same target key will return the cached value
    value = {"field1": "bar"}
    lookup.join(fields, "key1", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (1,)

    value = {"field1": "bar"}
    lookup.join(fields, "key2", value, key=None, timestamp=0, headers=None)
    assert value["f"] == (2,)


def test_sqlite_lookup_query_field_missing(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)
    fields = {
        "f": SQLiteLookupQueryField(
            query="SELECT col2 FROM test WHERE col1 = :missing", first_match_only=True
        )
    }

    with pytest.raises(sqlite3.ProgrammingError):
        lookup.join(fields, "aa", {}, key=None, timestamp=0, headers=None)


def test_sqlite_lookup_field_and_query_field_all_rows(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)

    # SQLiteLookupField with first_match_only=False
    fields = {
        "f": SQLiteLookupField(
            table="test", columns=["col1", "col2"], on="id", first_match_only=False
        )
    }
    value = {}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)

    # Should return all rows with id='k1'
    assert isinstance(value["f"], list)
    assert {"col1": "foo", "col2": 1} in value["f"]
    assert {"col1": "foo0", "col2": 0} in value["f"]
    assert {"col1": "foo5", "col2": 5} in value["f"]
    assert len(value["f"]) == 3


def test_sqlite_lookup_query_field_all_rows(sqlite_db):
    lookup = SQLiteLookup(sqlite_db)

    # SQLiteLookupQueryField with first_match_only=False
    fields = {
        "f": SQLiteLookupQueryField(
            query="SELECT col1, col2 FROM test WHERE id = :id", first_match_only=False
        )
    }
    value = {"id": "k1"}
    lookup.join(fields, "k1", value, key=None, timestamp=0, headers=None)
    # Should return all col2 values for id='k1'
    assert isinstance(value["f"], list)
    assert value == {
        "id": "k1",
        "f": [("foo", 1), ("foo0", 0), ("foo5", 5)],  # SQLite returns rows as lists
    }
