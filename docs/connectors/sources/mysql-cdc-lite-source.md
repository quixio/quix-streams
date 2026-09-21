# MySQL CDC Lite Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source streams row-level changes from a single MySQL table to a Kafka topic by
reading the server's binary log.

It is deliberately small. It checks two things — that `binlog_format` is `ROW`, and
that the table exists — and assumes everything else. **The checks it does not run have
not made the failures go away.** Each one moved from a start-up error with a fix in it
to the [checklist](#before-you-deploy-the-checklist) below, and most of them became
silent on the way.

Work through that checklist before you deploy. If you are not going to, do not use this
connector.

## How To Install

```bash
pip install quixstreams[mysql]
```

## How It Works

`MySqlCdcLiteSource` registers with MySQL as a replication client and reads the binary
log, filtered to one schema and one table. Each `INSERT`, `UPDATE` and `DELETE` row
becomes one Kafka message.

There is **no initial snapshot**. The topic begins at the server's binlog position at
the moment the source first starts, and carries every change from that point on. Rows
that were already in the table when the source started are never published. That
position is read once, before the stream opens, so a dropped connection is rebuilt from
where the source had got to rather than from wherever the server has moved on to.

Changes are buffered, produced and flushed, and only then is the binlog position they
cover committed to the source's state store. A restart resumes from that committed
position, so changes made while the source was down arrive when it comes back.

Every message is keyed `"<database>.<table>"`, so all changes to one table land on one
partition and stay in binlog order.

## How To Use

```python
from quixstreams import Application
from quixstreams.sources.community.mysql_cdc_lite import MySqlCdcLiteSource

source = MySqlCdcLiteSource(
    host="localhost",
    port=3306,
    user="cdc_user",
    password="cdc_password",
    database="test_database",
    table="test_table",
)

app = Application(
    broker_address="localhost:9092",
    consumer_group="mysql-cdc",
)

sdf = app.dataframe(source=source).print(metadata=True)
# YOUR LOGIC HERE!

if __name__ == "__main__":
    app.run()
```

Run it with **exactly one replica**. The replication client id is derived from `name`,
`database` and `table`, so every replica of one deployment derives the same id and
MySQL evicts them in turn. See [§6](#6-the-replication-client-id-is-unique).

## Before you deploy: the checklist

Run these as an admin user on the server the source will replicate from. Every one is a
single statement. Everything below was measured against `mysql:8.0` with this source;
the test that produced each observation is named next to it, in
`tests/test_quixstreams/test_sources/test_community/test_mysql_cdc_lite.py`.

### 1. `binlog_format` is `ROW`

```sql
SHOW GLOBAL VARIABLES LIKE 'binlog_format';   -- must print ROW
```

**The source checks this one.** It refuses to start otherwise, because any other format
puts SQL statements in the binary log instead of row images and there is nothing for a
CDC source to read. If the variable does not exist at all (MySQL 9.x removed it) the
source accepts that and continues.

### 2. `binlog_row_metadata` is `FULL`

```sql
SHOW GLOBAL VARIABLES LIKE 'binlog_row_metadata';   -- must print FULL
```

**MySQL 8.x ships `MINIMAL`, so on a default server this one is wrong.** Set
`binlog_row_metadata = FULL` in `my.cnf` and restart, or `SET GLOBAL
binlog_row_metadata = FULL` and add it to `my.cnf` so it survives the next restart.
Events already written stay as they are — rotate with `FLUSH BINARY LOGS` before you
start the source if you want a clean file.

If it is `MINIMAL`, this is what arrives on the topic for
`INSERT INTO t VALUES (1, 200, 'a,b', 'done')` where the columns are `INT`,
`TINYINT UNSIGNED`, `SET('a','b')`, `ENUM('new','done')`
(`test_minimal_row_metadata_is_silently_wrong`):

```json
{
  "columnnames": ["UNKNOWN_COL0", "UNKNOWN_COL1", "UNKNOWN_COL2", "UNKNOWN_COL3"],
  "columnvalues": [1, -56, null, null]
}
```

- Column names are gone. Downstream sees `UNKNOWN_COL0..n` — loud, you will notice.
- `200` in an `UNSIGNED` column arrives as **`-56`**. Silent. A valid number, the wrong
  one, and nothing downstream can tell.
- The `SET` and the `ENUM` arrive as **`null`**. Silent, and indistinguishable from a
  real SQL NULL.
- A binary column whose bytes are not valid UTF-8 raises inside the decoder instead,
  and the source dies on it.

### 3. `binlog_row_image` is `FULL`

```sql
SHOW GLOBAL VARIABLES LIKE 'binlog_row_image';   -- must print FULL
```

`FULL` is the default, so this usually passes — but the variable has **session scope**,
so a writer that connected before the global was raised keeps using its old value. On a
writer's own connection:

```sql
SELECT @@session.binlog_row_image;   -- must print FULL on every writer
```

If it is `MINIMAL`, an `UPDATE t SET changed='after' WHERE id=1` on a three-column row
arrives like this (`test_minimal_row_image_truncates_an_update`):

```json
{
  "columnnames":  ["id", "kept", "changed"],
  "columnvalues": [null, null, "after"],
  "oldkeys": {"keynames": ["id", "kept", "changed"], "keyvalues": [1, null, null]}
}
```

Every column is still named, so the message is shaped exactly like a complete row. The
columns the statement did not touch are `null`. A consumer that upserts this row
**writes `kept` to NULL and loses the primary key from the after-image** — and it has no
way to tell this apart from an update that genuinely set those columns to NULL. Silent,
and it corrupts the target.

### 4. The grants

```sql
SHOW GRANTS FOR 'cdc'@'%';
```

Exactly this set is enough
(`test_the_documented_grant_set_is_exactly_what_is_needed`):

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
GRANT SELECT ON mydb.mytable TO 'cdc'@'%';
```

The two replication privileges alone are **not** enough: the source opens one ordinary
connection with the database as its default, and neither of them is a schema privilege,
so MySQL answers `(1044, "Access denied for user 'cdc'@'%' to database 'mydb'")` at
start-up. That one is loud and immediate. `SELECT` on the single table satisfies it; the
stream itself never reads a row through SQL, so a wider grant buys nothing.

### 5. `binlog_expire_logs_seconds` outlasts your longest downtime

```sql
SHOW GLOBAL VARIABLES LIKE 'binlog_expire_logs_seconds';   -- 2592000 = 30 days
SHOW BINARY LOGS;                                          -- what is actually on disk
```

The source resumes from the position it last committed. If MySQL has purged the file
that position is in, those changes are gone from the server and there is no way to get
them. The source does not check retention up front, but it does recognise MySQL's answer
when it arrives and stops with the cause named, rather than retrying a file that is
never coming back (`test_a_purged_position_kills_the_source`):

```
MySqlCdcLiteError: MySQL no longer holds the binlog position committed for
mydb.mytable (('mysql-bin.000003', 1352)): that file has been purged, and the changes
it covered are gone from the server. This source has no snapshot to re-read them with,
and restarting it reads the same position back out of state and fails here again.
Raise binlog_expire_logs_seconds on <host> so it exceeds the longest downtime you
expect, then re-seed this source: give it a different `name`, which starts it from the
server's current position with a state store of its own and accepts the gap.
```

It will still restart and fail again — the stored position is left alone deliberately,
because discarding it is a decision about accepting data loss and not one a connector
should make for you. But the cause is in the first line the source logs, instead of
being inferred from a loop of driver errors. Set the retention higher than your worst
deployment outage, and check `SHOW BINARY LOGS` rather than trusting the variable —
MySQL 8.4's `binlog_space_limit` can purge files well before they expire.

### 6. The replication client id is unique

The source does not take a `server_id`. It derives one from `name`, `database` and
`table`:

```bash
python -c "import zlib; print(1000 + zlib.crc32(b'<name>|<database>|<table>') % (2**31 - 1 - 1000))"
```

with `<name>` defaulting to `mysql_cdc_lite_<database>_<table>`. It is printed on the
first line the source logs. Two things must hold:

```sql
SELECT @@server_id;   -- must not equal the derived id
```

and no other replication client — another CDC deployment, a real replica, a second
replica of *this* deployment — may use it. Because it is derived, **every replica of one
deployment derives the same id**, so run this source with exactly one replica. Two
deployments against the same server need different `name` values.

When it collides, MySQL evicts one client as soon as the other connects, and error 1236
comes back naming `server_uuid/server_id`. The two evict each other in turn, so neither
makes progress; the source retries three times and exits, is restarted, and evicts the
other one again. It looks like an unstable network.

### 7. `binlog_transaction_compression` is `OFF`

```sql
SHOW GLOBAL VARIABLES LIKE 'binlog_transaction_compression';   -- must print OFF
```

`OFF` is the default (MySQL 8.0.20 added the variable), so this usually passes. When it
is `ON`, MySQL writes each transaction as a single compressed payload event, and the
`mysql-replication` reader this source is built on has no support for that event type:
it is neither unpacked nor reported. Measured on 8.0.46 with the variable on, an
`INSERT` of three rows produced **zero** messages, and the position the source kept was
the server's own position *after* that transaction — a source restarted there delivered
only the rows written later. Those changes are not delayed, they are gone, and there is
nothing on the topic and nothing in the log to say so. The source does not check this
variable.

## What this connector does not protect you from

| Not done | What it means for you |
|---|---|
| No initial snapshot | The topic starts at the server's current binlog position. Rows already in the table are never published — there is no history, only changes from start-up on. |
| No `binlog_row_metadata` check or repair | [§2](#2-binlog_row_metadata-is-full): wrong values, silently, with no warning on the topic or in the log. |
| No `binlog_row_image` check | [§3](#3-binlog_row_image-is-full): truncated updates that look complete and corrupt whatever consumes them. |
| No partial-row-image refusal | The same thing from the other side: a row with columns missing is published rather than refused. |
| No partial-JSON refusal | With `binlog_row_value_options = PARTIAL_JSON`, MySQL writes such an `UPDATE` as a `PARTIAL_UPDATE_ROWS_EVENT`, which this source does not ask for and the reader therefore drops at packet level. Measured: the whole `UPDATE` produces **no message at all** — the non-JSON columns with it — and the position advances past it, so a restart does not bring it back. Leave that variable empty. |
| No retention guard | [§5](#5-binlog_expire_logs_seconds-outlasts-your-longest-downtime): a purged position is named when it happens, but nothing warns you beforehand that retention is shorter than your worst downtime. |
| No `server_id` collision detection | [§6](#6-the-replication-client-id-is-unique): eviction ping-pong that reads as a flaky network. |
| No reconnect-churn bound | Three *consecutive* failures are retried; the fourth kills the source. One that fails, reconnects, and fails again every thirty seconds forever is not detected — it just runs slowly and nobody is told. |
| No TLS verification by default | `tls=True` encrypts and does not authenticate: no CA, no hostname check. A machine-in-the-middle is not detected. Pass a CA file path as `tls` to verify the certificate and the hostname, or keep the default on a trusted network only. |
| No `server_id` parameter | You cannot set one. If the derived id collides, the only lever is `name`, which also resets the state store — so changing it to fix a collision *also skips the downtime window*. |
| No `binlog_transaction_compression` check | [§7](#7-binlog_transaction_compression-is-off): a compressed transaction is not unpacked, produces nothing, and the position moves past it. Silent from both ends. |
| No parameter validation | `commit_interval=0` and friends are accepted and misbehave in their own ways. |
| One statement is buffered whole | The buffer bound (1000 changes, not configurable) is a floor, not a ceiling, and is honoured only at a transaction or statement boundary this source has observed. MySQL splits one statement's rows across a row event per `binlog_row_event_max_size` (8 KB by default) — 5000 rows came out as ten events — and only the end of that group is a position the source can resume from, so a statement that changes a million rows buffers all of it. The scan bound is honoured at the same boundaries, so such a statement is also read whole: a `SIGTERM` that arrives anywhere inside it — on the table map that opens it as much as on any of its row events — waits for the end of the statement and can outlast the shutdown budget into a `SIGKILL`. A statement that writes a second table as well — through a trigger, or a multi-table `UPDATE` — is bounded at the transaction's commit instead, because MySQL marks the end of such a statement on the other table's rows, which this source is filtered away from and never receives. |

Things it does guarantee, and that were kept deliberately:

- **At-least-once.** Changes are produced and flushed *before* the position that covers
  them is committed. A crash in between replays the last batch — never drops it.
  Deduplicate downstream on the primary key plus `kind`.
- **The position lives in Kafka state, not on disk.** An ephemeral container filesystem
  does not cost you the downtime window
  (`test_restart_resumes_and_delivers_the_downtime_window`).
- **A reconnect does not skip changes.** The start position is resolved before the first
  stream opens, so a connection dropped before the first commit resumes where the source
  started rather than where the server has since got to
  (`test_a_reconnect_before_the_first_commit_skips_nothing`).
- **A restart before the first commit does not skip changes either.** The resolved start
  position is written to state before the stream opens, so a process that dies with
  nothing produced still leaves its successor a position to resume from rather than an
  empty store. Committing it that early can only replay, never drop
  (`test_a_process_that_dies_before_its_first_commit_skips_nothing`).
- **A stop drains the buffer.** `SIGTERM` while changes are buffered produces them
  rather than dropping them (`test_stop_drains_the_buffer`).
- **A stop is noticed during a scan, not only between scans.** The read is bounded per
  binlog event, including the events of other tables that get skipped, so a stream of
  ordinary statements cannot hold the source past its shutdown budget. Both bounds are
  honoured only at a transaction or statement boundary this source has observed: one
  landing on a statement's table map, or on any of its row events, reads that statement
  whole before it stops, because no earlier point in it can be resumed from
  (`test_a_scan_bound_landing_on_a_table_map_reads_the_statement_whole`). One statement
  is therefore the exception to the budget, as the row above says. When the boundary is
  on events this source never receives — a statement that also writes another table —
  the stop keeps the position from *before* that statement and the restart reads it
  again, so landing inside one costs duplicates rather than rows
  (`test_a_bound_on_a_trigger_written_table_map_delivers_the_row`).
- **A typo fails at start-up.** A `database` or `table` that does not exist, or that the
  user cannot see, is an error out of `setup()` naming both — not a source that runs
  quietly forever producing nothing (`test_a_missing_table_fails_at_setup`).
- **A purged position is named.** MySQL 1236 for a missing log file becomes an error
  saying which position is gone and what to do about it, rather than three retries and a
  bare driver exception.
- **The values are encoded, not stringified.** `DECIMAL`, `SET`, `ENUM`, `JSON`, `TIME`,
  `DATETIME`, `BIT`, binary and unsigned columns all have defined encodings, and every
  row of the table above is compared against the server's own rendering of the same
  value — `HEX`, `BIN`, `CAST(v AS CHAR)` — rather than against a value written down here
  (`test_every_documented_type_matches_the_servers_own_rendering`).

## Configuration

Here are some important configurations to be aware of (see [MySQL CDC Lite Source API](../../api-reference/sources.md#mysqlcdclitesource) for all parameters).

### Required:

- `host`: MySQL server hostname to replicate from.
- `user`: MySQL username. Needs the grants in [§4](#4-the-grants).
- `password`: MySQL password.
- `database`: database (schema) containing the table.
- `table`: table to stream changes from.

### Optional:

- `port`: MySQL server port.
    **Default**: `3306`
- `commit_interval`: how often (seconds) to produce the buffered changes and commit the
  binlog position they cover. The source reads at the start and at the end of each
  interval, so it bounds the delay between a change and its message; one read may
  itself run for an interval on a busy server. That read bound — and the internal
  1000-change buffer bound that commits early while catching up after downtime — is
  honoured only at a transaction or statement boundary this source has observed,
  because a position inside a statement cannot be resumed from, so one statement is
  read and buffered whole however many rows it changes. That same boundary rule bounds how long a shutdown
  takes: a `SIGTERM` arriving inside a statement group is not acted on until the group
  closes, so a single huge statement — or a run of statements MySQL never marks the end
  of, such as a trigger's — can hold the source past `shutdown_timeout` and into a
  `SIGKILL`.
    **Default**: `5.0`
- `tls`: how to connect to MySQL. `True` encrypts and does **not** verify the server
  certificate; a path to a CA file (`tls="/etc/ssl/mysql-ca.pem"`) encrypts and verifies
  the certificate and the hostname against it; `False` connects in plaintext.
    **Default**: `True`
- `name`: the source unique name. It is used to generate the default topic name, the
  state store name and the derived replication client id; renaming a source therefore
  resets its committed position.
    **Default**: `mysql_cdc_lite_<database>_<table>`
- `shutdown_timeout`: time in seconds the application waits for the source to gracefully
  shut down.
    **Default**: `10`
- `on_client_connect_success`: Optional callback for successful client authentication.
- `on_client_connect_failure`: Optional callback for failed client authentication.

## Message Data Format/Schema

Message `key` is the string `"<database>.<table>"`.

Message `value` is a JSON object with a fixed envelope:

```json
{
  "kind": "update",
  "schema": "mydb",
  "table": "mytable",
  "columnnames": ["id", "customer", "amount"],
  "columnvalues": [1, "ada", 250],
  "oldkeys": {
    "keynames": ["id", "customer", "amount"],
    "keyvalues": [1, "ada", 100]
  }
}
```

- `kind` is `"insert"`, `"update"` or `"delete"`.
- `columnnames`/`columnvalues` carry the row *after* the change. They are empty lists
  for a `"delete"`.
- `oldkeys` carries the row *before* the change, as `keynames`/`keyvalues`. It is `{}`
  for an `"insert"`.

Column values are encoded so the result is JSON:

| MySQL type | Encoded as |
|---|---|
| `DECIMAL` | string in the plain form MySQL prints, at the column's declared scale, e.g. `"12.34"` — never exponent notation, however small the value |
| `FLOAT`, `DOUBLE` | number at full `double` precision — `1.1` stored in a `FLOAT` ships as `1.100000023841858`, the exact value MySQL holds, not the six digits `SELECT` prints |
| `JSON` | a nested JSON value, not a string holding JSON text |
| `SET` | comma-joined, sorted, e.g. `"a,c"`. An **empty** `SET` arrives as `null`, indistinguishable from SQL NULL |
| `ENUM` | its string label |
| `BINARY`, `VARBINARY`, `BLOB` | base64 string of the bytes MySQL stores. A `BINARY(N)` is re-padded to `N` bytes with `0x00` first — the row image carries it with the pad trimmed off, and the column length is what a replica re-pads from |
| `BIT(N)` | string of `N` `"0"`/`"1"` characters, most significant first, e.g. `"10000001"` |
| `DATE`, `DATETIME`, `TIMESTAMP` | ISO-8601 string |
| `TIME` | `[-]HH:MM:SS` with the column's `fsp` fraction digits, as MySQL prints it (hours up to 838): none at all for a `TIME(0)`, and `fsp` of them otherwise even when the fraction is zero |

## Processing/Delivery Guarantees

`MySqlCdcLiteSource` provides **at-least-once** delivery. The binlog position is
committed only after the changes it covers have been flushed, so a crash between the two
replays the last batch.

Consumers must deduplicate on the primary-key columns inside `columnvalues`/`oldkeys`
together with `kind`.

## Testing Locally

Requires Docker.

1. Run a MySQL server configured as the checklist requires:

    ```bash
    docker run --rm -d --name mysql-cdc -p 3306:3306 \
      -e MYSQL_ROOT_PASSWORD=root_password \
      -e MYSQL_DATABASE=test_db \
      mysql:8.0 \
      --server-id=1 --log-bin=mysql-bin --binlog-format=ROW \
      --binlog-row-image=FULL --binlog-row-metadata=FULL
    ```

2. Create the replication user:

    ```bash
    docker exec -i mysql-cdc mysql -uroot -proot_password -e "
      CREATE USER 'cdc'@'%' IDENTIFIED BY 'cdc_password';
      GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
      GRANT SELECT ON test_db.* TO 'cdc'@'%';"
    ```

3. Point the source at it with TLS off:

    ```python
    source = MySqlCdcLiteSource(
        host="localhost",
        port=3306,
        user="cdc",
        password="cdc_password",
        database="test_db",
        table="test_table",
        tls=False,
    )
    ```

The connector's own test suite brings the same container up itself:

```bash
python -m pytest tests/test_quixstreams/test_sources/test_community/test_mysql_cdc_lite.py
```

Twenty-three tests, one MySQL container, about 30 seconds. Nineteen prove the source works;
four exist to keep this page honest, and will fail if MySQL's behaviour stops matching
what is written above.
