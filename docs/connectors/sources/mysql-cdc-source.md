# MySQL CDC Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source streams row-level changes (inserts, updates and deletes) from a single
MySQL table into a Kafka topic by acting as a MySQL replication client and reading the
server's binary log. It can optionally take an initial snapshot of the table's current
contents before it starts streaming changes.


## How To Install

Install Quix Streams with the following optional dependencies:

```bash
pip install quixstreams[mysql]
```


## How It Works

`MySqlCdcSource` connects to MySQL as a replication client and reads row events for one
database and table. Each read cycle buffers the change events it decoded, and on every
commit (by default every 5 seconds, or sooner once `max_buffer_size` changes have piled
up) the source:

1. produces every buffered change to the Kafka topic,
2. flushes the producer and waits for the broker to acknowledge them,
3. only then writes the binary-log position those changes cover into the source's
   state store, and flushes again.

The position is therefore never ahead of the data: a crash can only replay the last
batch, never skip it. See [Processing/Delivery Guarantees](#processingdelivery-guarantees).

On start-up the source reads its committed position from the state store and resumes
the stream there. On a first run it resolves a starting position from the server and
commits it *before* producing anything, so even a crash before the first change event
resumes from a known point instead of from "now".

You can learn more details about the [expected Kafka message format](#message-data-formatschema) below.


## How To Use

To use the MySQL CDC Source, hand `MySqlCdcSource` to `app.dataframe()`.

For more details around various settings, see [configuration](#configuration).

```python
from quixstreams import Application
from quixstreams.sources.community.mysql_cdc import MySqlCdcSource


source = MySqlCdcSource(
    host="localhost",
    port=3306,
    user="cdc_user",
    password="cdc_password",
    database="test_database",
    table="test_table",
    initial_snapshot=True,
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


## Configuration

Here are the important configurations to be aware of (see
[MySQL CDC Source API](../../api-reference/sources.md#mysqlcdcsource) for all parameters).

### Required:

- `host`: the MySQL server to replicate from.
- `user`: MySQL username, with `REPLICATION SLAVE`, `REPLICATION CLIENT` and `SELECT` on
    the table; see [MySQL Prerequisites](#mysql-prerequisites).
- `password`: MySQL password.
- `database`: the database (schema) containing the table.
- `table`: the table to stream changes from.

### Optional:

- `port`: MySQL server port.
    **Default**: `3306`
- `server_id`: the replication client id announced to MySQL. Must be unique across
    every replica and CDC client connected to the same server. The derived default is a
    pure function of `name`, `database` and `table`, so **every replica of one
    deployment derives the same id**: run `MySqlCdcSource` with exactly one replica. Two
    deployments against the same server need a distinct `name` **and**, if they read the
    same table, a distinct `server_id`. If two clients do share an id, MySQL evicts them
    in turn and the source exits after the third eviction rather than reconnecting
    forever.
    **Default**: `None` — a value derived from `name`, `database` and `table`, in the
    range `[1000, 2147483647]`.
- `initial_snapshot`: snapshot the table's current contents before streaming changes.
    Requires the table to have a `PRIMARY KEY`; see [Initial Snapshot](#initial-snapshot).
    **Default**: `False`
- `snapshot_host`: read the initial snapshot from this host (typically a read replica)
    instead of `host`; see [Snapshotting From a Read Replica](#snapshotting-from-a-read-replica).
    **Default**: `None` (snapshot from `host`)
- `snapshot_batch_size`: rows per snapshot page.
    **Default**: `1000`
- `force_snapshot`: re-run the initial snapshot even if one has already completed,
    **and re-anchor the binlog position with it**, which makes it the supported recovery
    from a purged binlog position. Requires `initial_snapshot=True`; setting it without
    one is rejected at start-up. It is static configuration, so it re-snapshots on
    **every** restart until you turn it off.
    **Default**: `False`
- `commit_interval`: how often (seconds) to produce the buffered changes and commit the
    binlog position they cover.
    **Default**: `5.0`
- `max_buffer_size`: commit early once this many changes are buffered, which bounds
    memory while catching up after downtime.
    **Default**: `1000`
- `poll_interval`: how long (seconds) to idle when the binlog stream had nothing to
    read. Consecutive empty polls double this, up to 1 second (or `commit_interval` if
    that is shorter), and the first change resets it — so a quiet table costs a couple of
    MySQL connections per second instead of twenty, at the price of up to a second of
    extra latency on the first event after a quiet period.
    **Default**: `0.1`
- `retry_backoff_secs`: maximum backoff (seconds) between attempts to rebuild the binlog
    stream after a connection failure. After 5 consecutive failures, or 20 reconnects
    within 10 minutes, the source stops and lets the platform restart it.
    **Default**: `5.0`

    `commit_interval`, `poll_interval`, `retry_backoff_secs` and `shutdown_timeout` must
    all be greater than `0`; a zero or negative value is rejected at start-up rather than
    clamped.

- `tls_enabled`: require an encrypted connection to MySQL. See
    [Transport Security](#transport-security).
    **Default**: `True`
- `tls_ca`: path to a PEM CA bundle. Providing one turns server-certificate verification
    on.
    **Default**: `None`
- `tls_cert` / `tls_key`: paths to a PEM client certificate and its private key, for
    mutual TLS. `tls_key` without `tls_cert` is rejected.
    **Default**: `None`
- `tls_verify_cert`: verify the server certificate. `None` means "verify if `tls_ca` was
    given"; `True` without `tls_ca` is rejected.
    **Default**: `None`
- `tls_verify_identity`: also check that the certificate matches `host`. Requires
    verification to be on.
    **Default**: `False`
- `allow_minimal_row_metadata`: start even when the server cannot provide
    `binlog_row_metadata = FULL`. Read [Running Without FULL Row
    Metadata](#running-without-full-row-metadata) before setting it — it is only safe for
    a text-only table with no unsigned columns.
    **Default**: `False`
- `name`: the source's unique name, which determines the default topic name, the state
    store name and the derived `server_id`. Renaming a source resets its position.
    **Default**: `mysql_cdc_<database>_<table>`
- `shutdown_timeout`: time in seconds the application waits for the source to shut down
    gracefully.
    **Default**: `10`
- `on_client_connect_success` / `on_client_connect_failure`: optional callbacks invoked
    after the connection and server validation succeed or fail.


## MySQL Prerequisites

Supported server versions: **MySQL 5.7 – 8.4, with `binlog_row_metadata = FULL`
required on 8.0.1+ unless `allow_minimal_row_metadata` is set.**

That variable was added in MySQL 8.0.1 and its default is `MINIMAL`, so on a supported
8.x server this is a configuration step, not a version check — see the
`binlog_row_metadata` bullet below for what it does and how to set it.

**MySQL 5.7 cannot provide it at all**, so 5.7 requires
`allow_minimal_row_metadata=True`, and on that path the source is only safe for a table
with **no `ENUM`, no `SET`, no `UNSIGNED` integer and no column whose bytes are not
UTF-8** (`BINARY`/`VARBINARY`/`BLOB`, or a `latin1`/`latin2`/… text column holding a byte
above `0x7F`). See [Running Without FULL Row
Metadata](#running-without-full-row-metadata). MariaDB is in the same position as 5.7 and
is untested.

1. **MySQL configuration**: binary logging must be enabled, in `ROW` format:

    ```ini
    # Add to MySQL configuration file (my.cnf or my.ini)
    [mysqld]
    server-id = 1
    log_bin = /var/log/mysql/mysql-bin.log
    binlog_expire_logs_seconds = 864000
    max_binlog_size = 100M
    binlog-format = ROW
    binlog_row_metadata = FULL
    binlog_row_image = FULL
    ```

    - `server-id` here is the **server's own** id. It must differ from the id of every
      replication client that connects to it, including this source. The source's
      `server_id` defaults to a derived value of at least 1000, so it will not collide
      with the conventional `server-id = 1` above. Two `MySqlCdcSource` instances
      against the same server also need distinct `server_id` values, which happens
      automatically when their `name`, `database` or `table` differ.
    - `binlog-format = ROW` is **required**. The source fails at start-up with any other
      value, because a statement-based binlog carries no row images for it to read.
    - `binlog_row_image = FULL` is **recommended**. With `MINIMAL` the source logs a
      warning and keeps running: update and delete events then carry only the primary
      key rather than every column. That is a *partial but correct* event, which is why
      this setting is a warning while `binlog_row_metadata` below is a hard requirement —
      the latter produces events that are wrong rather than incomplete.
    - `binlog_row_metadata = FULL` is **required**, and is *not* the server default —
      MySQL ships `MINIMAL`. The source refuses to start without it. Fix it with:

        ```sql
        SET GLOBAL binlog_row_metadata = FULL;   -- global and dynamic: no restart
        FLUSH BINARY LOGS;                       -- leave MINIMAL-era events behind
        ```

        and add `binlog_row_metadata = FULL` to `my.cnf` so it survives a restart.

        What `MINIMAL` actually costs is not just column names. `pymysqlreplication`
        discards **all** optional table metadata unless the variable reads exactly
        `FULL`, so change events lose four things together:

        - column names (`UNKNOWN_COL0`, `UNKNOWN_COL1`, …),
        - `ENUM` and `SET` values — they arrive as `null`, on every event,
        - column character sets — a `BINARY`/`VARBINARY`/`BLOB` column containing any
          byte above `0x7F` then fails to decode and kills the source, which replays the
          same row and fails again on every restart,
        - integer signedness — `INT UNSIGNED 4294967295` arrives as `-1`.

        Only the column names have a client-side recovery, which is why the source
        refuses rather than warning. `FLUSH BINARY LOGS` matters because binlog files
        written *before* the change still parse as `MINIMAL`: a source resuming into one
        of them hits the same problems on a server that passes the start-up check, and
        reports it with a message naming both ways out.

        If your server cannot be given `FULL` — MySQL 5.7, or a managed instance you do
        not control — see [Running Without FULL Row
        Metadata](#running-without-full-row-metadata).
    - `binlog_expire_logs_seconds` must exceed the longest downtime you expect. If the
      binlog file holding the committed position has been purged, the stream fails fast
      with MySQL's "Could not find first log file name in binary log index".

2. **MySQL user permissions**: the user needs `REPLICATION SLAVE`, `REPLICATION CLIENT`
   and `SELECT` on the table:

    ```sql
    -- Create replication user
    CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'secure_password';

    -- Grant replication privileges for CDC
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';

    -- Required for every deployment, snapshot or not (see below)
    GRANT SELECT ON your_database.your_table TO 'cdc_user'@'%';

    FLUSH PRIVILEGES;
    ```

    `SELECT` is **not** snapshot-only. The source connects to `database` to validate the
    server and to read the table's metadata, and MySQL refuses the connection outright
    (`ERROR 1044: Access denied for user ... to database ...`) to an account holding only
    the two replication privileges. Grant it on the whole table: a column-level grant
    makes the table visible but returns a partial column list, which the source cannot
    use.


## Running Without FULL Row Metadata

`allow_minimal_row_metadata=True` lets the source start against a server whose
`binlog_row_metadata` is not `FULL` — a MySQL 5.7 server, which has no such variable, or
a stock 8.x server you cannot reconfigure. It exists because such a server is perfectly
usable for *some* tables, and refusing outright would leave those deployments with no
option at all.

**It is not a "degraded mode" in the usual sense: three of the four failures are silent
and produce wrong values rather than missing ones.** `pymysqlreplication` discards all
optional table metadata unless the variable reads exactly `FULL`, so on change events
(snapshot rows are unaffected, which is what makes it worse — one topic then carries two
different answers for the same column):

| column in your table | what a change event carries |
|---|---|
| `ENUM` | `null`, always |
| `SET` | the **empty string**, always — the decoder cannot tell an empty set from a missing dictionary |
| `INT UNSIGNED`, `BIGINT UNSIGNED`, … | decoded as **signed**: `4294967295` arrives as `-1`, `18446744073709551615` as `-1`, and `BIGINT UNSIGNED` above 2<sup>63</sup> arrives negative |
| `BINARY`, `VARBINARY`, `BLOB` whose bytes all happen to be valid UTF-8 | the **decoded text**, not base64 — so `0x00 'a' 'b' 'c'` arrives as `"\x00abc"` where the snapshot says `"AGFiYw=="` |
| `BINARY`, `VARBINARY`, `BLOB`, **or a non-UTF-8 text column** (`latin1`, …) holding a byte above `0x7F` | the decoder raises `UnicodeDecodeError`; the source stops with an error naming this parameter, and stops again on the same row after every restart. A `latin1` column counts: `0xFF` is a legal `latin1` character and invalid UTF-8 |
| everything else — signed integers, `DECIMAL`, UTF-8 text, `DATE`/`DATETIME`/`TIMESTAMP`, `JSON`, `BIT`, `FLOAT`/`DOUBLE` | correct, and identical to the snapshot representation |

That last row is measured, not assumed: the connector's rig runs the same
column-by-column comparison against a `FULL` server and against a `MINIMAL` server with
this flag on, and the rows above are exactly the differences it finds — the assertion is
set equality, so a column type that starts degrading without being listed here fails the
test rather than reaching a user.

So use it only when **all** of these hold:

- the table has no `ENUM` and no `SET` column,
- the table has no `UNSIGNED` integer column,
- every text column is UTF-8 (`utf8mb4`/`utf8mb3`/`ascii`), and there are no
  `BINARY`/`VARBINARY`/`BLOB` columns,
- and you accept that adding any such column later will break the stream, loudly in the
  fourth case and silently in the first three.

The source logs a `WARNING` naming the parameter and all four consequences on every start
and every reconnect. Prefer `SET GLOBAL binlog_row_metadata = FULL;` whenever the server
allows it — it is dynamic and needs no restart.


## Transport Security

The connection to MySQL is **encrypted by default** (`tls_enabled=True`), and the server
certificate is **not verified** unless you provide one to verify against. Those are two
separate switches on purpose:

| configuration | connection | server authenticated |
|---|---|---|
| defaults | encrypted, required | no |
| `tls_ca="/path/ca.pem"` | encrypted, required | yes |
| `tls_ca=...`, `tls_verify_identity=True` | encrypted, required | yes, and the hostname is checked |
| `tls_cert=...`, `tls_key=...` | encrypted, required, client certificate sent | per `tls_ca` |
| `tls_enabled=False` | plaintext | n/a |

Encryption is on by default because it works out of the box: MySQL 5.7.6+ and 8.x
auto-generate a self-signed server certificate at first start. Verification is off by
default because it cannot work out of the box — a self-signed certificate has no CA to
check it against — so turning it on is a deliberate step: point `tls_ca` at the PEM
bundle containing the CA that signed your server's certificate. `tls_verify_cert=True`
without `tls_ca`, `tls_verify_identity=True` without verification, `tls_key` without
`tls_cert`, and any `tls_*` setting alongside `tls_enabled=False` are all rejected at
start-up rather than silently reinterpreted.

The source logs one `INFO` line at start-up saying which of the rows above it got, e.g.
`TLS: required, server certificate NOT verified (no tls_ca)`.

`tls_enabled=False` is a real plaintext connection, not "try TLS and fall back". Use it
only on a trusted network — for example a MySQL that does not offer TLS at all, which
otherwise fails with `SSL is required but the server doesn't support it`.


## Initial Snapshot

With `initial_snapshot=True` the source reads the table's current contents before it
starts streaming, emitting each row as a `snapshot_insert` event.

- **A `PRIMARY KEY` is required.** The snapshot pages through the table with keyset
  (seek) pagination — `WHERE (pk...) > (last seen pk...) ORDER BY pk... LIMIT n` — which
  is index-driven and cannot skip rows when other transactions delete rows behind the
  cursor. Composite primary keys are supported. A table without a primary key fails at
  start-up with a message telling you to add one or to set `initial_snapshot=False`.
- **Pages are produced and flushed one at a time**, so a large table does not have to
  fit in memory.
- **Progress is checkpointed** after each page, so an interrupted snapshot resumes at
  the last committed key instead of replaying the whole table. Checkpointing needs the
  primary-key values to survive a JSON round-trip, so it is only done when every primary
  key column is an integer or a string. For any other primary-key type (binary,
  temporal, `DECIMAL`) the source logs that progress cannot be checkpointed, and an
  interrupted snapshot restarts from the beginning. Pagination itself is unaffected.
- **The binlog position is resolved and committed before the first row is read**, so
  every change made while the snapshot runs is still ahead of the stream. Rows changed
  during the snapshot are therefore emitted twice — once as `snapshot_insert` and again
  as an `insert`/`update`/`delete` — and nothing is missed.
- `force_snapshot=True` re-runs the snapshot even if one has already completed, and
  **discards the committed binlog position** before it starts, re-anchoring it from the
  server. That combination is what makes it the supported recovery from a purged binlog
  position: without the re-anchor the source would republish the table and then die on
  the same unreachable position. It requires `initial_snapshot=True`, and it is static
  configuration, so it re-snapshots on **every** restart until you turn it off.


## Snapshotting From a Read Replica

Set `snapshot_host` to read the snapshot from a read replica instead of the primary,
keeping the full-table scan off the primary. The binlog stream always runs against
`host`.

When `snapshot_host` differs from `host`, the source does **not** start the stream at
the primary's current position — that would drop every change the replica had not yet
applied. It instead reads the replica's *executed* primary coordinates
(`Relay_Source_Log_File`/`Exec_Source_Log_Pos`, or the legacy
`Relay_Master_Log_File`/`Exec_Master_Log_Pos`) and starts there. Rows read from the
replica afterwards reflect a state at or after those coordinates, so the overlap
produces duplicates and never a gap.

Requirements:

- The configured user needs `REPLICATION CLIENT` on the **snapshot host** as well, so
  the source can read its replication status. If the coordinates cannot be read — the
  host is not a replica, or the privilege is missing — the source fails at start-up
  rather than silently falling back to the primary's current position.
- The primary must still retain the binlog file covering the replica's executed
  position. If replica lag exceeds `binlog_expire_logs_seconds`, the stream fails with
  "Could not find first log file name in binary log index".


## Message Data Format/Schema

- Message `key` is `"<database>.<table>"` as a string, for every event. This keeps all
  changes to one table on one partition and in binlog order. **It is not the row
  identity** — deduplicate downstream using the primary-key columns inside
  `columnvalues`/`oldkeys` together with `kind`.
- Message `value` is JSON, in the shapes below.
- **A given MySQL value is encoded identically whether it arrived through the initial
  snapshot or through the binlog.** The two come from different libraries, which return
  different Python types for the same column, so the encoding is defined on the MySQL
  type:

| MySQL type | emitted as | example |
|---|---|---|
| `TINYINT` … `BIGINT`, `UNSIGNED` variants | JSON number | `4294967295` |
| `FLOAT` | JSON number, 6 significant digits | `1.1` |
| `DOUBLE` | JSON number | `1.1000000000000001` |
| `DECIMAL` | string, declared scale kept | `"0.00"` |
| `CHAR`, `VARCHAR`, `TEXT` | string | `"hello"` |
| `BINARY`, `VARBINARY`, `BLOB`, `GEOMETRY` | base64 string | `"AP/+gA=="` |
| `DATE`, `DATETIME` | ISO-8601 string | `"2024-03-01T10:20:30.123456"` |
| `TIME` | string | `"1:02:03"` |
| `TIMESTAMP` | ISO-8601 string, **in UTC** | `"2024-03-01T08:00:00"` |
| `ENUM` | string | `"shipped"` |
| `SET` | **sorted**, comma-joined string | `"a,b"` |
| `JSON` | **canonical JSON string** | `"{\"a\":2,\"b\":\"y\"}"` |
| `BIT(n)` | zero-padded bit string, width `n` | `"00000101"` |
| `NULL` | `null` | `null` |

  Four of those need explaining:

  - **`JSON` columns are emitted as JSON *strings*, not nested objects.** `columnvalues`
    is a flat array of scalars, and nesting one element would change the message schema
    for every consumer. Call `json.loads()` (or your language's equivalent) on the value.
    Keys are sorted and separators are minimal on both paths, so the string is stable.
  - **`SET` values are sorted**, not in the order the column was declared in: the binlog
    delivers an unordered set, and sorting is the only ordering both paths can produce.
    An empty `SET` is `""`; a `NULL` `SET` is `null`.
  - **`BIT(n)` is a bit string** of exactly `n` characters, rather than base64 (which is
    width-ambiguous) or a number (`BIT(64)` exceeds the safe integer range of most JSON
    consumers).
  - **`TIMESTAMP` is always UTC.** The source sets every one of its MySQL sessions to
    `time_zone = '+00:00'`, because the binlog decoder renders `TIMESTAMP` in UTC and
    cannot be told otherwise. `DATETIME` is unaffected — MySQL never converts it.

  A column type with no rule above is emitted as its `str()` and the source logs a
  warning naming the type, once per process.

### Snapshot Insert Event
```json
{
  "kind": "snapshot_insert",
  "schema": "database_name",
  "table": "table_name",
  "columnnames": ["col1", "col2"],
  "columnvalues": ["value1", "value2"],
  "oldkeys": {}
}
```

### INSERT Event
```json
{
  "kind": "insert",
  "schema": "database_name",
  "table": "table_name",
  "columnnames": ["col1", "col2"],
  "columnvalues": ["value1", "value2"],
  "oldkeys": {}
}
```

### UPDATE Event
```json
{
  "kind": "update",
  "schema": "database_name",
  "table": "table_name",
  "columnnames": ["col1", "col2"],
  "columnvalues": ["new_value1", "new_value2"],
  "oldkeys": {
    "keynames": ["col1", "col2"],
    "keyvalues": ["old_value1", "old_value2"]
  }
}
```

### DELETE Event
```json
{
  "kind": "delete",
  "schema": "database_name",
  "table": "table_name",
  "columnnames": [],
  "columnvalues": [],
  "oldkeys": {
    "keynames": ["col1", "col2"],
    "keyvalues": ["deleted_value1", "deleted_value2"]
  }
}
```


## Processing/Delivery Guarantees

The MySQL CDC Source offers **at-least-once** guarantees. A binlog position is only
committed after the changes it covers have been produced *and* the producer flush has
returned, so no change is ever lost once a position is committed.

Changes can be delivered more than once in three situations:

1. A crash (or a failed flush) between the data flush and the position commit replays
   that batch on the next start.
2. Rows changed during an initial snapshot appear both as a `snapshot_insert` and as a
   later `insert`/`update`/`delete`.
3. `force_snapshot=True` replays the whole table.

Consumers must therefore be idempotent, deduplicating on the primary-key columns in
`columnvalues`/`oldkeys` together with `kind`.


## State & Recovery

The source stores its progress in a Quix Streams state store backed by a changelog
topic. The store is rebuilt from that changelog on every start, so it survives an
ephemeral container filesystem and there are no local state files and no state
directory to configure.

Three keys are used:

| Key | Value |
|---|---|
| `binlog_position_<database>_<table>` | `{"log_file": ..., "log_pos": ..., "committed_at": ...}` |
| `snapshot_completed_<database>_<table>` | `{"completed_at": ..., "rows": ...}`, absent until the snapshot finishes |
| `snapshot_progress_<database>_<table>` | `{"last_key": [...], "rows": ..., "updated_at": ...}`, present only while a snapshot is in progress |

The store (and its changelog topic) is named after the source, so renaming a source —
or pointing it at a different `database`/`table` — starts from a fresh position.


## Topic

The source's default topic name is `mysql_cdc_<database>_<table>`. When passed to
`Application.dataframe(source=...)` without a custom topic, the Kafka topic name is
prefixed as `source__mysql_cdc_<database>_<table>`.


## Testing Locally

You can test your application using a locally emulated MySQL host via Docker
with all correct settings by:

1. Execute the following in terminal (just copy+paste) to run MySQL with the
correct settings and set of test credentials:

```bash
TMPDIR=$(mktemp -d $HOME/mysql-cdc.XXXXXX)

cat > "$TMPDIR/custom-mysql.cnf" <<EOF
[mysqld]
server-id = 1
log_bin = /var/lib/mysql/mysql-bin.log
binlog_expire_logs_seconds = 864000
max_binlog_size = 100M
binlog-format = ROW
binlog_row_metadata = FULL
binlog_row_image = FULL
EOF

cat > "$TMPDIR/init-user.sql" <<EOF
CREATE DATABASE IF NOT EXISTS test_database;
USE test_database;
CREATE TABLE IF NOT EXISTS test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON test_database.test_table TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
EOF

docker run --rm -d \
  --name mysql-cdc \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root_password \
  -e MYSQL_DATABASE=test_database \
  -v "$TMPDIR/custom-mysql.cnf":/etc/mysql/conf.d/custom.cnf:ro \
  -v "$TMPDIR/init-user.sql":/docker-entrypoint-initdb.d/init-user.sql:ro \
  mysql:8.0

echo "sleeping then deleting temp dir"
sleep 30
rm -rf $TMPDIR
```

    The `binlog_row_metadata = FULL` line in that config is **required**, not
    decorative: without it the source refuses to start. See
    [MySQL Prerequisites](#mysql-prerequisites).

2. Connect using the [How To Use](#how-to-use) snippet above, which already points at
this container's host, credentials, database and table.


## Troubleshooting

- **"Binary logging is disabled on ..."** — enable `log_bin` in the MySQL configuration
  and restart the server.
- **"binlog_format is 'STATEMENT' ... but CDC requires 'ROW'"** — set
  `binlog_format=ROW`. Row events do not exist in any other format.
- **"Access denied" / "Could not read the binary log position"** — grant
  `REPLICATION SLAVE, REPLICATION CLIENT ON *.*` to the configured user. `Access denied
  for user ... to database ...` (error 1044) is the other half: the user also needs
  `SELECT` on the table, whether or not the initial snapshot is enabled.
- **"binlog_row_metadata is 'MINIMAL' ... but this source requires 'FULL'"** — the
  server is running MySQL's default. Run `SET GLOBAL binlog_row_metadata = FULL;` and
  `FLUSH BINARY LOGS;`, and add the setting to `my.cnf`. See
  [MySQL Prerequisites](#mysql-prerequisites) for what `MINIMAL` costs.
- **"... has no binlog_row_metadata variable"** — the server is MySQL 5.7 or MariaDB,
  which cannot supply the metadata this source needs. Either move to MySQL 8.0.1+ with
  `binlog_row_metadata = FULL`, or set `allow_minimal_row_metadata=True` if the table
  qualifies; see [Running Without FULL Row Metadata](#running-without-full-row-metadata).
- **"Could not decode a binlog event ..."** — the event carries no column character
  sets, so a `BINARY`/`VARBINARY`/`BLOB` column, or a non-UTF-8 text column holding a byte
  above `0x7F`, could not be decoded. Either the source is reading binlog files written
  *before* `binlog_row_metadata` was set to `FULL` (those still parse as `MINIMAL` even
  though the server now reports `FULL`), or it is running with
  `allow_minimal_row_metadata=True` against a table that is not text-only. Run
  `SET GLOBAL binlog_row_metadata = FULL;` and `FLUSH BINARY LOGS` and let the source
  resume into the new file, or restart it with `initial_snapshot=True` and
  `force_snapshot=True` to re-snapshot the table and re-anchor the position past the old
  events. The source never skips the row, and never uses `ignore_decode_errors` — both
  would be silent data loss.
- **"MySQL evicted the binlog stream ... another client announced server_id=N"** — two
  replication clients share one id and are evicting each other; the source exits after
  the third eviction. `MySqlCdcSource` must run with exactly **one replica**, because the
  default `server_id` is derived from `name`/`database`/`table` and is therefore identical
  across replicas of one deployment. A second deployment against the same server needs a
  distinct `name` or an explicit `server_id`.
- **"The binlog stream ... has been rebuilt N times in the last 600s"** — individual
  reconnects kept succeeding, so the per-attempt limit never tripped, but the source is
  reconnecting rather than streaming. Check the server's error log, the network, and
  whether another client is using the same `server_id`.
- **"MySQL no longer holds the binlog position committed for ..."** — the committed
  position has been purged from the server's binlog, and there is no gap-free recovery:
  the changes in between are gone from the server. Restart with `initial_snapshot=True`
  **and** `force_snapshot=True`, which republishes every row and re-anchors the position,
  then turn `force_snapshot` off again. Raise `binlog_expire_logs_seconds` so it exceeds
  the longest downtime you expect.
- **"SSL is required but the server doesn't support it"** (error 2026) — the server does
  not offer TLS at all. Configure TLS on the server, or set `tls_enabled=False` if the
  network is trusted.
- **"Connections using insecure transport are prohibited"** (error 3159) — the server has
  `require_secure_transport = ON` and the source was configured with `tls_enabled=False`.
  Remove that setting; TLS is the default.
- **"Table ... has no PRIMARY KEY"** — the initial snapshot needs one to paginate. Add a
  primary key, or set `initial_snapshot=False` to stream binlog changes only.
- **"Could not read an executed replication position from snapshot host ..."** — the
  `snapshot_host` is not a replica of `host`, or the user lacks `REPLICATION CLIENT`
  there. Point `snapshot_host` at the primary, or grant the privilege.
- **Snapshot host unreachable** — the source connects to `snapshot_host` during
  start-up validation, so a bad address fails immediately rather than mid-snapshot.
