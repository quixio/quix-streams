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
    **and re-anchor the binlog position with it**. A forced snapshot that is interrupted
    is continued on the next start rather than restarted. Requires
    `initial_snapshot=True`; setting it without one is rejected at start-up. It is static
    configuration, so it re-snapshots on **every** restart until you turn it off.
    **Default**: `False`
- `commit_interval`: how often (seconds) to produce the buffered changes and commit the
    binlog position they cover.
    **Default**: `5.0`
- `max_buffer_size`: commit early once this many changes are buffered, which bounds
    memory while catching up after downtime. A binlog event carrying more rows than the
    room that is left is split across reads, so the bound holds for a bulk
    `INSERT ... SELECT` too.
    **Default**: `1000`
- `poll_interval`: how long (seconds) to idle when the binlog stream had nothing to
    read. Consecutive empty polls double this, up to `commit_interval`, and the first
    change resets it. A drained poll that is holding buffered changes waits for the
    commit those changes are due at instead. Measured against a 20 rows/s trickle, that
    is 1.1 new MySQL connections per second rather than 29.
    **Default**: `0.1`
- `retry_backoff_secs`: maximum backoff (seconds) between attempts to rebuild the binlog
    stream after a connection failure. A rebuild that fails — usually because the server
    is still down — counts as one of those attempts and the loop tries again. After 5
    consecutive failures, or 20 reconnects inside a sliding 10-minute window, the source
    stops and lets the platform restart it from its last committed position.
    **Default**: `5.0`

    `commit_interval`, `poll_interval`, `retry_backoff_secs` and `shutdown_timeout` must
    all be greater than `0`; a zero or negative value is rejected at start-up rather than
    clamped.

- `tls_enabled`: require an encrypted connection to MySQL. See
    [Transport Security](#transport-security).
    **Default**: `True`
- `tls_ca`: path to a PEM CA bundle. Providing one turns verification on — the server's
    certificate chain **and** its hostname are then checked.
    **Default**: `None`
- `name`: the source's unique name, which determines the default topic name, the state
    store name and the derived `server_id`. Renaming a source resets its position.
    **Default**: `mysql_cdc_<database>_<table>`
- `shutdown_timeout`: time in seconds the application waits for the source to shut down
    gracefully.
    **Default**: `10`
- `on_client_connect_success` / `on_client_connect_failure`: optional callbacks invoked
    after the connection and server validation succeed or fail.


## MySQL Prerequisites

Supported server versions: **MySQL 8.0, 8.4 and 9.x.** MySQL 5.7 and MariaDB are not
supported: neither has `binlog_row_metadata`, and without it the binlog carries no column
names, no `ENUM`/`SET` values, no character sets and no integer signedness — none of which
the client can reconstruct.

One of the settings below is normally configured **for** you. `binlog_row_metadata`
ships as `MINIMAL` on MySQL 8.0.46, and the source raises it to `FULL` itself at
start-up, logging one line when it does. `binlog_row_image` already ships `FULL`, so the
source raises it only where someone has lowered it. Both are global and dynamic.
The source reads them again every time it rebuilds its binlog stream, so a server
restart that puts `my.cnf` back in charge is repaired on the next reconnect instead of
stopping the source; a server that still has them costs two `SHOW GLOBAL VARIABLES` and
no rotation. `log_bin` and `binlog_format` need a server restart, so those the source
can only refuse.

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
    - `binlog_row_metadata = FULL` and `binlog_row_image = FULL` are **required, and
      the source sets them for you**. On a stock 8.0.46 only `binlog_row_metadata` needs
      raising — `binlog_row_image` already ships `FULL` — so at start-up the source runs

        ```sql
        SET GLOBAL binlog_row_metadata = FULL;   -- global and dynamic: no restart
        SET GLOBAL binlog_row_image = FULL;      -- only if it has been lowered
        FLUSH BINARY LOGS;                       -- leave the older events behind
        ```

        for whichever of the two is not already `FULL`, and logs one `INFO` line saying
        so. Put them in `my.cnf` as well so the server keeps them across its own
        restarts; the source then finds them set and changes nothing.

        Setting a global needs `SYSTEM_VARIABLES_ADMIN` (see the grants below). Without
        it the source stops at start-up, because neither loss can be repaired on the
        client:

        - `binlog_row_metadata = MINIMAL` discards **all** optional table metadata, not
          some of it: column names become `UNKNOWN_COL0…n`, every `ENUM` arrives as
          `null` and every `SET` as `""`, `INT UNSIGNED 4294967295` decodes as `-1`, and
          a `BINARY`/`VARBINARY`/`BLOB` or non-UTF-8 text column fails to decode.
        - `binlog_row_image = MINIMAL` leaves every column a statement did not touch out
          of the event entirely, so `UPDATE t SET a=1` carries `a` and nothing else.

        On a **managed** MySQL — RDS, Aurora, Cloud SQL — `GRANT SYSTEM_VARIABLES_ADMIN`
        is refused even to the master account. Set `binlog_row_metadata` to `FULL` in the
        instance's parameter group, database flags or equivalent console setting instead,
        and apply it; the source then finds it set. The error message says both.

        Rotating the log (`FLUSH BINARY LOGS`) needs `RELOAD`. Without it the source logs
        that it skipped the rotation and carries on: events written from that moment are
        correct either way, and only a source resuming into a position written *before*
        the change reads the older ones. Such a replay is **refused, not published**: the
        source checks each event's own table map and stops when it finds one written
        under `MINIMAL` metadata, because an `ENUM` would arrive as `null`, a `SET` as
        `""` and an `INT UNSIGNED 4294967295` as `-1`, none of them distinguishable from
        the real value.

        One caveat that no client can work around: `binlog_row_image` is also a **session**
        variable, and a session takes its copy when it connects. An application connection
        that was already open when the source raised the global keeps writing the old row
        image until it reconnects — the source stops with an error on the first such
        event rather than publishing a change with columns missing, so reconnect those
        writers. Putting both settings in `my.cnf` avoids this entirely, which is why
        they are in the block above.
    - `binlog_expire_logs_seconds` must exceed the longest downtime you expect, **and
      the time the initial snapshot takes**: the position is anchored before the first
      row is read, so a snapshot that runs longer than the retention ends on a position
      the server has purged. The source checks while the snapshot runs — see
      [Initial Snapshot](#initial-snapshot).

2. **MySQL user permissions**: the user needs `REPLICATION SLAVE`, `REPLICATION CLIENT`
   and `SELECT` on the **whole** table, plus `SYSTEM_VARIABLES_ADMIN` unless the two
   row-image settings above are already `FULL`:

    ```sql
    -- Create replication user
    CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'secure_password';

    -- Grant replication privileges for CDC
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';

    -- Required for every deployment, snapshot or not (see below).
    -- On the whole table: a column-level grant is rejected at start-up.
    GRANT SELECT ON your_database.your_table TO 'cdc_user'@'%';

    -- Lets the source set binlog_row_metadata/binlog_row_image itself.
    -- Not needed if my.cnf already gives both FULL, and refused on RDS/Aurora/Cloud SQL
    -- where the parameter group is the way to set them.
    GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'cdc_user'@'%';

    FLUSH PRIVILEGES;
    ```

    `SELECT` is **not** snapshot-only. The source connects to `database` to validate the
    server and to read the table's metadata, and MySQL refuses the connection outright
    (`ERROR 1044: Access denied for user ... to database ...`) to an account holding only
    the two replication privileges.

    It must be granted on the **whole table**. A column-level grant such as
    `GRANT SELECT (id, name)` makes the table visible but filters *both*
    `information_schema.COLUMNS` and `SHOW COLUMNS` down to the granted columns, so
    nothing in the catalogue reveals the missing ones. The source therefore runs
    `SELECT * FROM <table> LIMIT 0` at start-up, which MySQL refuses with
    `ERROR 1142` unless the account can read every column, and stops with a message
    naming the `GRANT` to run. Granting each column individually passes, because the
    column list is then complete.


## Transport Security

The connection to MySQL is **encrypted by default** (`tls_enabled=True`), and the server
is **not authenticated** unless you give the source something to authenticate it with:

| configuration | connection | server authenticated |
|---|---|---|
| defaults | encrypted, required | no |
| `tls_ca="/path/ca.pem"` | encrypted, required | yes — chain **and** hostname |
| `tls_enabled=False` | plaintext | n/a |

Encryption is on by default because it works out of the box: MySQL auto-generates a
self-signed server certificate at first start. Verification is off by default because it
cannot work out of the box — a self-signed certificate has no CA to check it against — so
turning it on is one deliberate step: point `tls_ca` at the PEM bundle containing the CA
that signed your server's certificate. There is no separate switch for hostname checking;
a CA turns on the whole check. Client-certificate (mutual TLS) authentication is not
supported.

The source logs one `INFO` line at start-up saying which of the rows above it got, e.g.
`TLS: required, server certificate NOT verified (no tls_ca)`.

`tls_enabled=False` is a real plaintext connection, not "try TLS and fall back". Use it
only on a trusted network — for example a MySQL that does not offer TLS at all, which
otherwise fails with `SSL is required but the server doesn't support it`. Passing
`tls_ca` alongside it is rejected at start-up rather than ignored.


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
  the last committed key instead of replaying the whole table. The state store holds
  JSON, so each key value is stored with the type it came back as and rebuilt on resume:
  `BINARY`/`VARBINARY`/`BLOB` as base64, `DECIMAL` as its exact decimal string,
  `DATE`/`DATETIME`/`TIMESTAMP` as ISO-8601 and `TIME` as whole microseconds. A key of
  some other type still cannot be stored; the source logs that once and an interrupted
  snapshot then restarts from the beginning. Pagination itself is unaffected.
- **Every column is named in the query**, read from `information_schema.COLUMNS` in
  ordinal order, rather than selected with `SELECT *`. `SELECT *` omits `INVISIBLE`
  columns while the binlog row images carry them, which includes the `my_row_id` that
  MySQL adds to a table with no primary key when
  `sql_generate_invisible_primary_key = ON`. Both paths therefore emit the same
  `columnnames` for the same table — provided the account can read every column, which
  start-up checks; see [MySQL Prerequisites](#mysql-prerequisites).
- **The binlog position is resolved and committed before the first row is read**, so
  every change made while the snapshot runs is still ahead of the stream. Rows changed
  during the snapshot are therefore emitted twice — once as `snapshot_insert` and again
  as an `insert`/`update`/`delete` — and nothing is missed.
- **That anchored position is watched while the snapshot runs**, once a minute at most.
  If the server keeps binary logs for less time than the snapshot needs, the anchor is
  purged before the stream reaches it. The source warns as soon as the rows-per-second
  it is achieving projects past `binlog_expire_logs_seconds`, and stops with an error
  once the anchored file is actually gone — naming the elapsed time to raise the setting
  above. It discards the stored position and progress at that point, so the next start
  re-anchors and re-reads the table without needing `force_snapshot` or a state wipe.
- **Progress is checkpointed with the primary-key columns it paginated on.** If the
  table's primary key changes between runs, the stored key cannot be compared against
  the new one, so it is discarded and the snapshot restarts instead of failing on every
  restart with a driver error.
- `force_snapshot=True` re-runs the snapshot even if one has already completed, and
  **discards the committed binlog position** before it starts, re-anchoring it from the
  server. Because it is static configuration it applies on **every** restart until you
  turn it off — so if a forced snapshot is interrupted, the next start *continues* it
  from its stored progress at the position it originally anchored, instead of
  re-anchoring and starting again. Re-anchoring there would move the position past the
  changes made to the pages already produced, and those changes would never be
  published. It requires `initial_snapshot=True`.


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

- Message `key` is `"<database>.<table>"` as a **string**, for every event: the source's
  default topic sets a string key serializer and deserializer, so `key.split(".")` works
  on the consuming side without decoding bytes first. This keeps all changes to one table
  on one partition and in binlog order. **It is not the row identity** — deduplicate
  downstream using the primary-key columns inside `columnvalues`/`oldkeys` together with
  `kind`.
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
| `TIME` | MySQL `TIME` string, `[-]HH:MM:SS[.ffffff]` | `"26:03:04"`, `"-838:59:59"` |
| `TIMESTAMP` | ISO-8601 string, **in UTC** | `"2024-03-01T08:00:00"` |
| `ENUM` | string | `"shipped"` |
| `SET` | **sorted**, comma-joined string | `"a,b"` |
| `JSON` | **canonical JSON string** | `"{\"a\":2,\"b\":\"y\"}"` |
| `BIT(n)` | zero-padded bit string, width `n` | `"00000101"` |
| `NULL` | `null` | `null` |

  **`null` always means SQL NULL.** A column MySQL did not send is not a value, and
  the source will not guess one: an event whose row image is missing a column — because
  the writer's session still has `binlog_row_image = MINIMAL`, or because a `JSON`
  column arrived as a partial update — stops the source with an error naming the
  columns and the variable. `columnnames` is therefore always the table's full column
  list, `INVISIBLE` columns included.

  Six of those need explaining:

  - **`JSON` columns are emitted as JSON *strings*, not nested objects.** `columnvalues`
    is a flat array of scalars, and nesting one element would change the message schema
    for every consumer. Call `json.loads()` (or your language's equivalent) on the value.
    Keys are sorted and separators are minimal on both paths, so the string is stable.
  - **`FLOAT` carries six significant digits**, which is what MySQL's text protocol
    prints for a 4-byte float on 8.0, 8.4 and 9.x, and so what a `SELECT` — and with it
    the initial snapshot — returns. The binlog carries the stored 4-byte value itself
    (`0.3333333432674408` for a column holding a third), so it is rendered the same way
    and the two paths ship one value. `DOUBLE` needs no rule: MySQL prints the shortest
    representation that reads back as the same number, and so does Python. The two paths
    can still disagree on the deprecated `FLOAT(M,D)` when the value needs more than six
    significant digits — `12345.6789` through the snapshot against `12345.7` through the
    binlog — because the declared scale lives in the table definition, which the binlog
    does not carry.
  - **`SET` values are sorted**, not in the order the column was declared in: the binlog
    delivers an unordered set, and sorting is the only ordering both paths can produce.
    An empty `SET` is `""`; a `NULL` `SET` is `null`.
  - **`BIT(n)` is a bit string** of exactly `n` characters, rather than base64 (which is
    width-ambiguous) or a number (`BIT(64)` exceeds the safe integer range of most JSON
    consumers).
  - **`TIMESTAMP` is always UTC.** The source sets every one of its MySQL sessions to
    `time_zone = '+00:00'`, because the binlog decoder renders `TIMESTAMP` in UTC and
    cannot be told otherwise. `DATETIME` is unaffected — MySQL never converts it.
  - **`TIME` is a MySQL `TIME` string, not a duration.** MySQL's range is `-838:59:59`
    to `838:59:59`, so the hour field counts hours and can pass 24; the value is what
    `CAST(col AS CHAR)` prints, and fractional seconds appear only when non-zero.

  A column type with no rule above is emitted as its `str()` and the source logs a
  warning naming the type, once per table and column.

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
| `snapshot_progress_<database>_<table>` | `{"last_key": [...], "key_types": [...], "pk_columns": [...], "rows": ..., "updated_at": ...}`, present only while a snapshot is in progress |

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

    The two `binlog_row_*` lines are what the source would otherwise set itself on
    first start (`root` can). Having them in the config means the server keeps them
    across restarts. See [MySQL Prerequisites](#mysql-prerequisites).

2. Connect using the [How To Use](#how-to-use) snippet above, which already points at
this container's host, credentials, database and table.


## Troubleshooting

- **"Binary logging is disabled on ..."** — enable `log_bin` in the MySQL configuration
  and restart the server.
- **"binlog_format is 'STATEMENT' ... but CDC requires 'ROW'"** — set
  `binlog_format=ROW`. Row events do not exist in any other format. A server that has no
  `binlog_format` variable at all can only write row images, and the source logs that
  and carries on.
- **"Access denied" / "Could not read the binary log position"** — grant
  `REPLICATION SLAVE, REPLICATION CLIENT ON *.*` to the configured user. `Access denied
  for user ... to database ...` (error 1044) is the other half: the user also needs
  `SELECT` on the table, whether or not the initial snapshot is enabled.
- **"This source needs binlog_row_metadata = FULL ... may not set it"** — run the
  `GRANT SYSTEM_VARIABLES_ADMIN` the message quotes, or set the named variable to `FULL`
  in `my.cnf` yourself and restart the server. On RDS, Aurora or Cloud SQL that `GRANT`
  is refused even to the master account: set it in the instance's parameter group or
  database flags and apply it.
- **"The account this source connects with may not read every column of ..."** — the
  account holds a column-level `SELECT` grant. `information_schema.COLUMNS` is filtered
  by the same privileges, so the snapshot would silently read only the granted columns
  while the binlog carries all of them. Run the `GRANT SELECT ON <db>.<table>` the
  message quotes.
- **"MySQL wrote an event ... while binlog_row_metadata was below FULL"** — the source
  is replaying events written before it raised `binlog_row_metadata`. Those carry no
  `ENUM`/`SET` dictionaries and no integer signedness, none of which can be recovered on
  the client, so the source refuses the event rather than shipping `null`, `""` and
  negative numbers. Put `binlog_row_metadata = FULL` in `my.cnf`, then restart with
  `initial_snapshot=True` and `force_snapshot=True` to re-read the table and re-anchor
  past them.
- **"The binlog stream holds no table metadata for ..."** — a row event arrived whose
  table is not in the stream's table map, so its columns, character sets and `ENUM`/`SET`
  values are all unknown. The source stops at its last committed position rather than
  guess; starting it again reads the table map from the stream afresh and replays the
  event.
- **"Unknown system variable 'binlog_row_metadata'"** — the server is MySQL 5.7 or
  MariaDB. Neither is supported; move to MySQL 8.0 or later.
- **"Could not decode a binlog event ..."** — the source is reading events written before
  it set `binlog_row_metadata = FULL`; those carry no column character sets, so a
  `BINARY`/`VARBINARY`/`BLOB` or non-UTF-8 text column cannot be decoded. Let the source
  resume past them, or restart it with `initial_snapshot=True` and `force_snapshot=True`
  to re-snapshot the table and re-anchor the position past them. The source never skips
  the row — that would be silent data loss.
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
  the changes in between are gone from the server. With `initial_snapshot=True` the
  source discards its own stored position and snapshot progress before it stops, so
  simply starting it again re-reads the table and re-anchors — no configuration change
  and no `force_snapshot`. With `initial_snapshot=False` the position is **kept**,
  because there is nothing to re-read and dropping it would skip the gap silently: add a
  primary key and restart with `initial_snapshot=True` and `force_snapshot=True`, or
  accept the gap and give the source a different `name`, which starts it from the
  server's current position with a state store of its own. Raise
  `binlog_expire_logs_seconds` so it exceeds the longest downtime you expect.
- **"MySQL wrote a partial row image for ..."** — a writer that connected before the
  source raised `binlog_row_image` is still writing `MINIMAL` (or `NOBLOB`) images, whose
  events are missing columns. Set `binlog_row_image = FULL` in `my.cnf`, reconnect those
  writers, then restart the source with `initial_snapshot=True` and `force_snapshot=True`
  to re-read the table and re-anchor past the truncated events.
- **"MySQL wrote a partial JSON update for ..."** — the server has
  `binlog_row_value_options = PARTIAL_JSON`, so a `JSON` column arrives as a diff against
  a value the source does not hold. Set `binlog_row_value_options = ''`, reconnect the
  writers, and re-anchor as above.
- **"MySQL no longer holds ... the position the initial snapshot anchored"** — the
  snapshot ran for longer than the server keeps its binary logs. Raise
  `binlog_expire_logs_seconds` above the elapsed time the message names and start the
  source again; it has already discarded its stored position and progress, so it
  re-anchors and re-reads the table by itself.
- **"SSL is required but the server doesn't support it"** (error 2026) — the server does
  not offer TLS at all. Configure TLS on the server, or set `tls_enabled=False` if the
  network is trusted.
- **A hostname or certificate verification error after setting `tls_ca`** — a CA turns on
  both the chain check and the hostname check, so `host` must match the server
  certificate's subject or SAN. Point `host` at the name the certificate was issued for.
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
