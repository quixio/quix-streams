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
    every replica and CDC client connected to the same server.
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
- `force_snapshot`: re-run the initial snapshot even if one has already completed.
    **Default**: `False`
- `commit_interval`: how often (seconds) to produce the buffered changes and commit the
    binlog position they cover.
    **Default**: `5.0`
- `max_buffer_size`: commit early once this many changes are buffered, which bounds
    memory while catching up after downtime.
    **Default**: `1000`
- `poll_interval`: how long (seconds) to idle when the binlog stream had nothing to read.
    **Default**: `0.1`
- `retry_backoff_secs`: maximum backoff (seconds) between attempts to rebuild the binlog
    stream after a connection failure. After 5 consecutive failures the source stops and
    lets the platform restart it.
    **Default**: `5.0`
- `name`: the source's unique name, which determines the default topic name, the state
    store name and the derived `server_id`. Renaming a source resets its position.
    **Default**: `mysql_cdc_<database>_<table>`
- `shutdown_timeout`: time in seconds the application waits for the source to shut down
    gracefully.
    **Default**: `10`
- `on_client_connect_success` / `on_client_connect_failure`: optional callbacks invoked
    after the connection and server validation succeed or fail.


## MySQL Prerequisites

Supported server versions: MySQL 5.7 through 8.4.

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
      key rather than every column.
    - `binlog_row_metadata = FULL` is **recommended**, and is *not* the server default —
      MySQL 8.x ships `MINIMAL` and MySQL 5.7 has no such variable at all. Only `FULL`
      puts column names into the binlog itself. Without it the source falls back to
      reading them from `INFORMATION_SCHEMA` — which needs the `SELECT` grant below on
      the whole table — and caches them for as long as it runs, so a column renamed
      under a running source keeps its old name in change events until the source is
      restarted. The source logs a warning and keeps running. If neither source of names
      is available, change events name their columns `UNKNOWN_COL0`, `UNKNOWN_COL1`, ….
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
    the two replication privileges. It is also what lets the source read column names
    from `INFORMATION_SCHEMA` when `binlog_row_metadata` is not `FULL`. Grant it on the
    whole table: a column-level grant makes the table visible but returns a partial
    column list, which the source cannot use and which degrades change events to
    `UNKNOWN_COL0`, `UNKNOWN_COL1`, ….


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
- `force_snapshot=True` re-runs the snapshot even if one has already completed. It is
  static configuration, so it re-snapshots on **every** restart until you turn it off.


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
- Values that JSON cannot represent are encoded on the way out: `bytes`/`bytearray`
  become base64 strings, anything date- or time-like becomes ISO-8601, and everything
  else (`DECIMAL`, geometry, ...) becomes its `str()` representation.

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
- **Change events name their columns `UNKNOWN_COL0`, `UNKNOWN_COL1`, …** — the binlog
  carried no column names and they could not be read from `INFORMATION_SCHEMA` either.
  Grant `SELECT` on the whole table (not on individual columns), and set
  `binlog_row_metadata=FULL` so the names come from the binlog in the first place.
- **"A slave with the same server_id is already connected"** — another replica or CDC
  client is using the same id. Set a distinct `server_id`, or give the two sources
  different `name`/`database`/`table` values so their derived ids differ.
- **"Could not find first log file name in binary log index"** — the committed position
  has been purged from the server's binlog. Either raise
  `binlog_expire_logs_seconds`, or accept the gap and re-snapshot with
  `force_snapshot=True`.
- **"Table ... has no PRIMARY KEY"** — the initial snapshot needs one to paginate. Add a
  primary key, or set `initial_snapshot=False` to stream binlog changes only.
- **"Could not read an executed replication position from snapshot host ..."** — the
  `snapshot_host` is not a replica of `host`, or the user lacks `REPLICATION CLIENT`
  there. Point `snapshot_host` at the primary, or grant the privilege.
- **Snapshot host unreachable** — the source connects to `snapshot_host` during
  start-up validation, so a bad address fails immediately rather than mid-snapshot.
