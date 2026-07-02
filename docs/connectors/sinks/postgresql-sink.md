# PostgreSQL Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

PostgreSQL is a powerful, open-source object-relational database system.

Quix Streams provides a sink to write processed data to PostgreSQL.

## How To Install

To use the PostgreSQL sink, you need to install the required dependencies:

```bash
pip install quixstreams[postgresql]
```

## How To Use

To sink data to PostgreSQL, you need to create an instance of PostgreSQLSink and pass it to the StreamingDataFrame.sink() method:

```python
from quixstreams import Application
from quixstreams.sinks.community.postgresql import PostgreSQLSink

app = Application(broker_address="localhost:9092")
topic = app.topic("numbers-topic")

# Initialize PostgreSQLSink
postgres_sink = PostgreSQLSink(
    host="localhost",
    port=5432,
    dbname="mydatabase",
    user="myuser",
    password="mypassword",
    table_name="numbers",
    schema_auto_update=True
)

sdf = app.dataframe(topic)
# Do some processing here ...
# Sink data to PostgreSQL
sdf.sink(postgres_sink)

if __name__ == '__main__':
    app.run()
```

To drop duplicate or replayed messages at the database rather than raising an error, set `on_conflict_do_nothing=True` together with `primary_key_columns`:

```python
postgres_sink = PostgreSQLSink(
    host="localhost",
    port=5432,
    dbname="mydatabase",
    user="myuser",
    password="mypassword",
    table_name="numbers",
    primary_key_columns="event_id",   # column (or columns) with a unique/primary-key constraint
    on_conflict_do_nothing=True,      # duplicate rows are silently ignored
)
```

## How It Works

PostgreSQLSink is a batching sink.
It batches processed records in memory per topic partition and writes them to the PostgreSQL database when a checkpoint has been committed.

Under the hood, it dynamically adjusts the schema by adding new columns if schema_auto_update is enabled. Processed records are written as rows in the specified table.

What Data Can Be Sent to PostgreSQL?

PostgreSQLSink can accept only dictionary values.

If the record values are not dictionaries, you need to convert them to dictionaries using StreamingDataFrame.apply() before sinking.

- Key Column: When `include_metadata=True` (the default), the record key is inserted into a column named `__key`. If the record key is `None`, the `__key` column is not written for that row. When `include_metadata=False`, the `__key` column is omitted entirely.
- Timestamp Column: When `include_metadata=True` (the default), the record timestamp is inserted into a column named `timestamp`, with values stored in PostgreSQL’s `TIMESTAMP` format. When `include_metadata=False`, the `timestamp` column is omitted entirely.
- Other Columns: Additional fields in the dictionary will be mapped to table columns. New columns are automatically added to the schema if `schema_auto_update=True`.

### Delivery Guarantees

PostgreSQLSink provides at-least-once guarantees, meaning that the same records may be written multiple times in case of errors during processing.


## Configuration

PostgreSQLSink accepts the following configuration parameters:

## Required

- `host`: The address of the PostgreSQL server.
- `port`: The port of the PostgreSQL server.
- `dbname`: The name of the PostgreSQL database.
- `user`: The database user name.
- `password`: The database user password.
- `table_name`: PostgreSQL table name as either a string or a callable which receives 
  a `SinkItem` (from quixstreams.sinks.base.item) and returns a string.


### Optional

- `schema_name`: The schema name. Schemas are a way of organizing tables and 
  not related to the table data, referenced as `<schema_name>.<table_name>`.  
  PostrgeSQL uses "public" by default under the hood.
- `schema_auto_update`: If `True`, the sink will automatically update the schema by adding new columns when new fields are detected. Default: True.
- `primary_key_columns`: A single or multiple (composite) primary key columns.
  Can also provide a callable that accepts the message value as input, and returns a string or list of strings.
  Often paired with `upsert_on_primary_key=True`.
  It must include all currently defined primary_key columns on a given table.
  Once set, no others can be added; to change, the primary key must be removed manually.
- `upsert_on_primary_key`: if `True`, upsert based on the given `primary_key` argument.
  If False, every message is treated as an independent entry, and any primary key collisions will consequently raise an exception.
- `include_metadata`: if `True` (default), the sink writes two metadata columns for every row: `__key` (the record key) and `timestamp` (the record timestamp as a PostgreSQL `TIMESTAMP`). Set to `False` to omit both columns and write only the record's data fields. Use `False` when writing into an existing table whose schema does not include `__key` or `timestamp` columns.

  ```python
  # Write only data fields — no __key or timestamp columns
  postgres_sink = PostgreSQLSink(
      host="localhost",
      port=5432,
      dbname="mydatabase",
      user="myuser",
      password="mypassword",
      table_name="orders",
      include_metadata=False,
  )
  ```
- `on_conflict_do_nothing`: If `True`, duplicate rows are silently ignored using PostgreSQL's `ON CONFLICT DO NOTHING` clause on the INSERT statement. Use this when replayed or duplicate messages should be dropped at the database rather than inserted or upserted. Cannot be combined with `upsert_on_primary_key=True` — constructing the sink with both set to `True` raises a `ValueError`. Default: `False`.

**Choosing a conflict strategy:** use `upsert_on_primary_key=True` when you want the latest message to overwrite an existing row (last-write-wins). Use `on_conflict_do_nothing=True` when you want duplicate or replayed messages silently dropped. A primary key or unique constraint on the table is required for either option to take effect. The two options are mutually exclusive.


## Testing Locally

Rather than connect to a hosted InfluxDB3 instance, you can alternatively test your 
application using a local instance of Influxdb3 using Docker:

1. Execute in terminal:

    ```bash
    docker run --rm -d --name postgres \
    -e POSTGRES_PASSWORD=local \
    -e POSTGRES_USER=local \
    -e POSTGRES_DB=local \
    -p 5432:5432 \
    postgres
    ```

2. Use the following settings for `PostgreSQLSink` to connect:

    ```python
    PostgreSQLSink(
        host="localhost",
        port=5432,
        user="local",
        password="local",
        dbname="local",
        table_name="<YOUR TABLE NAME>",
    )
    ```
