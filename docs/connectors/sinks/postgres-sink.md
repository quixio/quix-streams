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

To sink data to PostgreSQL, you need to create an instance of PostgresSink and pass it to the StreamingDataFrame.sink() method:

```python
from quixstreams import Application
from postgres_sink import PostgresSink

app = Application(broker_address="localhost:9092")
topic = app.topic("numbers-topic")

# Initialize PostgresSink
postgres_sink = PostgresSink(
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

## How It Works

PostgresSink is a batching sink.
It batches processed records in memory per topic partition and writes them to the PostgreSQL database when a checkpoint has been committed.

Under the hood, it dynamically adjusts the schema by adding new columns if schema_auto_update is enabled. Processed records are written as rows in the specified table.

What Data Can Be Sent to PostgreSQL?

PostgresSink can accept only dictionary values.

If the record values are not dictionaries, you need to convert them to dictionaries using StreamingDataFrame.apply() before sinking.
	-	Key Column: The record key is inserted into a column named __key, if present.
	-	Timestamp Column: The record timestamp is inserted into a column named timestamp, with values stored in PostgreSQL’s TIMESTAMP format.
	-	Other Columns: Additional fields in the dictionary will be mapped to table columns. New columns are automatically added to the schema if schema_auto_update=True.

### Delivery Guarantees

PostgresSink provides at-least-once guarantees, meaning that the same records may be written multiple times in case of errors during processing.


## Configuration

PostgresSink accepts the following configuration parameters:
	-	host - The address of the PostgreSQL server.
	-	port - The port of the PostgreSQL server.
	-	dbname - The name of the PostgreSQL database.
	-	user - The database user name.
	-	password - The database user password.
	-	table_name - The name of the PostgreSQL table where data will be written.
	-	schema_auto_update - If True, the sink will automatically update the schema by adding new columns when new fields are detected. Default: True.
	-	ddl_timeout - Timeout (in seconds) for DDL operations such as creating tables or adding columns. Default: 10.
	-	batch_size - The number of records to write to PostgreSQL in one request. Default: 1000.
	-	request_timeout - Timeout (in seconds) for each write request. Default: 10.

