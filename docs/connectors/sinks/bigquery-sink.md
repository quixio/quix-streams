# BigQuery Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

BigQuery is a managed, serverless data warehouse developed by Google, offering scalable analysis over large quantities of data. 

Quix Streams provides a sink to write processed data to BigQuery

## How To Install
The dependencies for this sink are not included to the default `quixstreams` package.

To install them, run the following command:

```commandline
pip install quixstreams[bigquery]
```

## How To Use

To sink data to BigQuery, you need to create an instance of `BigQuery` and pass 
it to the `StreamingDataFrame.sink()` method:

```python
import os

from quixstreams import Application
from quixstreams.sinks.community.bigquery import BigQuerySink

app = Application(
    broker_address="localhost:9092",
    auto_offset_reset="earliest",
    consumer_group="consumer-group",
)

topic = app.topic("topic-name")

# Read the service account credentials in JSON format from some environment variable.
service_account_json = os.environ['BIGQUERY_SERVICE_ACCOUNT_JSON']

# Initialize a sink
bigquery_sink = BigQuerySink(
    project_id="<project ID>",
    location="<location>",
    dataset_id="<dataset ID>",
    table_name="<table name>",
    service_account_json=service_account_json,
    ddl_timeout=10.0,
    insert_timeout=10.0,
    retry_timeout=30.0,
)

sdf = app.dataframe(topic)
sdf.sink(bigquery_sink)

if __name__ == '__main__':
    app.run()
```

## How It Works
`BigQuery` is a batching sink.  
It batches processed records in memory per topic partition, and writes them to BigQuery when a checkpoint has been committed.

!!! info

    `BigQuerySink` can accept only dictionaries values.
    
    If the record values are not dicts, you need to convert them to dicts using `StreamingDataFrame.apply()` before sinking.

Under the hood, it uses the BigQuery REST API and writes data in batches in JSON format.

Each key in the record's dictionary will be inserted as a column to the resulting BigQuery table.

### Automatic schema updates
When it is first initialized, `BigQuerySink` will create the dataset and the table with minimal schema if they don't exist.

The initial table schema will have a single required column "timestamp" of a type `TIMESTAMP`. 

During the processing, the Sink will:

1. Add a column `__key` for the message keys with the type inferred from the first key it observes.  
For example, if the message keys are `bytes`, the Sink will add a new column `__key` of type `BYTES`.
2. Add new nullable columns to the table based on the keys from the records dictionaries.  
The column types are also inferred from Python types of the values automatically.

Here is how the Python types are mapped to the BigQuery column types:

```
{
    int: "NUMERIC",
    float: "NUMERIC",
    decimal.Decimal: "NUMERIC",
    str: "STRING",
    bytes: "BYTES",
    datetime: "DATETIME",
    date: "DATE",
    list: "JSON",
    dict: "JSON",
    tuple: "JSON",
    bool: "BOOLEAN",
}
```

To bypass the automatic schema updates, define the table with the necessary schema upfront.  

The Sink will not modify the column if it already exists.

### Data conversion
Some data types may be automatically converted by the underlying `google-cloud-bigquery` library when the data is written.  

For example, values of type `BYTES` are encoded to `base64` format before being sent.

## Delivery Guarantees
`BigQuerySink` provides at-least-once guarantees, and the same records may be written multiple times in case of errors during processing.  

## Configuration
For the full description of expected parameters, see the [BigQuery Sink API](../../api-reference/sinks.md#bigquerysink) page.

