# Apache Iceberg Sink 

TODO: Intro

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.


## How To Use Iceberg Sink

Create an instance of `IcebergSink` and pass 
it to the `StreamingDataFrame.sink()` method.

For the full description of expected parameters, ee the [Iceberg Sink API](../../api-reference/sinks.md#icebergsink) page.  

```python
from quixstreams import Application
from quixstreams.sinks.community.iceberg import IcebergSink, AWSIcebergConfig

# Configure S3 bucket credentials  
iceberg_config = AWSIcebergConfig(
    aws_s3_uri="", aws_region="", aws_access_key_id="", aws_secret_access_key=""
)

# Configure the sink to write data to S3 with the AWS Glue catalog spec 
iceberg_sink = IcebergSink(
    table_name="glue.sink-test",
    config=iceberg_config,
    data_catalog_spec="aws_glue",
)

app = Application(broker_address='localhost:9092', auto_offset_reset="earliest")
topic = app.topic('sink_topic')

# Do some processing here
sdf = app.dataframe(topic=topic).print(metadata=True)

# Sink results to the IcebergSink
sdf.sink(iceberg_sink)


if __name__ == "__main__":
    # Start the application
    app.run()
```

## How the Iceberg Sink Works
`IcebergSink` is a batching sink.  
It batches processed records in memory per topic partition, and writes them to the configured destination when a checkpoint is committed.

## Schema and Partition Spec
TODO


## Data Format
TODO


## Retrying Failures
`IcebergSink` will retry failed commits automatically with a random delay up to 5 seconds.

## Delivery Guarantees
`IcebergSink` provides at-least-once guarantees, and the results may contain duplicated rows of data if there were errors during processing.
