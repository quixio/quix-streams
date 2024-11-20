# Amazon Kinesis Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source reads data from an Amazon Kinesis stream, dumping it to a
kafka topic using desired `StreamingDataFrame`-based transformations.


## How To Install

To use the Kinesis sink, you need to install the required dependencies:

```bash
pip install quixstreams[kinesis]
```

## How It Works

`KinesisSource` reads from a Kinesis stream and produces its messages to a Kafka topic.

Records are read in a streaming fashion and committed intermittently, offering 
[at-least-once guarantees](#processingdelivery-guarantees).

Each shard in the Kinesis stream is consumed in a round-robin fashion to ensure 
reads are equally distributed.

You can learn more details about the [expected kafka message format](#message-data-formatschema) below.

## How To Use


To use Kinesis Source, hand `KinesisSource` to `app.dataframe()`.

For more details around various settings, see [configuration](#configuration).

```python
from quixstreams import Application
from quixstreams.sources.community.kinesis import KinesisSource


kinesis = KinesisSource(
    stream_name="<YOUR STREAM>",
    aws_access_key_id="<YOUR KEY ID>",
    aws_secret_access_key="<YOUR SECRET KEY>",
    aws_region="<YOUR REGION>",
    auto_offset_reset="earliest",  # start from the beginning of the stream (vs end)
)

app = Application(
    broker_address="<YOUR BROKER INFO>",
    consumer_group="<YOUR GROUP>",
)

sdf = app.dataframe(source=kinesis).print(metadata=True)
# YOUR LOGIC HERE!

if __name__ == "__main__":
    app.run()
```

## Configuration

Here are some important configurations to be aware of (see [Kinesis Source API](../../api-reference/sources.md#kinesissource) for all parameters).

### Required:

- `stream_name`: the name of the desired stream to consume.
- `aws_region`: AWS region (ex: us-east-1).    
    **Note**: can alternatively set the `AWS_REGION` environment variable.
- `aws_access_key_id`: AWS User key ID.
    **Note**: can alternatively set the `AWS_ACCESS_KEY_ID` environment variable.
- `aws_secret_access_key`: AWS secret key.    
    **Note**: can alternatively set the `AWS_SECRET_ACCESS_KEY` environment variable.


### Optional:

- `aws_endpoint_url`: Only fill when testing against a locally-hosted Kinesis instance.    
    **Note**: can leave other `aws` settings blank when doing so.    
    **Note**: can alternatively set the `AWS_ENDPOINT_URL_KINESIS` environment variable.
- `commit_interval`: How often to commit stream reads.    
    **Default**: `5.0s`

## Message Data Format/Schema

This is the default format of messages handled by `Application`:

- Message `key` will be the Kinesis record `PartitionKey` as a `string`.

- Message `value` will be the Kinesis record `Data` in `bytes` (transform accordingly
    with your `SDF` as desired).

- Message `timestamp` will be the Kinesis record `ArrivalTimestamp` (ms).


## Processing/Delivery Guarantees

The Kinesis Source offers "at-least-once" guarantees: offsets are managed using
an internal Quix Streams changelog topic.

As such, in rare circumstances where topic flushing ends up failing, messages may be 
processed (produced) more than once.
    
## Topic

The default topic name the Application dumps to is `source-kinesis_<stream name>`.


## Testing Locally

Rather than connect to AWS, you can alternatively test your application using 
a local Kinesis host via docker:

1. Set `aws_endpoint_url` for `KinesisSource` _OR_ the `AWS_ENDPOINT_URL_KINESIS` 
    environment variable to:
    
    `localhost:8085`

2. execute in terminal:

    `docker run --rm -d --name kinesis \
  -p 4566:4566 \
  -e SERVICES=kinesis \
  -e EDGE_PORT=4566 \
  -e DEBUG=1 \
  localstack/localstack:latest
`
