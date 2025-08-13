# AmazonS3 Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes batches of data to Amazon S3 in various formats.  
By default, the data will include the kafka message key, value, and timestamp.  

## How To Install

To use the S3 sink, you need to install the required dependencies:

```bash
pip install quixstreams[s3]
```

## How It Works

`S3FileSink` is a batching sink that writes data directly to Amazon S3.  

It batches processed records in memory per topic partition and writes them to S3 objects in a specified bucket and prefix structure. Objects are organized by topic and partition, with each batch being written to a separate object named by its starting offset.

Batches are written to S3 during the commit phase of processing. This means the size of each batch (and therefore each S3 object) is influenced by your application's commit settings - either through `commit_interval` or the `commit_every` parameters.

!!! note

    The S3 bucket must already exist and be accessible. The sink does not create the bucket automatically. If the bucket does not exist or access is denied, an error will be raised when initializing the sink.

## How To Use

Create an instance of `S3FileSink` and pass it to the `StreamingDataFrame.sink()` method.

```python
import os

from quixstreams import Application
from quixstreams.sinks.community.file.s3 import S3FileSink
from quixstreams.sinks.community.file.formats import JSONFormat


# Configure the sink to write JSON files to S3
file_sink = S3FileSink(
    bucket="my-bucket",
    region_name="eu-west-2",

    # Optional: defaults to current working directory
    directory="data",
    
    # Optional: defaults to "json"
    # Available formats: "json", "parquet" or an instance of Format
    format=JSONFormat(compress=True),
    
    # Optional: AWS credentials
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    
    # Optional: For local testing
    endpoint_url="http://localhost:4566",
    
    # Optional: Additional keyword arguments are passed to boto3 client
)

app = Application(broker_address='localhost:9092', auto_offset_reset="earliest")
topic = app.topic('sink-topic')

sdf = app.dataframe(topic=topic)
sdf.sink(file_sink)

if __name__ == "__main__":
    app.run()
```

!!! note
    Instead of passing AWS credentials explicitly, you can set them using environment variables:
    ```bash
    export AWS_ACCESS_KEY_ID="your_access_key"
    export AWS_SECRET_ACCESS_KEY="your_secret_key"
    export AWS_DEFAULT_REGION="eu-west-2"
    export AWS_ENDPOINT_URL_S3="http://your.url.here"
    ```
    Then you can create the sink with just the bucket name:
    ```python
    s3_sink = S3FileSink(bucket="my-bucket")
    ```

## S3 Object Organization

Objects in S3 follow this structure:
```
my-bucket/
└── data/
    └── sink_topic/
        ├── 0/
        │   ├── 0000000000000000000.jsonl
        │   ├── 0000000000000000123.jsonl
        │   └── 0000000000000001456.jsonl
        └── 1/
            ├── 0000000000000000000.jsonl
            ├── 0000000000000000789.jsonl
            └── 0000000000000001012.jsonl
```

Each object is named using the batch's starting offset (padded to 19 digits) and the appropriate file extension for the chosen format.

## Supported Formats

- **JSON**: Supports appending to existing files
- **Parquet**: Does not support appending (new file created for each batch)

## Delivery Guarantees

`S3FileSink` provides at-least-once guarantees, and the results may contain duplicated data if there were errors during processing.


## Testing Locally

Rather than connect to AWS, you can alternatively test your application using a local 
emulated S3 host via Docker (using minio):

1. Execute in terminal:

    ```bash
    docker run --rm -d --name minio \
    -p 9000-9001:9000-9001 \
    -e MINIO_ROOT_USER=admin \
    -e MINIO_ROOT_PASSWORD=admin_pw \
    -v /data \
    quay.io/minio/minio server /data --console-address ":9001"
    ```

2. 
    - Navigate to the UI at `http://localhost:9001`
    - Authenticate with `username=admin`, `password=admin_pw`
    - Create a bucket.

3. Connect using the following:
    ```python
    from quixstreams.sinks.community.file.s3 import S3FileSink
    
    S3FileSink(
        bucket="<YOUR BUCKET NAME>",
        aws_access_key_id='admin',
        aws_secret_access_key='admin_pw',
        region_name='us-east-1',
        endpoint_url='http://localhost:9000',
    )
    ```
