# AWS S3 File Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source reads records from files located in an AWS S3 bucket path and produces 
them as messages to a kafka topic using any desired `StreamingDataFrame`-based transformations. 

The resulting messages can be produced in "replay" mode, where the time between record 
producing is matched as close as possible to the original. (per topic partition only).


## How To Install

Install Quix Streams with the following optional dependencies:

```bash
pip install quixstreams[s3]
```

## How It Works

`FileSource` steps through each folder within the provided path and dumps each record 
contained in each file as a message to a Kafka topic. Folders are navigated in 
lexicographical order.

Records are read in a streaming fashion and committed after every file, offering 
[at-least-once guarantees](#processingdelivery-guarantees).

It can handle one given file type (ex. JSONlines or Parquet) at a time, and also 
supports file decompression.

You can learn more details about the [expected kafka message format](#message-data-formatschema) below.

## How To Use

Import and instantiate an `S3FileSource` instance and hand it to an Application using
`app.add_source(<S3FileSource>)` or instead to a StreamingDataFrame with 
`app.dataframe(source=<S3FileSource>)` if further data manipulation is required.

For more details around various settings, see [configuration](#configuration).

```python
from quixstreams import Application
from quixstreams.sources.community.file.s3 import S3FileSource


def key_setter(record: dict) -> str:
    return record["host_id"]


def value_setter(record: dict) -> dict:
    return {k: record[k] for k in ["field_x", "field_y"]}


def timestamp_setter(record: dict) -> int:
    return record['timestamp']


source = S3FileSource(
    filepath='folder_a/folder_b',
    bucket="<YOUR BUCKET NAME>",
    region_name="<YOUR REGION>",
    aws_access_key_id="<YOUR KEY ID>",
    aws_secret_access_key="<YOUR SECRET KEY>",
    key_setter=key_setter,
    value_setter=value_setter,
    timestamp_setter=timestamp_setter,
    file_format="json",
    compression="gzip",
    has_partition_folders=False,
    replay_speed=0.5,
)
app = Application(
    broker_address="localhost:9092", 
    consumer_group='file-source',
    auto_offset_reset='latest',
)
app.add_source(source)


if __name__ == "__main__":
    app.run()
```

## Configuration

Here are some important configurations to be aware of (see [File Source API](../../api-reference/sources.md#filesource) for all parameters).

### Required:

- `filepath`: folder to recursively iterate from (a file will be used directly).  
    For S3, exclude bucket name or starting "/".    
    **Note**: If using alongside `FileSink`, provide the path to the topic name folder (ex: `"path/to/topic_a/"`).
- `bucket`: The S3 bucket name only (ex: `"your-bucket"`).
- `region_name`: AWS region (ex: us-east-1).    
    **Note**: can alternatively set the `AWS_REGION` environment variable.
- `aws_access_key_id`: AWS User key ID.
    **Note**: can alternatively set the `AWS_ACCESS_KEY_ID` environment variable.
- `aws_secret_access_key`: AWS secret key.    
    **Note**: can alternatively set the `AWS_SECRET_ACCESS_KEY` environment variable.

### Optional:

- `format`: what format the message files are in (ex: `"json"`, `"parquet"`).    
    **Advanced**: can optionally provide a `Format` instance (`compression` will then be ignored).    
    **Default**: `"json"`
- `compression`: what compression is used on the given files, if any (ex: `"gzip"`)    
    **Default**: `None`
- `replay_speed`: Produce the messages with this speed multiplier, which roughly 
    reflects the time "delay" between the original message producing.    
    Use any `float` `>= 0.0`, where `0.0` is no delay, and `1.0` is the original speed.    
    **Note**: Time delay will only be accurate _per partition_, NOT overall.    
    **Default**: 1.0


## Supported File Hierarchies

All `*FileSource` types support both single file referencing and recursive folder traversal.

In addition, it also supports a topic-partition file structure as produced by a Quix 
Streams `*FileSink` instance. 


### Using with a Topic-Partition hierarchy (from `*FileSink`)

A Topic-Partition structure allows reproducing messages to the exact partition
they originated from.

When using a Quix Streams `*FileSink`, it will produce files using this structure:

```
    my_sinked_topics/
    ├── topic_a/          # topic name (use this path to File Source!)
    │   ├── 0/            # topic partition number
    │   │   ├── 0000.ext  # formatted offset files (ex: JSON)
    │   │   └── 0011.ext
    │   └── 1/
    │       ├── 0003.ext
    │       └── 0016.ext
    └── topic_b/
        └── etc...
```

To have `*FileSource` reflect this partition mapping for messages (instead of just producing
messages to whatever partition is applicable), it must know how many partition folders 
there are so it can create a topic with that many partitions.

To enable this: 
1. subclass your `*FileSource` instance and define the `file_partition_counter` method.
    - this will be run before processing any files.
2. Enable the use of `file_partition_counter` by setting the flag `has_partition_folders=True`.
3. Extract the original Kafka key with `key_setter` (by default, it uses the same field name that `*FinkSink` writes to).
   - see [message data schema](#message-data-formatschema) for more info around expected defaults.

#### Example
As a simple example, using the topic-partition file structure:

```
├── my_topic/        
│   ├── 0/          
│   │   ├── 0000.ext
│   │   └── 0011.ext
│   └── 1/
│       ├── 0003.ext
│       └── 0016.ext
```

you could define `file_partition_counter` on `LocalFileSource` like this:

```python
from quixstreams.sources.community.file.local import LocalFileSource

class MyLocalFileSource(LocalFileSource):
    
    def file_partition_counter(self) -> int:
        return len([f for f in self._filepath.iterdir()])  # `len(['0', '1'])`
```

Also, for our `key_setter`:
```python
def my_key_setter(record: dict) -> str:
    return record["original_key_field"]
```

Then when initing with your new class:

```python
source = MyLocalFileSource(
    ..., # required args,
    has_partition_folders=True,
    key_setter=my_key_setter,
)
```

This will produce these messages across the 2 partitions in their original partitioning
and ordering.


## Message Data Format/Schema

The expected file schema largely depends on the chosen 
file format.

For easiest use (especially alongside [`FileSink`](../sinks/amazon-s3-sink.md)), 
you can follow these patterns: 

### Row-based Formats (ex: JSON)

Files should have records with the following fields, with `_value` being a 
JSON-deserializable item:

  - `_key`
  - `_value`
  - `_timestamp`


This will result in the following Kafka message format for `Application`:

- Message `key` will be the record `_key` as `bytes`.
- Message `value` will be the record `_value` as a `json`/`dict`
- Message `timestamp` will be the record `_timestamp` (ms).

### Columnar Formats (ex: Parquet)
These do not expect an explicit `value` field; instead all columns should be included 
individually while including `_key` and `_timestamp`:

  - `_key`
  - `_timestamp`
  - `field_a`
  - `field_b`    
  etc...


This will result in the following Kafka message format for `Application`:

- Message `key` will be the record `_key` as `bytes`.
- Message `value` will be every record field except `_key` and `_timestamp` packed as a `json`/`dict`
- Message `timestamp` will be the record `_timestamp` (ms).


### Custom Schemas (Advanced)

If the original files are not formatted as expected, custom loaders can be configured 
on some `Format` classes (ex: `JsonFormat`) which can be handed to `FileSource(format=<Format>)`.

Formats can be imported from `quixstreams.sources.community.file.formats`.

## Processing/Delivery Guarantees

This Source offers "at-least-once" guarantees with message delivery: messages are
guaranteed to be committed when a file is finished processing.

However, it does not save any state/position: an unhandled exception will cause the 
`Application` to fail, and rerunning the `Application` will begin processing from the
beginning (reproducing all previously processed messages).

## Topic

The default topic will have a partition count that reflects the partition count found 
within the provided topic's folder structure.

The default topic name the Application dumps to is based on the last folder name of 
the `FileSource` `directory` as: `source__<last folder name>`.

## Testing Locally

Rather than connect to AWS, you can alternatively test your application using a local 
emulated S3 host via Docker:

1. Execute in terminal:

    ```bash
    docker run --rm -d --name s3 \
    -p 4566:4566 \
    -e SERVICES=s3 \
    -e EDGE_PORT=4566 \
    -e DEBUG=1 \
    localstack/localstack:latest
    ```

2. Set `endpoint_url` for `S3Origin` _OR_ the `AWS_ENDPOINT_URL_S3` 
    environment variable to `http://localhost:4566`

3. Set all other `aws_` parameters for `S3Origin` to _any_ string. 
They will not be used, but they must still be populated!
