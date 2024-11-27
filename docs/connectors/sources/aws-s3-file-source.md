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
pip install quixstreams[aws]
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

S3 File Source is just a special configuration of the `FileSource` connector.

Simply provide it an `S3FileOrigin` (`FileSource(origin=<ORIGIN>)`).

Then, hand the configured `FileSource` to your SDF (`app.dataframe(source=<SOURCE>)`).

For more details around various settings, see [configuration](#configuration).

```python
from quixstreams import Application
from quixstreams.sources.community.file import FileSource
from quixstreams.sources.community.file.origins import S3FileOrigin

app = Application(broker_address="localhost:9092", auto_offset_reset="earliest")

file_origin = S3FileOrigin(
    aws_s3_bucket="<YOUR BUCKET NAME>",
    aws_access_key_id="<YOUR KEY ID>",
    aws_secret_access_key="<YOUR SECRET KEY>",
    aws_region="<YOUR REGION>",
)
source = FileSource(
    filepath="path/to/your/topic_folder/",
    file_origin=file_origin,
    file_format="json",
    file_compression="gzip",
)
sdf = app.dataframe(source=source).print(metadata=True)
# YOUR LOGIC HERE!

if __name__ == "__main__":
    app.run()
```

## Configuration

Here are some important configurations to be aware of (see [File Source API](../../api-reference/sources.md#filesource) for all parameters).

### Required:

`S3FileOrigin`:

- `aws_s3_bucket`: The S3 bucket name only (ex: `"your-bucket"`).
- `aws_region`: AWS region (ex: us-east-1).    
    **Note**: can alternatively set the `AWS_REGION` environment variable.
- `aws_access_key_id`: AWS User key ID.
    **Note**: can alternatively set the `AWS_ACCESS_KEY_ID` environment variable.
- `aws_secret_access_key`: AWS secret key.    
    **Note**: can alternatively set the `AWS_SECRET_ACCESS_KEY` environment variable.


`FileSource`:

- `filepath`: a filepath to recursively read through (exclude bucket name).    
    **Note**: If using alongside `FileSink`, provide the path to the topic name folder (ex: `"path/to/topic_a/"`).    
- `file_origin`: An `S3FileOrigin` instance.


### Optional:

`FileSource`:

- `file_format`: what format the message files are in (ex: `"json"`, `"parquet"`).    
    **Advanced**: can optionally provide a `Format` instance (`file_compression` will then be ignored).    
    **Default**: `"json"`
- `file_compression`: what compression is used on the given files, if any (ex: `"gzip"`)    
    **Default**: `None`
- `as_replay`: Produce the messages with the original time delay between them, else as fast as possible.    
    **Note**: Time delay will only be accurate _per partition_, NOT overall.    
    **Default**: `True`

## Message Data Format/Schema

The expected file schema largely depends on the chosen 
file format.

For easiest use (especially alongside [`FileSink`](../sinks/file-sink.md)), 
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
on some `Format` classes (ex: `JsonFormat`) which can be handed to `FileSource(file_format=<Format>)`.

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
the `FileSource` `filepath` as: `source__<last folder name>`.
