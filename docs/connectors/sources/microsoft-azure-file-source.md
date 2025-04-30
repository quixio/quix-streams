# Microsoft Azure File Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source reads records from files located in an Azure container path and produces 
them as messages to a kafka topic using any desired `StreamingDataFrame`-based transformations. 

The resulting messages can be produced in "replay" mode, where the time between record 
producing is matched as close as possible to the original. (per topic partition only).


## How To Install

Install Quix Streams with the following optional dependencies:

```bash
pip install quixstreams[azure-file]
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

Import and instantiate an `AzureFileSource` instance and hand it to an Application using
`app.add_source(<AzureFileSource>)` or instead to a StreamingDataFrame with 
`app.dataframe(source=<AzureFileSource>)` if further data manipulation is required.

For more details around various settings, see [configuration](#configuration).

```python
from quixstreams import Application
from quixstreams.sources.community.file.azure import AzureFileSource


def key_setter(record: dict) -> str:
    return record["host_id"]


def value_setter(record: dict) -> dict:
    return {k: record[k] for k in ["field_x", "field_y"]}


def timestamp_setter(record: dict) -> int:
    return record['timestamp']


source = AzureFileSource(
    filepath='folder_a/folder_b',
    container="<YOUR CONTAINER NAME>",
    connection_string="<YOUR CONNECTION STRING>",
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
    For Azure, be sure to exclude container name.    
    **Note**: If using alongside `*FileSink`, provide the path to the topic name folder (ex: `"path/to/topic_a/"`).
- `connection_string`: Azure client authentication string.
- `container`: Azure container name.

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

For easiest use (especially alongside [`FileSink`](../sinks/microsoft-azure-file-sink.md)), 
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

Rather than connect to Azure, you can alternatively test your application using a local 
emulated Azure host via Docker:

1. Execute in terminal:

    ```bash
    docker run --rm -d --name azurite \
    -p 10000:10000 \
    mcr.microsoft.com/azure-storage/azurite:latest
    ```

2. Set `connection_string` for `AzureOrigin` to: 

```python
"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
```
