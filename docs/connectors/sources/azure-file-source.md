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
pip install quixstreams[azure]
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

Azure File Source is just a special configuration of the `FileSource` connector.

Simply provide it an `AzureOrigin` (`FileSource(origin=<ORIGIN>)`).

Then, hand the configured `FileSource` to your `SDF` (`app.dataframe(source=<SOURCE>)`).

For more details around various settings, see [configuration](#configuration).

```python
from quixstreams import Application
from quixstreams.sources.community.file import FileSource
from quixstreams.sources.community.file.origins import AzureOrigin

app = Application(broker_address="localhost:9092", auto_offset_reset="earliest")

origin = AzureOrigin(
    container="<YOUR CONTAINER NAME>",
    connection_string="<YOUR CONNECTION STRING>",
)
source = FileSource(
    directory="path/to/your/topic_folder/",
    origin=origin,
    format="json",
    compression="gzip",
)
sdf = app.dataframe(source=source).print(metadata=True)
# YOUR LOGIC HERE!

if __name__ == "__main__":
    app.run()
```

## Configuration

Here are some important configurations to be aware of (see [File Source API](../../api-reference/sources.md#filesource) for all parameters).

### Required:

`AzureOrigin`:

- `connection_string`: Azure client authentication string.
- `container`: Azure container name.


`FileSource`:

- `directory`: a directory to recursively read through (exclude container name).    
    **Note**: If using alongside `FileSink`, provide the path to the topic name folder (ex: `"path/to/topic_a/"`).    
- `origin`: An `AzureOrigin` instance.


### Optional:

`FileSource`:

- `format`: what format the message files are in (ex: `"json"`, `"parquet"`).    
    **Advanced**: can optionally provide a `Format` instance (`compression` will then be ignored).    
    **Default**: `"json"`
- `compression`: what compression is used on the given files, if any (ex: `"gzip"`)    
    **Default**: `None`
- `as_replay`: Produce the messages with the original time delay between them, else as fast as possible.    
    **Note**: Time delay will only be accurate _per partition_, NOT overall.    
    **Default**: `True`

## File hierarchy/structure

The File Source expects a folder structure like so:

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
