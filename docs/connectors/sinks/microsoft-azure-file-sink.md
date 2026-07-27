# Microsoft Azure File Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes batches of data to Microsoft Azure in various formats.  
By default, the data will include the kafka message key, value, and timestamp.  

## How To Install

To use the Azure sink, you need to install the required dependencies:

```bash
pip install quixstreams[azure-file]
```

## How It Works

`AzureFileSink` is a batching sink that writes data directly to Microsoft Azure.  

It batches processed records in memory per topic partition and writes them to Azure objects in a specified container and prefix structure. Objects are organized by topic and partition, with each batch being written to a separate object named by its starting offset.

Batches are written to Azure during the commit phase of processing. This means the size of each batch (and therefore each Azure object) is influenced by your application's commit settings - either through `commit_interval` or the `commit_every` parameters.

!!! note

    The Azure container must already exist and be accessible. The sink does not create the container automatically. If the container does not exist or access is denied, an error will be raised when initializing the sink.

## How To Use

Create an instance of `AzureFileSink` and pass it to the `StreamingDataFrame.sink()` method.

```python
from quixstreams import Application
from quixstreams.sinks.community.file.azure import AzureFileSink
from quixstreams.sinks.community.file.destinations import AzureFileDestination


# Configure the sink to write JSON files to Azure
file_sink = AzureFileSink(
    azure_container="<YOUR AZURE CONTAINER NAME>",
    azure_connection_string="<YOUR AZURE CONNECTION STRING>",

    # Optional: defaults to current working directory
    directory="data",

    # Optional: defaults to "json"
    # Available formats: "json", "parquet" or an instance of Format
    format=JSONFormat(compress=True),
)

app = Application(broker_address='localhost:9092', auto_offset_reset="earliest")
topic = app.topic('sink-topic')

sdf = app.dataframe(topic=topic)
sdf.sink(file_sink)

if __name__ == "__main__":
    app.run()
```

## Azure Object Organization

Objects in Azure follow this structure:
```
my-container/
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

`FileSink` provides at-least-once guarantees, and the results may contain duplicated data if there were errors during processing.


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