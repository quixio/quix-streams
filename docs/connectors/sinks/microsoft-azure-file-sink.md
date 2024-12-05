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

`FileSink` with `AzureFileDestination` is a batching sink that writes data directly to Microsoft Azure.  

It batches processed records in memory per topic partition and writes them to Azure objects in a specified container and prefix structure. Objects are organized by topic and partition, with each batch being written to a separate object named by its starting offset.

Batches are written to Azure during the commit phase of processing. This means the size of each batch (and therefore each Azure object) is influenced by your application's commit settings - either through `commit_interval` or the `commit_every` parameters.

!!! note

    The Azure container must already exist and be accessible. The sink does not create the container automatically. If the container does not exist or access is denied, an error will be raised when initializing the sink.

## How To Use

Create an instance of `FileSink` with `AzureFileDestination` and pass it to the `StreamingDataFrame.sink()` method.

```python
from quixstreams import Application
from quixstreams.sinks.community.file import FileSink
from quixstreams.sinks.community.file.destinations import AzureFileDestination


# Configure the sink to write JSON files to Azure
file_sink = FileSink(
    # Optional: defaults to current working directory
    directory="data",
    # Optional: defaults to "json"
    # Available formats: "json", "parquet" or an instance of Format
    format=JSONFormat(compress=True),
    destination=AzureFileDestination(
        container="<YOUR CONTAINER NAME>",
        connection_string="<YOUR CONNECTION STRING>",
    )
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