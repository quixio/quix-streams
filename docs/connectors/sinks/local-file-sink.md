# Local File Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes batches of data to local files in various formats.
By default, the data will include the kafka message key, value, and timestamp.

## How It Works

`FileSink` with `LocalDestination` is a batching sink that writes data to your local filesystem.

It batches processed records in memory per topic partition and writes them to files in a specified directory structure. Files are organized by topic and partition. When append mode is disabled (default), each batch is written to a separate file named by its starting offset. When append mode is enabled, all records for a partition are written to a single file.

The sink can either create new files for each batch or append to existing files (when using formats that support appending).

## How To Use

Create an instance of `FileSink` and pass it to the `StreamingDataFrame.sink()` method.

```python
from quixstreams import Application
from quixstreams.sinks.community.file import FileSink
from quixstreams.sinks.community.file.destinations import LocalDestination
from quixstreams.sinks.community.file.formats import JSONFormat

# Configure the sink to write JSON files
file_sink = FileSink(
    # Optional: defaults to current working directory
    directory="data",
    # Optional: defaults to "json"
    # Available formats: "json", "parquet" or an instance of Format
    format=JSONFormat(compress=True),
    # Optional: defaults to LocalDestination(append=False)
    destination=LocalDestination(append=True),
)

app = Application(broker_address='localhost:9092', auto_offset_reset="earliest")
topic = app.topic('sink-topic')

sdf = app.dataframe(topic=topic)
sdf.sink(file_sink)

if __name__ == "__main__":
    app.run()
```

## File Organization

Files are organized in the following directory structure:
```
data/
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

Each file is named using the batch's starting offset (padded to 19 digits) and the appropriate file extension for the chosen format.

## Supported Formats

- **JSON**: Supports appending to existing files
- **Parquet**: Does not support appending (new file created for each batch)

## Delivery Guarantees

`FileSink` provides at-least-once guarantees, and the results may contain duplicated data if there were errors during processing.
