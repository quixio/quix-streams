# File Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes batches of data to files on disk in various formats.  
By default, the data will include the kafka message key, value, and timestamp.  

Currently, it supports the following formats:

- JSON
- Parquet

## How the File Sink Works

`FileSink` is a batching sink.  

It batches processed records in memory per topic partition and writes them to files in a specified directory structure. Files are organized by topic and partition, with each batch being written to a separate file named by its starting offset.

The sink can either create new files for each batch or append to existing files (when using formats that support appending).

## How To Use File Sink

Create an instance of `FileSink` and pass it to the `StreamingDataFrame.sink()` method.

For the full description of expected parameters, see the [File Sink API](../../api-reference/sinks.md#filesink) page.  

```python
from quixstreams import Application
from quixstreams.sinks.community.file import FileSink

# Configure the sink to write JSON files
file_sink = FileSink(
    output_dir="./output",
    format="json",
    append=False  # Set to True to append to existing files when possible
)

app = Application(broker_address='localhost:9092', auto_offset_reset="earliest")
topic = app.topic('sink_topic')

# Do some processing here
sdf = app.dataframe(topic=topic).print(metadata=True)

# Sink results to the FileSink
sdf.sink(file_sink)

if __name__ == "__main__":
    # Start the application
    app.run()
```

## File Organization
Files are organized in the following directory structure:
```
output_dir/
├── sink_topic/
│   ├── 0/
│   │   ├── 0000000000000000000.json
│   │   ├── 0000000000000000123.json
│   │   └── 0000000000000001456.json
│   └── 1/
│       ├── 0000000000000000000.json
│       ├── 0000000000000000789.json
│       └── 0000000000000001012.json
```

Each file is named using the batch's starting offset (padded to 19 digits) and the appropriate file extension for the chosen format.

## Supported Formats
- **JSON**: Supports appending to existing files
- **Parquet**: Does not support appending (new file created for each batch)

## Delivery Guarantees
`FileSink` provides at-least-once guarantees, and the results may contain duplicated data if there were errors during processing.
