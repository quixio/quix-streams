# CSV Source

A base CSV source that reads data from a CSV file and produces rows to the Kafka topic in JSON format.

The CSV source reads the file, produce the data and exit. It doesn't keep any state. On restart, the whole file will be re-consumed.

## How to use CSV Source

To use a CSV Source, you need to create and instance of `CSVSource` and pass it to the `app.dataframe()` method.

```python
from quixstreams import Application
from quixstreams.sources.core.csv import CSVSource

def main():
    app = Application()
    # Create the Source instance with a file path and a name.
    # The name will be included to the default topic name. 
    source = CSVSource(path="input.csv", name="csv")

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()

if __name__ == "__main__":
    main()
```

## File format

The CSV source expect the input file to have headers.

Every row will be converted to a JSON dictionary and set to the topic.

Example file:

```csv
field1,field2,timestamp
foo1,bar1,1
foo2,bar2,2
foo3,bar3,3
```

What the source will produce:
```json lines
{"field1": "foo1", "field2": "bar1", "timestamp":  "1"}
{"field1": "foo2", "field2": "bar2", "timestamp":  "2"}
{"field1": "foo3", "field2": "bar3", "timestamp":  "3"}
```

## Key and timestamp extractors
By default, the produced Kafka messages don't have keys and use current epoch as timestamps.

To specify keys and timestamps for the messages, you may pass `key_extractor` and `timestamp_extractor` callables:

```python
from typing import AnyStr

from quixstreams import Application
from quixstreams.sources.core.csv import CSVSource


def key_extractor(row: dict) -> AnyStr:
    return row["field1"]


def timestamp_extractor(row: dict) -> int:
    return int(row["timestamp"])


def main():
    app = Application(broker_address="localhost:9092")
    # input.csv:
    #   field1,field2,timestamp
    #   foo1,bar1,1
    #   foo2,bar2,2
    #   foo3,bar3,3

    source = CSVSource(
        path="input.csv",
        name="csv",
        # Extract field "field1" from each row and use it as a message key.
        # Keys must be either strings or bytes.
        key_extractor=key_extractor,
        # Extract field "timestamp" from each row and use it as a timestamp.
        # Timestamps must be integers in milliseconds.
        timestamp_extractor=timestamp_extractor,
    )

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()
```


## Topic

The default topic used for the CSV source will use the `name` as a part of the topic name and expect keys to be strings and values to be JSON objects.  
