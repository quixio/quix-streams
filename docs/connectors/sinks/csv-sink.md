# CSV Sink

A basic sink to write processed data to a single CSV file.

It's meant to be used mostly for local debugging.

## How To Use CSV Sink

To use a CSV sink, you need to create an instance of `CSVSink` and pass 
it to the `StreamingDataFrame.sink()` method:

```python
from quixstreams import Application
from quixstreams.sinks.csv import CSVSink

app = Application(broker_address='localhost:9092')
topic = app.topic('input-topic')

# Initialize a CSVSink with a file path 
csv_sink = CSVSink(path='file.csv')

sdf = app.dataframe(topic)
# Do some processing here ...
# Sink data to a CSV file
sdf.sink(csv_sink)
```

## How CSV Sink works
`CSVSink` is a batching sink.  
It batches processed records in memory per topic partition, and writes them to the file on checkpoint.  

The output file format is the following:
```
key,value,timestamp,topic,partition,offset
b'afd7e8ab-4af5-4322-8417-dbfc7a0d7694',"{""number"": 0}",1722945524540,numbers-10k-keys,0,0
b'557bae7f-14b6-46c4-abc3-12f232b54c8e',"{""number"": 1}",1722945524546,numbers-10k-keys,0,1
```
## Serialization Formats
By default, `CSVSink` serializes record keys by doing `str()` on them, and messages values with `json.dumps`.

To use your own serilization, pass `key_serializer` and `value_serializer` to `CSVSink`:

```python
import json
from quixstreams.sinks.csv import CSVSink

# Initialize a CSVSink with a file path 
csv_sink = CSVSink(
    path='file.csv',
    # Define custom serializers for keys and values here.
    # The callables must accept one argument for key/value, and return a string
    key_serializer=lambda key: json.dumps(key),
    value_serializer=lambda value: str(value), 
)
```

## Delivery guarantees
The `CSVSink` provides at-least-once guarantees, and the result CSV file may contain duplicated data in case of errors during processing.
