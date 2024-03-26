# Upgrading from Legacy Quix Streams <2.0.0

If you were using a previous version of Quix Streams, more specifically the Quix
`ParameterData` or `EventData` formats and plan on using old 
topics or mixing legacy + new library versions, then this section is for you!

To learn more about the previous version of Quix Streams, please see [here](https://github.com/quixio/quix-streams/blob/release/v0.5/README.md)

***Note: Many of these details can be skipped for most users, but the context
may help you understand speed/operational differences you might experience, or if
you are an edge case that might require more work to convert.***

<br>



## Data Formats
Quix Streams <2.0 uses custom JSON-based serialization formats and utilizes special metadata messages.

The most important of them are:
- `ParameterData`
- `EventData`

Example of `ParameterData`
```json
{
  "C": "JT",
  "K": "ParameterData",
  "V": {
    "Epoch": 1000000,
    "Timestamps": [10, 11, 12],
    "NumericValues": {
      "p0": [100.0, 110.0, 120.0],
      },
    "StringValues": {
      "p1": ["100", "110", "120"]
      },
    "BinaryValues": {
      "p2": ["MTAw", "MTEw", "MTIw"]
      }
  },
  "S": 34,
  "E": 251
}
```

Example of `EventData`:

```json
{
  "C": "JT",
  "K": "EventData",
  "V": {
    "Timestamp": 100,
    "Tags": {
      "tag1": "tagValue"
    },
    "Id": "event1",
    "Value": "value a"
  },
  "S": 34,
  "E": 251
}
```

In order to consume or produce these, you must use the respective serializers and deserializers:

- `QuixDeserializer` (can deserialize both)
- `QuixEventsSerializer`
- `QuixTimeseriesSerializer`

Quix Streams supports producing and consuming of both EventData and ParameterData with some limitations

## Limitations

### Message Metadata Unsupported
In Quix Streams <2.0 it was possible to define a stream metadata and properties:
```python
# Open the output topic which is where data will be streamed out to
topic_producer = client.get_topic_producer(topic_id_or_name = "mytesttopic")

# Set stream ID or leave parameters empty to get stream ID generated.
stream = topic_producer.create_stream()
stream.properties.name = "Hello World Python stream"

# Add metadata about time series data you are about to send. 
stream.timeseries.add_definition("ParameterA").set_range(-1.2, 1.2)
stream.timeseries.buffer.time_span_in_milliseconds = 100
```

These metadata were passed to Kafka topics as messages.

Quix Streams >=2.0 has deprecated the metadata messages, and they are silently omitted 
during processing.


### "Split" Messages Unsupported

Quix Streams >= 2.0 has deprecated the "split" messaging format (it will remain 
supported in legacy C# library).

***`quixstreams>=2.0` will (gracefully) skip any split messages it encounters.***

<br>

### Batched/"Buffered" Message Handling

`quixstreams>=2.0` has deprecated producer-based batching/buffering of messages, 
which essentially consolidates multiple messages into one.

However, it will still be able to consume these messages without issue.

To process them, it will separate each record/message out and handle them individually. 

For example, a message with: 
```
{
  "NumericValues": {"p0": [10, 20, 30]},
  "StringValues": {"p1": ["a", "b", "c"]}
}
```
would equate to 3 independently processed messages.


<br>

### Multiple Message/Serialization Types

The Quix Streams <2.0 client supported and actively managed multiple message types like `ParameterData` and `EventData`
on one topic under the hood, which significantly complicates the client messaging model.

As such, `quixstreams>=2.0` intends to _encourage_ producing only one message type
per topic by enforcing 1 serializer per-topic, per-application. Additionally, 
`quixstreams>=2.0` ignores all but `TimeseriesData` and `EventData` message types 
when using a `QuixDeserializer`. 

It is still possible to handle multiple types in one application, 
but it must be done manually with a custom serialization and handling logic.

It is also possible to handle them by just using different applications, where each
handles a specific message type.
