# Serialization and Deserialization (SerDes)

SerDes simply refers to how you pack (serialize) or unpack (deserialize) your data 
when publishing to or reading from a topic.
<br>
These settings are defined per-topic via these parameters of `Application.topic()` function:
- `key_serializer`
- `value_serializer`
- `key_deserializer`
- `value_deserializer`

By default, message values are serialized with  `JSON`, message keys are serialized with `bytes` (i.e. passed as they are received from Kafka).

## Supported formats:
- `bytes`
- `string`
- `integer`
- `double`
- `json`
- `quix` - for deserializers only
- `quix_events` & `quix_timeseries` - for serializers only.

## Using SerDes
To set a serializer, you may either pass a string shorthand for it, or an instance of `streamingdataframes.models.serializers.Serializer` and `streamingdataframes.models.serializers.Deserializer` directly 
to the `Application.topic()`.

Example with format shorthands:
```python
from streamingdataframes.models.serializers import JSONDeserializer
app = Application(...)
# Deserializing message values from JSON to objects and message keys as strings 
input_topic = app.topic(value_deserializer='json', key_deserializer='string')

# Serializing message values to JSON and message keys to bytes
output_topic = app.topic(value_serializer='json', key_deserializer='bytes')
```

Passing `Serializer` and `Deserializer` instances directly:

```python
from streamingdataframes.models.serializers import JSONDeserializer, JSONSerializer
app = Application(...)
input_topic = app.topic(value_deserializer=JSONDeserializer())
output_topic = app.topic(value_deserializer=JSONSerializer())
```

You can find all available serializers in `streamingdataframes.models.serializers` module.

We also plan on including other popular ones like Avro and Protobuf in the near future.

## Data format
Currently, Quix Streams 2.0a expects all values to be serialized and deserialized as dictionaries.
If you need to consume messages formatted as simple types, you need to pass `column_name="<some_column>"` to Deserializer class.
The Deserializer object will wrap the received value to the dictionary with `column_name` as a key.

Example:

```python
from streamingdataframes.models.serializers import IntegerDeserializer
app = Application(...)
input_topic = app.topic(value_deserializer=IntegerDeserializer(column_name='number'))
# Will deserialize message with value "123" to "{'number': 123}" ...
```