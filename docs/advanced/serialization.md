# Serialization and Deserialization

Quix Streams supports multiple serialization formats to exchange data between Kafka topics:

- `bytes`
- `string`
- `integer`
- `double`
- `json`

The serialization settings are defined per-topic using these parameters of `Application.topic()` function:

- `key_serializer`
- `value_serializer`
- `key_deserializer`
- `value_deserializer`

By default, message values are serialized with `json` and message keys are serialized with `bytes` (i.e. passed as they are received from Kafka).

Note: The legacy `quix` serializer and legacy `quix_events` and `quix_timeseries` deserializers are still supported but may be deprecated in future. New stream processing applications should avoid using these 3 formats.

## Configuring Serialization
To set a serializer, you may either pass a string shorthand for it, or an instance of `quixstreams.models.serializers.Serializer` and `quixstreams.models.serializers.Deserializer` directly 
to the `Application.topic()`.

**Example:**

```python
from quixstreams import Application
app = Application(broker_address='localhost:9092', consumer_group='consumer')
# Deserializing message values from JSON to objects and message keys as strings 
input_topic = app.topic('input', value_deserializer='json', key_deserializer='string')

# Serializing message values to JSON and message keys to bytes
output_topic = app.topic('output', value_serializer='json', key_deserializer='bytes')
```


Passing `Serializer` and `Deserializer` instances directly:

```python
from quixstreams import Application
from quixstreams.models import JSONDeserializer, JSONSerializer

app = Application(broker_address='localhost:9092', consumer_group='consumer')
input_topic = app.topic('input', value_deserializer=JSONDeserializer())
output_topic = app.topic('output', value_serializer=JSONSerializer())
```

You can find all available serializers in `quixstreams.models.serializers` module.

## Jsonschema support

The json serializer and deserializer support validation of the data against a jsonschema.

```python
from jsonschema import Draft202012Validator

from quixstreams import Application
from quixstreams.models import JSONDeserializer, JSONSerializer

MY_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "id": {"type": "number"},
    },
    "required": ["id"],
}

app = Application(broker_address='localhost:9092', consumer_group='consumer')
input_topic = app.topic('input', value_deserializer=JSONDeserializer(schema=MY_SCHEMA))
output_topic = app.topic('output', value_serializer=JSONSerializer(schema=MY_SCHEMA))
```
