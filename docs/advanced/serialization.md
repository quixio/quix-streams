# Serialization and Deserialization

Quix Streams supports multiple serialization formats for exchanging data between Kafka topics:

- `bytes`
- `string`
- `integer`
- `double`
- `json`
- `avro`
- `protobuf`

The serialization settings are defined per topic using these parameters of the `Application.topic()` function:

- `key_serializer`
- `value_serializer`
- `key_deserializer`
- `value_deserializer`

By default, message values are serialized with `json`, and message keys are serialized with `bytes` (i.e., passed as they are received from Kafka).

**Note:** The legacy `quix` serializer and legacy `quix_events` and `quix_timeseries` deserializers are still supported but may be deprecated in the future. New stream processing applications should avoid using these three formats.

## Configuring Serialization

To set a serializer, you can either pass a string shorthand for it or an instance of `quixstreams.models.serializers.Serializer` and `quixstreams.models.serializers.Deserializer` directly to the `Application.topic()` function.

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

You can find all available serializers in the `quixstreams.models.serializers` module.

## JSON Schema Support

The JSON serializer and deserializer support data validation against a JSON Schema.

```python
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

## Avro

Apache Avro is a row-based binary serialization format. Avro stores the schema in JSON format alongside the data, enabling efficient processing and schema evolution.

You can learn more about the Apache Avro format [here](https://avro.apache.org/docs/).
The Avro serializer and deserializer need to be passed explicitly and must include the schema.

> **WARNING**: The Avro serializer and deserializer require the `fastavro` library.  
> You can install Quix Streams with the necessary dependencies using:  
> `pip install quixstreams[avro]`

```python
from quixstreams import Application
from quixstreams.models.serialize.avro import AvroSerializer, AvroDeserializer

MY_SCHEMA = {
    "type": "record",
    "name": "testschema",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "id", "type": "int", "default": 0},
    ],
}

app = Application(broker_address='localhost:9092', consumer_group='consumer')
input_topic = app.topic('input', value_deserializer=AvroDeserializer(schema=MY_SCHEMA))
output_topic = app.topic('output', value_serializer=AvroSerializer(schema=MY_SCHEMA))
```

## Protobuf

Protocol Buffers are language-neutral, platform-neutral extensible mechanisms for serializing structured data.

You can learn more about the Protocol Buffers format [here](https://protobuf.dev/).
The Protobuf serializer and deserializer need to be passed explicitly and must include the schema.

> **WARNING**: The Protobuf serializer and deserializer require the `protobuf` library.  
> You can install Quix Streams with the necessary dependencies using:  
> `pip install quixstreams[protobuf]`

```python
from quixstreams import Application
from quixstreams.models.serialize.protobuf import ProtobufSerializer, ProtobufDeserializer

from my_input_models_pb2 import InputProto
from my_output_models_pb2 import OutputProto

app = Application(broker_address='localhost:9092', consumer_group='consumer')
input_topic = app.topic('input', value_deserializer=ProtobufDeserializer(msg_type=InputProto))
output_topic = app.topic('output', value_serializer=ProtobufSerializer(msg_type=OutputProto))
```

By default, the Protobuf deserializer will deserialize the message to a Python dictionary. Doing this has a big performance impact. You can disable this behavior by initializing the deserializer with `to_dict` set to `False`. The Protobuf message object will then be used as the message value, limiting the available StreamingDataframe API.
