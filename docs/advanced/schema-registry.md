# Schema Registry

Serializers and deserializers for JSON Schema, Avro, and Protobuf support integration with a Schema Registry.

The current implementation wraps Confluent's serializers and deserializers, which are tightly coupled with the Schema Registry.

To integrate your existing Schema Registry, install the `schema_registry` extra

```bash
pip install quixstreams[schema_registry]
```

and pass `SchemaRegistryClientConfig` to your serializers and deserializers. Additional optional configuration can be provided via `SchemaRegistrySerializationConfig`. 

> NOTE: Not every `Serializer`/`Deserializer` uses `SchemaRegistrySerializationConfig`; refer to each serialization type below for
> valid use.

```python
from quixstreams.models import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)

schema_registry_client_config = SchemaRegistryClientConfig(
    url='localhost:8081',
    basic_auth_user_info='username:password',
)

# optional and depends on serialization type 
schema_registry_serialization_config = SchemaRegistrySerializationConfig(
    auto_register_schemas=False,
)
```

**Note:** For the full list of available options, refer to the [Serializers API](../api-reference/serialization.md#quixstreamsmodelsserializersschema_registry).

## JSON Schema

For both the serializer and deserializer, a `schema` must be provided.

The serializer optionally accepts a `SchemaRegistrySerializationConfig`.

```python
from quixstreams.models import JSONDeserializer, JSONSerializer

MY_SCHEMA = {
    "title": "MyObject",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "id": {"type": "number"},
    },
    "required": ["id"],
}

deserializer = JSONDeserializer(
    schema=MY_SCHEMA,
    schema_registry_client_config=schema_registry_client_config,
)
serializer = JSONSerializer(
    schema=MY_SCHEMA,
    schema_registry_client_config=schema_registry_client_config,
    schema_registry_serialization_config=schema_registry_serialization_config,
)
```

## Avro

The serializer requires a `schema`, but the deserializer can automatically fetch the required schema from the Schema Registry.

The serializer optionally accepts a `SchemaRegistrySerializationConfig`.

```python
from quixstreams.models.serializers.avro import AvroDeserializer, AvroSerializer

MY_SCHEMA = {
    "type": "record",
    "name": "testschema",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "id", "type": "int", "default": 0},
    ],
}

deserializer = AvroDeserializer(
    schema_registry_client_config=schema_registry_client_config,
)
serializer = AvroSerializer(
    schema=MY_SCHEMA,
    schema_registry_client_config=schema_registry_client_config,
    schema_registry_serialization_config=schema_registry_serialization_config,
)
```

## Protobuf

For both the serializer and deserializer, `msg_type` must be provided.

The serializer AND deserializer optionally accept a `SchemaRegistrySerializationConfig`.


```python
from quixstreams.models.serializers.protobuf import ProtobufDeserializer, ProtobufSerializer

from my_input_models_pb2 import InputProto
from my_output_models_pb2 import OutputProto

deserializer = ProtobufDeserializer(
    msg_type=InputProto,
    schema_registry_client_config=schema_registry_client_config,
    schema_registry_serialization_config=schema_registry_serialization_config,
)
serializer = ProtobufSerializer(
    msg_type=OutputProto,
    schema_registry_client_config=schema_registry_client_config,
    schema_registry_serialization_config=schema_registry_serialization_config,
)
```

See the [Serialization and Deserialization](./serialization.md) page to learn more about how to integrate the serializer and deserializer with your application.
