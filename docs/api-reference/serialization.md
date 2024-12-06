<a id="quixstreams.models.serializers.simple_types"></a>

## quixstreams.models.serializers.simple\_types

<a id="quixstreams.models.serializers.simple_types.BytesDeserializer"></a>

### BytesDeserializer

```python
class BytesDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L56)

A deserializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.BytesSerializer"></a>

### BytesSerializer

```python
class BytesSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L65)

A serializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.StringDeserializer"></a>

### StringDeserializer

```python
class StringDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L74)

<a id="quixstreams.models.serializers.simple_types.StringDeserializer.__init__"></a>

<br><br>

#### StringDeserializer.\_\_init\_\_

```python
def __init__(codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L75)

Deserializes bytes to strings using the specified encoding.


<br>
***Arguments:***

- `codec`: string encoding
A wrapper around `confluent_kafka.serialization.StringDeserializer`.

<a id="quixstreams.models.serializers.simple_types.IntegerDeserializer"></a>

### IntegerDeserializer

```python
class IntegerDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L93)

Deserializes bytes to integers.

A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.

<a id="quixstreams.models.serializers.simple_types.DoubleDeserializer"></a>

### DoubleDeserializer

```python
class DoubleDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L111)

Deserializes float to IEEE 764 binary64.

A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.

<a id="quixstreams.models.serializers.simple_types.StringSerializer"></a>

### StringSerializer

```python
class StringSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L129)

<a id="quixstreams.models.serializers.simple_types.StringSerializer.__init__"></a>

<br><br>

#### StringSerializer.\_\_init\_\_

```python
def __init__(codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L130)

Serializes strings to bytes using the specified encoding.


<br>
***Arguments:***

- `codec`: string encoding

<a id="quixstreams.models.serializers.simple_types.IntegerSerializer"></a>

### IntegerSerializer

```python
class IntegerSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L142)

Serializes integers to bytes

<a id="quixstreams.models.serializers.simple_types.DoubleSerializer"></a>

### DoubleSerializer

```python
class DoubleSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L155)

Serializes floats to bytes

<a id="quixstreams.models.serializers.json"></a>

## quixstreams.models.serializers.json

<a id="quixstreams.models.serializers.json.JSONSerializer"></a>

### JSONSerializer

```python
class JSONSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L32)

<a id="quixstreams.models.serializers.json.JSONSerializer.__init__"></a>

<br><br>

#### JSONSerializer.\_\_init\_\_

```python
def __init__(
    dumps: Callable[[Any], Union[str, bytes]] = default_dumps,
    schema: Optional[Mapping] = None,
    validator: Optional[Validator] = None,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    schema_registry_serialization_config: Optional[
        SchemaRegistrySerializationConfig] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L33)

Serializer that returns data in json format.


<br>
***Arguments:***

- `dumps`: a function to serialize objects to json.
Default - :py:func:`quixstreams.utils.json.dumps`
- `schema`: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/`jsonschema.validators.Draft202012Validator`).
Default - `None`
- `validator`: A jsonschema validator used to validate the data. Takes precedences over the schema.
Default - `None`
- `schema_registry_client_config`: If provided, serialization is offloaded to Confluent's JSONSerializer.
Default - `None`
- `schema_registry_serialization_config`: Additional configuration for Confluent's JSONSerializer.
Default - `None`
>***NOTE:*** `schema_registry_client_config` must also be set.

<a id="quixstreams.models.serializers.json.JSONDeserializer"></a>

### JSONDeserializer

```python
class JSONDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L119)

<a id="quixstreams.models.serializers.json.JSONDeserializer.__init__"></a>

<br><br>

#### JSONDeserializer.\_\_init\_\_

```python
def __init__(
    loads: Callable[[Union[bytes, bytearray]], Any] = default_loads,
    schema: Optional[Mapping] = None,
    validator: Optional[Validator] = None,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None
)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L120)

Deserializer that parses data from JSON


<br>
***Arguments:***

- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.
- `schema`: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/`jsonschema.validators.Draft202012Validator`).
Default - `None`
- `validator`: A jsonschema validator used to validate the data. Takes precedences over the schema.
Default - `None`
- `schema_registry_client_config`: If provided, deserialization is offloaded to Confluent's JSONDeserializer.
Default - `None`

<a id="quixstreams.models.serializers.avro"></a>

## quixstreams.models.serializers.avro

<a id="quixstreams.models.serializers.avro.AvroSerializer"></a>

### AvroSerializer

```python
class AvroSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L26)

<a id="quixstreams.models.serializers.avro.AvroSerializer.__init__"></a>

<br><br>

#### AvroSerializer.\_\_init\_\_

```python
def __init__(
    schema: Schema,
    strict: bool = False,
    strict_allow_default: bool = False,
    disable_tuple_notation: bool = False,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    schema_registry_serialization_config: Optional[
        SchemaRegistrySerializationConfig] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L27)

Serializer that returns data in Avro format.

For more information see fastavro [schemaless_writer](https://fastavro.readthedocs.io/en/latest/writer.html#fastavro._write_py.schemaless_writer) method.


<br>
***Arguments:***

- `schema`: The avro schema.
- `strict`: If set to True, an error will be raised if records do not contain exactly the same fields that the schema states.
Default - `False`
- `strict_allow_default`: If set to True, an error will be raised if records do not contain exactly the same fields that the schema states unless it is a missing field that has a default value in the schema.
Default - `False`
- `disable_tuple_notation`: If set to True, tuples will not be treated as a special case. Therefore, using a tuple to indicate the type of a record will not work.
Default - `False`
- `schema_registry_client_config`: If provided, serialization is offloaded to Confluent's AvroSerializer.
Default - `None`
- `schema_registry_serialization_config`: Additional configuration for Confluent's AvroSerializer.
Default - `None`
>***NOTE:*** `schema_registry_client_config` must also be set.

<a id="quixstreams.models.serializers.avro.AvroDeserializer"></a>

### AvroDeserializer

```python
class AvroDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L112)

<a id="quixstreams.models.serializers.avro.AvroDeserializer.__init__"></a>

<br><br>

#### AvroDeserializer.\_\_init\_\_

```python
def __init__(
    schema: Optional[Schema] = None,
    reader_schema: Optional[Schema] = None,
    return_record_name: bool = False,
    return_record_name_override: bool = False,
    return_named_type: bool = False,
    return_named_type_override: bool = False,
    handle_unicode_errors: str = "strict",
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None
)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L113)

Deserializer that parses data from Avro.

For more information see fastavro [schemaless_reader](https://fastavro.readthedocs.io/en/latest/reader.html#fastavro._read_py.schemaless_reader) method.


<br>
***Arguments:***

- `schema`: The Avro schema.
- `reader_schema`: If the schema has changed since being written then the new schema can be given to allow for schema migration.
Default - `None`
- `return_record_name`: If true, when reading a union of records, the result will be a tuple where the first value is the name of the record and the second value is the record itself.
Default - `False`
- `return_record_name_override`: If true, this will modify the behavior of return_record_name so that the record name is only returned for unions where there is more than one record. For unions that only have one record, this option will make it so that the record is returned by itself, not a tuple with the name.
Default - `False`
- `return_named_type`: If true, when reading a union of named types, the result will be a tuple where the first value is the name of the type and the second value is the record itself NOTE: Using this option will ignore return_record_name and return_record_name_override.
Default - `False`
- `return_named_type_override`: If true, this will modify the behavior of return_named_type so that the named type is only returned for unions where there is more than one named type. For unions that only have one named type, this option will make it so that the named type is returned by itself, not a tuple with the name.
Default - `False`
- `handle_unicode_errors`: Should be set to a valid string that can be used in the errors argument of the string decode() function.
Default - `"strict"`
- `schema_registry_client_config`: If provided, deserialization is offloaded to Confluent's AvroDeserializer.
Default - `None`

<a id="quixstreams.models.serializers.protobuf"></a>

## quixstreams.models.serializers.protobuf

<a id="quixstreams.models.serializers.protobuf.ProtobufSerializer"></a>

### ProtobufSerializer

```python
class ProtobufSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L24)

<a id="quixstreams.models.serializers.protobuf.ProtobufSerializer.__init__"></a>

<br><br>

#### ProtobufSerializer.\_\_init\_\_

```python
def __init__(
    msg_type: Message,
    deterministic: bool = False,
    ignore_unknown_fields: bool = False,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    schema_registry_serialization_config: Optional[
        SchemaRegistrySerializationConfig] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L25)

Serializer that returns data in protobuf format.

Serialisation from a python dictionary can have a significant performance impact. An alternative is to pass the serializer an object of the `msg_type` class.


<br>
***Arguments:***

- `msg_type`: protobuf message class.
- `deterministic`: If true, requests deterministic serialization of the protobuf, with predictable ordering of map keys
Default - `False`
- `ignore_unknown_fields`: If True, do not raise errors for unknown fields.
Default - `False`
- `schema_registry_client_config`: If provided, serialization is offloaded to Confluent's ProtobufSerializer.
Default - `None`
- `schema_registry_serialization_config`: Additional configuration for Confluent's ProtobufSerializer.
Default - `None`
>***NOTE:*** `schema_registry_client_config` must also be set.

<a id="quixstreams.models.serializers.protobuf.ProtobufDeserializer"></a>

### ProtobufDeserializer

```python
class ProtobufDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L110)

<a id="quixstreams.models.serializers.protobuf.ProtobufDeserializer.__init__"></a>

<br><br>

#### ProtobufDeserializer.\_\_init\_\_

```python
def __init__(
    msg_type: Message,
    use_integers_for_enums: bool = False,
    preserving_proto_field_name: bool = False,
    to_dict: bool = True,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    schema_registry_serialization_config: Optional[
        SchemaRegistrySerializationConfig] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L111)

Deserializer that parses protobuf data into a dictionary suitable for a StreamingDataframe.

Deserialisation to a python dictionary can have a significant performance impact. You can disable this behavior using `to_dict`, in that case the protobuf message will be used as the StreamingDataframe row value.


<br>
***Arguments:***

- `msg_type`: protobuf message class.
- `use_integers_for_enums`: If true, use integers instead of enum names.
Default - `False`
- `preserving_proto_field_name`: If True, use the original proto field names as
defined in the .proto file. If False, convert the field names to
lowerCamelCase.
Default - `False`
- `to_dict`: If false, return the protobuf message instead of a dict.
Default - `True`
- `schema_registry_client_config`: If provided, deserialization is offloaded to Confluent's ProtobufDeserializer.
Default - `None`
- `schema_registry_serialization_config`: Additional configuration for Confluent's ProtobufDeserializer.
Default - `None`
>***NOTE:*** `schema_registry_client_config` must also be set.

<a id="quixstreams.models.serializers.schema_registry"></a>

## quixstreams.models.serializers.schema\_registry

<a id="quixstreams.models.serializers.schema_registry.SchemaRegistryClientConfig"></a>

### SchemaRegistryClientConfig

```python
class SchemaRegistryClientConfig(BaseSettings)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/schema_registry.py#L22)

Configuration required to establish the connection with a Schema Registry.


<br>
***Arguments:***

- `url`: Schema Registry URL.
- `ssl_ca_location`: Path to CA certificate file used to verify the
Schema Registry's private key.
- `ssl_key_location`: Path to the client's private key (PEM) used for
authentication.
>***NOTE:*** `ssl_certificate_location` must also be set.
- `ssl_certificate_location`: Path to the client's public key (PEM) used
for authentication.
>***NOTE:*** May be set without `ssl_key_location` if the private key is
stored within the PEM as well.
- `basic_auth_user_info`: Client HTTP credentials in the form of
`username:password`.
>***NOTE:*** By default, userinfo is extracted from the URL if present.

<a id="quixstreams.models.serializers.schema_registry.SchemaRegistrySerializationConfig"></a>

### SchemaRegistrySerializationConfig

```python
class SchemaRegistrySerializationConfig(BaseSettings)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/schema_registry.py#L48)

Configuration that instructs Serializer how to handle communication with a

Schema Registry.


<br>
***Arguments:***

- `auto_register_schemas`: If True, automatically register the configured schema
with Confluent Schema Registry if it has not previously been associated with the
relevant subject (determined via subject.name.strategy). Defaults to True.
- `normalize_schemas`: Whether to normalize schemas, which will transform schemas
to have a consistent format, including ordering properties and references.
- `use_latest_version`: Whether to use the latest subject version for serialization.
>***NOTE:*** There is no check that the latest schema is backwards compatible with the
object being serialized. Defaults to False.
- `subject_name_strategy`: Callable(SerializationContext, str) -> str
Defines how Schema Registry subject names are constructed. Standard naming
strategies are defined in the confluent_kafka.schema_registry namespace.
Defaults to topic_subject_name_strategy.
- `skip_known_types`: Whether or not to skip known types when resolving
schema dependencies. Defaults to False.
- `reference_subject_name_strategy`: Defines how Schema Registry subject names
for schema references are constructed. Defaults to reference_subject_name_strategy.
- `use_deprecated_format`: Specifies whether the Protobuf serializer should
serialize message indexes without zig-zag encoding. This option must be explicitly
configured as older and newer Protobuf producers are incompatible.
If the consumers of the topic being produced to are using confluent-kafka-python <1.8,
then this property must be set to True until all old consumers have been upgraded.

<a id="quixstreams.models.serializers.quix"></a>

## quixstreams.models.serializers.quix

<a id="quixstreams.models.serializers.quix.QuixDeserializer"></a>

### QuixDeserializer

```python
class QuixDeserializer(JSONDeserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L76)

Handles Deserialization for any Quix-formatted topic.

Parses JSON data from either `TimeseriesData` and `EventData` (ignores the rest).

<a id="quixstreams.models.serializers.quix.QuixDeserializer.__init__"></a>

<br><br>

#### QuixDeserializer.\_\_init\_\_

```python
def __init__(loads: Callable[[Union[bytes, bytearray]], Any] = default_loads)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L83)


<br>
***Arguments:***

- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.split_values"></a>

<br><br>

#### QuixDeserializer.split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L100)

Each Quix message might contain data for multiple Rows.
This property informs the downstream processors about that, so they can
expect an Iterable instead of Mapping.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.deserialize"></a>

<br><br>

#### QuixDeserializer.deserialize

```python
def deserialize(model_key: str, value: Union[List[Mapping],
                                             Mapping]) -> Iterable[Mapping]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L153)

Deserialization function for particular data types (Timeseries or EventData).


<br>
***Arguments:***

- `model_key`: value of "__Q_ModelKey" message header
- `value`: deserialized JSON value of the message, list or dict


<br>
***Returns:***

Iterable of dicts

<a id="quixstreams.models.serializers.quix.QuixTimeseriesSerializer"></a>

### QuixTimeseriesSerializer

```python
class QuixTimeseriesSerializer(QuixSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L321)

Serialize data to JSON formatted according to Quix Timeseries format.

The serializable object must be dictionary, and each item must be of `str`, `int`,
`float`, `bytes` or `bytearray` type.
Otherwise, the `SerializationError` will be raised.

Input:
```python
{'a': 1, 'b': 1.1, 'c': "string", 'd': b'bytes', 'Tags': {'tag1': 'tag'}}
```

Output:
```json
{
    "Timestamps": [123123123],
    "NumericValues": {"a": [1], "b": [1.1]},
    "StringValues": {"c": ["string"]},
    "BinaryValues": {"d": ["Ynl0ZXM="]},
    "TagValues": {"tag1": ["tag"]}
}
```

<a id="quixstreams.models.serializers.quix.QuixEventsSerializer"></a>

### QuixEventsSerializer

```python
class QuixEventsSerializer(QuixSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L409)

Serialize data to JSON formatted according to Quix EventData format.
The input value is expected to be a dictionary with the following keys:
    - "Id" (type `str`, default - "")
    - "Value" (type `str`, default - ""),
    - "Tags" (type `dict`, default - {})

>***NOTE:*** All the other fields will be ignored.

Input:
```python
{
    "Id": "an_event",
    "Value": "any_string",
    "Tags": {"tag1": "tag"}}
}
```

Output:
```json
{
    "Id": "an_event",
    "Value": "any_string",
    "Tags": {"tag1": "tag"}},
    "Timestamp":1692703362840389000
}
```

