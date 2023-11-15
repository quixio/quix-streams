<a id="quixstreams.models.serializers.quix"></a>

## quixstreams.models.serializers.quix

<a id="quixstreams.models.serializers.quix.QuixDeserializer"></a>

### QuixDeserializer

```python
class QuixDeserializer(JSONDeserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L70)

<a id="quixstreams.models.serializers.quix.QuixDeserializer.split_values"></a>

#### split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L93)

Each Quix message might contain data for multiple Rows.
This property informs the downstream processors about that, so they can
expect an Iterable instead of Mapping.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.deserialize"></a>

#### deserialize

```python
def deserialize(model_key: str, value: Union[List[Mapping],
                                             Mapping]) -> Iterable[Mapping]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L146)

Deserialization function for particular data types (Timeseries or EventData).

**Arguments**:

- `model_key`: value of "__Q_ModelKey" message header
- `value`: deserialized JSON value of the message, list or dict

**Returns**:

Iterable of dicts

<a id="quixstreams.models.serializers.quix.QuixTimeseriesSerializer"></a>

### QuixTimeseriesSerializer

```python
class QuixTimeseriesSerializer(QuixSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L311)

Serialize data to JSON formatted according to Quix Timeseries format.

The serializable object must be dictionary, and each item must be of `str`, `int`,
`float`, `bytes` or `bytearray` type.
Otherwise, the `SerializationError` will be raised.

Example of the format:
    Input:
    ```
        {'a': 1, 'b': 1.1, 'c': "string", 'd': b'bytes', 'Tags': {'tag1': 'tag'}}
    ```

    Output:
    ```
    {
        "Timestamps" [123123123],
        "NumericValues: {"a": [1], "b": [1.1]},
        "StringValues": {"c": ["string"]},
        "BinaryValues: {"d": ["Ynl0ZXM="]},
        "TagValues": {"tag1": ["tag']}
    }
    ```

<a id="quixstreams.models.serializers.quix.QuixEventsSerializer"></a>

### QuixEventsSerializer

```python
class QuixEventsSerializer(QuixSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/quix.py#L400)

Serialize data to JSON formatted according to Quix EventData format.
The input value is expected to be a dictionary with the following keys:
- "Id" (type `str`, default - "")
- "Value" (type `str`, default - ""),
- "Tags" (type `dict`, default - {})

Note: All the other fields will be ignored.

**Example**:

Input:

Output:
    ```
    {
        "Id": "an_event",
        "Value": "any_string"
        "Tags": {"tag1": "tag"}},
    }
    ```
    ```
    {
        "Id": "an_event",
        "Value": "any_string",
        "Tags": {"tag1": "tag"}},
        "Timestamp":1692703362840389000
    }
    ```

<a id="quixstreams.models.serializers.simple_types"></a>

## quixstreams.models.serializers.simple\_types

<a id="quixstreams.models.serializers.simple_types.wrap_serialization_error"></a>

#### wrap\_serialization\_error

```python
def wrap_serialization_error(func)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L29)

A decorator to wrap `confluent_kafka.SerializationError` into our own type.

<a id="quixstreams.models.serializers.simple_types.BytesDeserializer"></a>

### BytesDeserializer

```python
class BytesDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L44)

A deserializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.BytesSerializer"></a>

### BytesSerializer

```python
class BytesSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L55)

A serializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.IntegerDeserializer"></a>

### IntegerDeserializer

```python
class IntegerDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L84)

Deserializes bytes to integers.

A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.

<a id="quixstreams.models.serializers.simple_types.DoubleDeserializer"></a>

### DoubleDeserializer

```python
class DoubleDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L103)

Deserializes float to IEEE 764 binary64.

A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.

<a id="quixstreams.models.serializers.simple_types.IntegerSerializer"></a>

### IntegerSerializer

```python
class IntegerSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L135)

Serializes integers to bytes

<a id="quixstreams.models.serializers.simple_types.DoubleSerializer"></a>

### DoubleSerializer

```python
class DoubleSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/serializers/simple_types.py#L148)

Serializes floats to bytes

<a id="quixstreams.models.topics"></a>

## quixstreams.models.topics

<a id="quixstreams.models.topics.Topic"></a>

### Topic

```python
class Topic()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/topics.py#L58)

<a id="quixstreams.models.topics.Topic.name"></a>

#### name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/topics.py#L87)

Topic name

<a id="quixstreams.models.topics.Topic.row_serialize"></a>

#### row\_serialize

```python
def row_serialize(row: Row, key: Optional[Any] = None) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/topics.py#L93)

Serialize Row to a Kafka message structure

**Arguments**:

- `row`: Row to serialize
- `key`: message key to serialize, optional. Default - current Row key.

**Returns**:

KafkaMessage object with serialized values

<a id="quixstreams.models.topics.Topic.row_deserialize"></a>

#### row\_deserialize

```python
def row_deserialize(
        message: ConfluentKafkaMessageProto) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/8d2a3ed9581929ed2d75e40a48d48ae0b87ca920/quixstreams/models/topics.py#L116)

Deserialize incoming Kafka message to a Row.

**Arguments**:

- `message`: an object with interface of `confluent_kafka.Message`

**Returns**:

Row, list of Rows or None if the message is ignored.

