<a id="quixstreams.models.serializers.quix"></a>

## quixstreams.models.serializers.quix

<a id="quixstreams.models.serializers.quix.QuixDeserializer"></a>

### QuixDeserializer

```python
class QuixDeserializer(JSONDeserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/quix.py#L70)

Handles Deserialization for any Quix-formatted topic.

Parses JSON data from either `TimeseriesData` and `EventData` (ignores the rest).

<a id="quixstreams.models.serializers.quix.QuixDeserializer.__init__"></a>

<br><br>

#### QuixDeserializer.\_\_init\_\_

```python
def __init__(column_name: Optional[str] = None,
             loads: Callable[[Union[bytes, bytearray]], Any] = default_loads)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/quix.py#L77)


<br>
***Arguments:***

- `column_name`: if provided, the deserialized value will be wrapped into
dictionary with `column_name` as a key.
- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.split_values"></a>

<br><br>

#### QuixDeserializer.split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/quix.py#L97)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/quix.py#L150)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/quix.py#L315)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/quix.py#L404)

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

<a id="quixstreams.models.serializers.simple_types.BytesDeserializer"></a>

### BytesDeserializer

```python
class BytesDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L44)

A deserializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.BytesSerializer"></a>

### BytesSerializer

```python
class BytesSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L55)

A serializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.StringDeserializer"></a>

### StringDeserializer

```python
class StringDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L64)

<a id="quixstreams.models.serializers.simple_types.StringDeserializer.__init__"></a>

<br><br>

#### StringDeserializer.\_\_init\_\_

```python
def __init__(column_name: Optional[str] = None, codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L65)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L84)

Deserializes bytes to integers.

A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.

<a id="quixstreams.models.serializers.simple_types.DoubleDeserializer"></a>

### DoubleDeserializer

```python
class DoubleDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L103)

Deserializes float to IEEE 764 binary64.

A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.

<a id="quixstreams.models.serializers.simple_types.StringSerializer"></a>

### StringSerializer

```python
class StringSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L122)

<a id="quixstreams.models.serializers.simple_types.StringSerializer.__init__"></a>

<br><br>

#### StringSerializer.\_\_init\_\_

```python
def __init__(codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L123)

Serializes strings to bytes using the specified encoding.


<br>
***Arguments:***

- `codec`: string encoding

<a id="quixstreams.models.serializers.simple_types.IntegerSerializer"></a>

### IntegerSerializer

```python
class IntegerSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L135)

Serializes integers to bytes

<a id="quixstreams.models.serializers.simple_types.DoubleSerializer"></a>

### DoubleSerializer

```python
class DoubleSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/serializers/simple_types.py#L148)

Serializes floats to bytes

<a id="quixstreams.models.topics"></a>

## quixstreams.models.topics

<a id="quixstreams.models.topics.Topic"></a>

### Topic

```python
class Topic()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/topics.py#L58)

A representation of a Kafka topic and its expected data format via
designated key and value serializers/deserializers.

Typically created with an `app = quixstreams.app.Application()` instance via
`app.topic()`, and used by `quixstreams.dataframe.StreamingDataFrame`
instance.

<a id="quixstreams.models.topics.Topic.__init__"></a>

<br><br>

#### Topic.\_\_init\_\_

```python
def __init__(
    name: str,
    value_deserializer: Optional[DeserializerType] = None,
    key_deserializer: Optional[DeserializerType] = BytesDeserializer(),
    value_serializer: Optional[SerializerType] = None,
    key_serializer: Optional[SerializerType] = BytesSerializer())
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/topics.py#L68)

Can specify serialization that should be used when consuming/producing

to the topic in the form of a string name (i.e. "json" for JSON) or a
serialization class instance directly, like JSONSerializer().



<br>
***Example Snippet:***

<blockquote>
Specify an input and output topic for a `StreamingDataFrame` instance,
where the output topic requires adjusting the key serializer.

```python
from quixstreams.dataframe import StreamingDataFrame
from quixstreams.models import Topic, JSONSerializer

input_topic = Topic("input-topic", value_deserializer="json")
output_topic = Topic(
    "output-topic", key_serializer="str", value_serializer=JSONSerializer()
)
sdf = StreamingDataFrame(input_topic)
sdf.to_topic(output_topic)
```
</blockquote>


<br>
***Arguments:***

- `name`: topic name
- `value_deserializer`: a deserializer type for values
- `key_deserializer`: a deserializer type for keys
- `value_serializer`: a serializer type for values
- `key_serializer`: a serializer type for keys

<a id="quixstreams.models.topics.Topic.name"></a>

<br><br>

#### Topic.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/topics.py#L115)

Topic name

<a id="quixstreams.models.topics.Topic.row_serialize"></a>

<br><br>

#### Topic.row\_serialize

```python
def row_serialize(row: Row, key: Optional[Any] = None) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/topics.py#L121)

Serialize Row to a Kafka message structure


<br>
***Arguments:***

- `row`: Row to serialize
- `key`: message key to serialize, optional. Default - current Row key.


<br>
***Returns:***

KafkaMessage object with serialized values

<a id="quixstreams.models.topics.Topic.row_deserialize"></a>

<br><br>

#### Topic.row\_deserialize

```python
def row_deserialize(
        message: ConfluentKafkaMessageProto) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/834fc5d6bef64104a0732bcfe8db3dc49d2a9f40/quixstreams/models/topics.py#L144)

Deserialize incoming Kafka message to a Row.


<br>
***Arguments:***

- `message`: an object with interface of `confluent_kafka.Message`


<br>
***Returns:***

Row, list of Rows or None if the message is ignored.

