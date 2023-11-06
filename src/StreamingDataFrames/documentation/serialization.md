# Serialization and Deserialization (SERDES)

SERDES simply refers to how you pack (serialize) or unpack (deserialize) your data 
when publishing to or reading from a topic. With our `Application.topic()`, we use the `JSON`
format by default.

There are numerous ways to SERDES your data, and we provide some plain formats for you 
to select from, including `bytes`, `string`, `integer`, etc.

We also plan on including other popular ones like `PROTOBUF` in the near future.

<br>

## Using a SERDES

SERDES are used by providing the appropriate SERDES class, or string shorthand of it,
to a `Topic` object (or, to `Application.topic()` which forwards all the same arguments 
to `Topic`).

You can select them like so:

```python
from streamingdataframes.models.serializers import (
    JSONDeserializer
)
from streamingdataframes.app import Application

app = Application()
topic_in = app.topic(
    "my_input_topic", value_deserializer=JSONDeserializer(),
)
topic_out = app.topic(
    "my_output_topic", value_serializer="json",
)
```


<br>

## Picking a SERDES

You can find all available serializers in `streamingdataframes.models.serializers`.


<br>

## Message Key and Value SERDES

Most people refer to serializing the message _value_ when discussing SERDES. However,
you can also SERDES message _keys_, but you probably won't need to.

Should you need it, they use the same SERDES'es classes:

```python
topic = app.topic("my_topic", key_serializer="str")
```
