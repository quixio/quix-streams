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

**Note:** JSON Schema, Avro, and Protobuf serialization formats support integration with a Schema Registry. See the [Schema Registry](./schema-registry.md) page to learn more.

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
output_topic = app.topic('output', value_serializer='json', key_serializer='bytes')
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
The Avro serializer and deserializer need to be passed explicitly. Local schemaless Avro deserialization must include the schema; Schema Registry-backed deserialization can fetch the writer schema when `schema_registry_client_config` is provided.

> **WARNING**: The Avro serializer and deserializer require the `fastavro` library.  
> You can install Quix Streams with the necessary dependencies using:  
> `pip install quixstreams[avro]`

```python
from quixstreams import Application
from quixstreams.models.serializers.avro import AvroSerializer, AvroDeserializer

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
from quixstreams.models.serializers.protobuf import ProtobufSerializer, ProtobufDeserializer

from my_input_models_pb2 import InputProto
from my_output_models_pb2 import OutputProto

app = Application(broker_address='localhost:9092', consumer_group='consumer')
input_topic = app.topic('input', value_deserializer=ProtobufDeserializer(msg_type=InputProto))
output_topic = app.topic('output', value_serializer=ProtobufSerializer(msg_type=OutputProto))
```

By default, the Protobuf deserializer will deserialize the message to a Python dictionary. Doing this has a big performance impact. You can disable this behavior by initializing the deserializer with `to_dict` set to `False`. The Protobuf message object will then be used directly as the record value. This means column-based operations like `sdf['field']` and `sdf.drop()` will raise an error at runtime, as they require a Python dictionary. `sdf.fill()` will silently do nothing for non-dict values. You can still use `sdf.apply()` and `sdf.update()` with functions that accept the Protobuf object directly.

## MA Streaming Open Data

`MAStreamingDeserializer` is a built-in decoder for the MA Streaming Open Data v1 protocol (used by ATLAS / McLaren Applied telemetry feeds). It wraps the generic Protobuf deserializer and adds two-stage decoding (outer `Packet` envelope + dispatch to the matching inner-payload class), a per-sample row projection, and an optional filter.

> **WARNING:** Requires the `protobuf` extra: `pip install quixstreams[protobuf]`.

```python
from quixstreams import Application
from quixstreams.models.serializers import MAStreamingDeserializer

app = Application(broker_address="localhost:9092", consumer_group="masd")
topic = app.topic(
    "telemetry-in",
    value_deserializer=MAStreamingDeserializer(),
)
```

With no arguments the deserializer returns a list of flat row dicts (`packet_type`, `timestamp`, `name`, `value`, `validity`) for sample-bearing packets (`PeriodicData`, `RowData`, `SynchroData`) and an empty list for everything else. Fan rows out into the dataframe with `sdf.apply(lambda x: x, expand=True)`. `packet_type` is a discriminator column for `partitionBy("packet_type")` in lakehouse sinks.

Per-sample timestamps are reconstructed from the wire format: `start_time + i * interval` for `PeriodicData`, cumulative `intervals` for `SynchroData`, per-row `timestamps[]` for `RowData`. Each sample becomes one row with its own absolute timestamp.

### Filtering

`filter` is a mapping with two optional keys: `types` (packet-type allowlist) and `signals` (parameter_identifier allowlist). Both are pure allowlists — anything not listed is dropped.

```python
MAStreamingDeserializer(
    filter={
        "types": ["PeriodicData", "Configuration", "Event"],
        "signals": ["vCar", "nEngine"],
    },
)
```

Filtering runs on the protobuf binary, before any dict construction, so it stays fast even when most data is dropped.

### Output shape — asymmetric

Engineering (sample-bearing) packets and non-engineering packets use two different shapes — engineering is **flat with a `packet_type` discriminator column**, non-engineering is a **tagged-union dict** with the type as the root key. Both shapes flow through the same Kafka stream.

```python
# Kafka msg #1 — a PeriodicData packet (engineering, flat rows)
[
    {"packet_type": "PeriodicData", "timestamp": 1715000000, "name": "vCar",    "value": 312.4, "validity": "DATA_STATUS_VALID"},
    {"packet_type": "PeriodicData", "timestamp": 1715000010, "name": "vCar",    "value": 313.1, "validity": "DATA_STATUS_VALID"},
    {"packet_type": "PeriodicData", "timestamp": 1715000000, "name": "nEngine", "value": 11200, "validity": "DATA_STATUS_VALID"},
]

# Kafka msg #2 — a Configuration packet (non-engineering, tagged-union, content only)
[
    {"Configuration": {"channels": [...], "sample_rates": {...}}}
]

# Kafka msg #3 — an Event packet (non-engineering, tagged-union)
[
    {"Event": {"identifier": "PIT_ENTRY", "timestamp": 1715000023}}
]

# Kafka msg #4 — a NewSession packet (not in filter)
[]
```

Rationale: engineering data is high-throughput time-series and benefits from a flat schema (cheap row materialization, `partitionBy("packet_type")` works directly, fits any time-series store). Non-engineering payloads vary too much across types to flatten uniformly, so we keep them under the type as the root key and let downstream branch on it. Envelope fields (`session_key`, `is_essential`, `id`) are dropped from non-engineering inner dicts — sinks recover session context from upstream Kafka keys or topic names.

Downstream routing:

```python
def route(record):
    if "packet_type" in record:
        handle_engineering(record)               # flat row
    else:
        ((packet_type, inner),) = record.items()  # tagged-union
        handle_non_engineering(packet_type, inner)
```

When `filter=None` the non-engineering path returns `[]` (today's behavior); engineering still emits flat rows.

`build_table` is **on by default** — you don't need to pass it. The bundled `DEFAULT_BUILD_TABLE` projects sample-bearing packets into the five-column flat schema (`packet_type, timestamp, name, value, validity`) and is what makes signal filtering possible. You only touch `build_table` to opt out (`build_table=None` returns raw packet dicts) or to rename columns. Signal filtering requires the default (or any custom) `build_table` to be active and `decode_content=True`; combining `filter["signals"]` with `build_table=None` or `decode_content=False` raises `ValueError` at construction.

### Configuring the filter from environment variables

For deployment on Quix Cloud (or any container platform), the natural pattern is to drive the filter from env vars. A comma-separated string per axis works well:

```python
import os

def csv_env(name: str) -> list[str]:
    raw = os.environ.get(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]

filter_spec: dict = {}
types = csv_env("FILTER_TYPES")
if types:
    filter_spec["types"] = types
signals = csv_env("FILTER_SIGNALS")
if signals:
    filter_spec["signals"] = signals

MAStreamingDeserializer(filter=filter_spec)
```

Leave an env var empty (or unset) to drop that axis of the filter — the library rejects a literal empty list with `ValueError` as an anti-footgun guard, so the `if ...: include` pattern translates "empty env" into "axis not present" cleanly. If **both** axes end up empty, `filter_spec` is `{}` which the library accepts as a no-op alias for `filter=None`.

Runnable examples live in [`apps/masd_filter_examples/`](https://github.com/quixio/quix-streams/tree/main/apps/masd_filter_examples).
