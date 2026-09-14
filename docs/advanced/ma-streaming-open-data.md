# MA Streaming Open Data

## What this is

The **MA Streaming Open Data v1** protocol is a Protobuf-based telemetry wire format published by Motion Applied (Apache 2.0). Every Kafka message is framed as an outer `Packet` envelope that carries a `type` discriminator string and an `content` field — raw bytes of a separate inner-payload protobuf message whose schema depends on that type. ATLAS-based data systems emit this format.

`MAStreamingDeserializer` handles both decoding stages in one call: it unwraps the outer `Packet`, dispatches the `content` bytes to the matching inner-payload class, and — for the three sample-bearing packet types (`PeriodicData`, `SynchroData`, `RowData`) — fans the samples out into flat row dicts ready for time-series sinks. The generic `ProtobufDeserializer` handles only the outer envelope; per-type dispatch, timestamp reconstruction, and row projection would still be your responsibility.

## Prerequisites

Install Quix Streams with the Protobuf extra:

```bash
pip install quixstreams[protobuf]
```

No additional build toolchain is required. The vendored `open_data_pb2.py` is committed alongside the library.

## Quickstart

The smallest working pipeline reads a topic carrying MA Streaming Open Data packets and prints every flat row:

```python
from quixstreams import Application
from quixstreams.models.serializers import MAStreamingDeserializer

app = Application(
    broker_address="localhost:9092",
    consumer_group="my-group",
)

topic = app.topic(
    name="telemetry-input",
    value_deserializer=MAStreamingDeserializer(),
    key_deserializer="str",
)

sdf = app.dataframe(topic)
sdf = sdf.apply(lambda rows: rows, expand=True)  # fan list → one row per sample
sdf = sdf.update(print)

app.run()
```

With default settings, each Kafka message carrying a `PeriodicData`, `SynchroData`, or `RowData` packet becomes a Python list of flat dicts — one dict per sample. `sdf.apply(..., expand=True)` fans that list into individual `StreamingDataFrame` rows. Non-sample-bearing packets (session management, configuration, events) produce an empty list and generate no downstream rows.

### Default output shape

A single `PeriodicData` Kafka message with two signals each recorded at two timestamps produces four rows:

```python
[
    {"packet_type": "PeriodicData", "timestamp": 1715000000, "name": "vCar",    "value": 312.4,   "validity": "DATA_STATUS_VALID"},
    {"packet_type": "PeriodicData", "timestamp": 1715000010, "name": "vCar",    "value": 313.1,   "validity": "DATA_STATUS_VALID"},
    {"packet_type": "PeriodicData", "timestamp": 1715000000, "name": "nEngine", "value": 11200.0, "validity": "DATA_STATUS_VALID"},
    {"packet_type": "PeriodicData", "timestamp": 1715000010, "name": "nEngine", "value": 11350.0, "validity": "DATA_STATUS_VALID"},
]
```

## Understanding the output shapes

The deserializer uses two different shapes depending on packet type. This asymmetry is intentional: engineering time-series data benefits from a flat schema that fits any column store; non-engineering payloads vary too much across types to flatten uniformly.

### Sample-bearing packets — flat rows

`PeriodicData`, `SynchroData`, and `RowData` carry time-series sample columns. The deserializer fans each sample into one flat dict. The default projection emits five columns:

| Column | Source | Description |
|---|---|---|
| `packet_type` | `Packet.type` | Discriminator. Useful for `partitionBy("packet_type")` in lakehouse sinks. |
| `timestamp` | Reconstructed per sample | Absolute timestamp (see below). |
| `name` | `data_format.parameter_identifiers.parameter_identifiers[i]` | Signal name (parameter identifier). |
| `value` | Sample value field | Measured value; type varies by sample list (double, int32, bool, string). |
| `validity` | `DataStatus` enum name | Status string such as `DATA_STATUS_VALID` or `DATA_STATUS_INVALID`. |

Timestamps are reconstructed from the wire format. `PeriodicData` stores samples as `start_time + i × interval`; `SynchroData` uses a cumulative sum of the `intervals` field; `RowData` carries per-row `timestamps`. You never reconstruct timestamps manually.

### Non-sample-bearing packets — tagged-union (when filter is active)

When a `filter` is set and a non-sample-bearing packet type survives the type allowlist, the deserializer emits it as a **tagged-union** record: a single-key dict whose key is the `Packet.type` string and whose value is the decoded inner-payload dict. Envelope fields (`session_key`, `is_essential`, `id`) are dropped from the inner value — sinks can recover session context from upstream Kafka keys or topic naming.

```python
# A NewSession packet inside a filtered pipeline:
[{"NewSession": {"session_key": "run-42", "session_identifier": "2024-05-07T10:00:00Z"}}]

# A Configuration packet:
[{"Configuration": {"channels": [...], "sample_rates": {...}}}]
```

Without a filter, non-sample-bearing packets return `[]` and do not appear in the stream at all.

### Raw packet shape (build_table=None)

Pass `build_table=None` to disable row projection. The deserializer returns the raw `MessageToDict` output for the decoded inner payload with no per-sample fan-out. Useful for diagnostic pipelines that need the full unmodified payload.

```python
MAStreamingDeserializer(build_table=None)
```

When `build_table=None` and `filter=None`, the deserializer returns a single dict. When `build_table=None` with a `filter` active, it returns a single-element tagged-union list.

## Filtering

`filter` accepts a dict with two optional keys: `"types"` and `"signals"`. Both are pure allowlists — anything not listed is dropped.

### Filter by packet type

```python
MAStreamingDeserializer(
    filter={"types": ["PeriodicData", "SynchroData", "NewSession"]},
)
```

All other packet types return `[]`. Non-sample-bearing types that survive (e.g. `NewSession` above) are returned as tagged-union records.

### Filter by signal name

```python
MAStreamingDeserializer(
    filter={"signals": ["vCar", "nEngine"]},
)
```

Signal filtering applies inside sample-bearing packets only. Rows whose `name` is not in the allowlist are skipped before any dict is constructed. A packet whose columns contain none of the listed signals produces `[]`.

### Combining both axes

```python
MAStreamingDeserializer(
    filter={
        "types": ["PeriodicData", "RowData", "NewSession"],
        "signals": ["vCar", "nEngine"],
    },
)
```

Type filtering runs first. Signal filtering runs inside surviving sample-bearing packets. Non-sample-bearing survivors (`NewSession`) return as tagged-union records; signal filtering does not apply to them.

### Output shape summary

| Condition | Per-message output |
|---|---|
| No filter — sample-bearing packet | `[{flat row}, ...]` |
| No filter — non-sample-bearing packet | `[]` |
| Filter active — sample-bearing, signals match | `[{"PeriodicData": {flat row}}, ...]` |
| Filter active — sample-bearing, signals don't match | `[]` |
| Filter active — non-sample-bearing, type in allowlist | `[{"NewSession": {...}}]` |
| Filter active — any packet, type not in allowlist | `[]` |

When a filter is active the deserializer always wraps output in a list of tagged-union records. Use `sdf.apply(lambda x: x, expand=True)` to fan the list out, then read the single root key to route by type:

```python
def route(record: dict):
    if "packet_type" in record:
        handle_sample(record)                        # flat row from default projection
    else:
        (packet_type, inner), = record.items()       # tagged-union
        handle_non_sample(packet_type, inner)
```

### Validation errors raised at construction

The `filter` argument is validated when you construct `MAStreamingDeserializer(...)`, not at message-processing time:

| Error | Cause |
|---|---|
| `TypeError: filter must be a Mapping` | Passed a string, list, or other non-dict value. |
| `TypeError: filter["types"] must be a sequence of strings, got str` | Passed a bare string instead of a list, e.g. `"types": "PeriodicData"`. |
| `ValueError: filter["types"] is empty` | Passed an empty list. Omit the key to disable that axis. |
| `ValueError: filter has unknown keys: ['signal']` | Typo — the key is `"signals"`, not `"signal"`. |
| `ValueError: filter["types"] contains unknown packet types: ['Periodicdata']` | Packet-type strings are case-sensitive. |
| `ValueError: filter["signals"] is set but build_table is disabled` | `build_table=None` and `filter["signals"]` are incompatible. |
| `ValueError: filter["signals"] is set but decode_content=False` | Signal filtering requires the inner payload to be decoded. |

### Driving the filter from environment variables

For containerised deployments, store each axis as a comma-separated environment variable:

```python
import os
from quixstreams.models.serializers import MAStreamingDeserializer

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

deserializer = MAStreamingDeserializer(filter=filter_spec or None)
```

Leaving an env var empty or unset omits that axis. The library rejects an explicit empty list with `ValueError`, so the `if ...:` guard converts "empty env var" into "axis not present" before the validator sees it. If both axes end up empty, `filter_spec` is `{}`, which the library accepts as a no-op alias for `filter=None`.

## Custom projections with build_table

The `build_table` parameter controls which columns appear in the flat rows produced for sample-bearing packets. The default projection emits five columns. You can subset or rename them.

### Supported columns

Only these seven source field names are recognised by the projection engine:

| Column | Wire source | Notes |
|---|---|---|
| `parameter_identifier` | `data_format.parameter_identifiers.parameter_identifiers[i]` | Signal name. Appears as `name` in the default projection. |
| `timestamp` | Reconstructed per sample | Absolute timestamp. |
| `value` | Sample value | Type varies by sample list kind. |
| `status` | `DataStatus` enum name | Appears as `validity` in the default projection. |
| `session_key` | `Packet.session_key` | Envelope field; same value for every row from the same packet. |
| `packet_type` | `Packet.type` | Envelope field; discriminator. |
| `is_essential` | `Packet.is_essential` | Envelope boolean. |

Note that the default projection's output columns (`name`, `validity`) are different from the source field names (`parameter_identifier`, `status`). When you pass `build_table` as a list you must use the source field names from the table above, not the default output names.

### List form — subset columns, keep source names

Pass a list of source field names. The output column names match the source field names:

```python
MAStreamingDeserializer(
    build_table=["parameter_identifier", "timestamp", "value"],
)
# Row: {"parameter_identifier": "vCar", "timestamp": 1715000000, "value": 312.4}
```

### Dict form — rename output columns

Pass a dict where keys are the output column names and values are lists of source field aliases. The first alias in the list that matches a supported column is used as the source:

```python
MAStreamingDeserializer(
    build_table={
        "signal":    ["parameter_identifier"],
        "ts_ns":     ["timestamp"],
        "raw_value": ["value"],
        "source":    ["packet_type"],
    },
)
# Row: {"signal": "vCar", "ts_ns": 1715000000, "raw_value": 312.4, "source": "PeriodicData"}
```

The dict-form alias list is evaluated at construction time to resolve the canonical source field; it is not used again per message.

### What happens with an unsupported column name

Passing an unrecognised name raises `ValueError` at construction, not at runtime:

```python
# Raises: ValueError: build_table contains unsupported column: 'signal_name'
MAStreamingDeserializer(build_table=["signal_name", "timestamp", "value"])
```

## Supported packet types

Every built-in `Packet.type` string, its inner protobuf message class, and the output it produces under the default configuration:

| `Packet.type` | Inner class | Sample-bearing | Default output |
|---|---|---|---|
| `PeriodicData` | `PeriodicDataPacket` | Yes | Flat rows (list) |
| `SynchroData` | `SynchroDataPacket` | Yes | Flat rows (list) |
| `RowData` | `RowDataPacket` | Yes | Flat rows (list) |
| `NewSession` | `NewSessionPacket` | No | `[]` (tagged-union with filter) |
| `EndOfSession` | `EndOfSessionPacket` | No | `[]` (tagged-union with filter) |
| `SessionInfo` | `SessionInfoPacket` | No | `[]` (tagged-union with filter) |
| `StreamStarted` | `StreamStartedPacket` | No | `[]` (tagged-union with filter) |
| `StreamStopped` | `StreamStoppedPacket` | No | `[]` (tagged-union with filter) |
| `Configuration` | `ConfigurationPacket` | No | `[]` (tagged-union with filter) |
| `Event` | `EventPacket` | No | `[]` (tagged-union with filter) |
| `Marker` | `MarkerPacket` | No | `[]` (tagged-union with filter) |
| `Error` | `ErrorPacket` | No | `[]` (tagged-union with filter) |
| `Metadata` | `MetadataPacket` | No | `[]` (tagged-union with filter) |
| `RawCANData` | `RawCANDataPacket` | No | `[]` (tagged-union with filter) |
| `AxisData` | `AxisDataPacket` | No | `[]` (tagged-union with filter) |
| `MapData` | `MapDataPacket` | No | `[]` (tagged-union with filter) |
| `DataFormatConfiguration` | `DataFormatConfigurationPacket` | No | `[]` (tagged-union with filter) |
| `DataFormatDefinition` | `DataFormatDefinitionPacket` | No | `[]` (tagged-union with filter) |
| `CoverageCursorInfo` | `CoverageCursorInfoPacket` | No | `[]` (tagged-union with filter) |
| `SystemStatus` | `SystemStatusMessage` | No | `[]` (tagged-union with filter) |

A type not in this table produces a one-time `WARNING` log entry and an empty list. It never raises.

## Recipes

### 1 — Filter to specific signals and write to a topic

Subscribe to two signals from periodic and synchro packets only, then forward the flat rows to a downstream topic for lakehouse ingestion.

```python
from quixstreams import Application
from quixstreams.models.serializers import MAStreamingDeserializer

app = Application(
    broker_address="localhost:9092",
    consumer_group="signal-sink",
)

topic = app.topic(
    name="telemetry-input",
    value_deserializer=MAStreamingDeserializer(
        filter={
            "types": ["PeriodicData", "SynchroData"],
            "signals": ["vCar", "nEngine"],
        },
        build_table=[
            "parameter_identifier",
            "timestamp",
            "value",
            "status",
            "session_key",
        ],
    ),
    key_deserializer="str",
)

out = app.topic("engineering-rows", value_serializer="json")

sdf = app.dataframe(topic)
sdf = sdf.apply(lambda rows: rows, expand=True)
# Each row: {"parameter_identifier": ..., "timestamp": ..., "value": ...,
#            "status": ..., "session_key": ...}
sdf.to_topic(out)

app.run()
```

### 2 — Route engineering rows and session events to separate topics

Use `sdf.filter()` to branch on whether a record is a flat engineering row or a tagged-union session event, then send each branch to its own output topic.

```python
from quixstreams import Application
from quixstreams.models.serializers import MAStreamingDeserializer

app = Application(
    broker_address="localhost:9092",
    consumer_group="router",
)

topic = app.topic(
    name="telemetry-input",
    value_deserializer=MAStreamingDeserializer(
        filter={
            "types": [
                "PeriodicData", "SynchroData", "RowData",
                "NewSession", "EndOfSession",
            ],
        },
    ),
    key_deserializer="str",
)

engineering_out = app.topic("engineering-rows", value_serializer="json")
session_out = app.topic("session-events", value_serializer="json")

sdf = app.dataframe(topic)
sdf = sdf.apply(lambda rows: rows, expand=True)

sdf.filter(lambda r: "packet_type" in r).to_topic(engineering_out)
sdf.filter(lambda r: "packet_type" not in r).to_topic(session_out)

app.run()
```

Engineering rows carry a `packet_type` key (flat projection). Tagged-union session records do not — their single root key is the packet type string (`"NewSession"`, `"EndOfSession"`).

### 3 — Capture parameter-identifier definitions with State

`DataFormatDefinitionPacket` carries the mapping from parameter identifiers to human-readable names. Store the definition in `State` when it arrives so subsequent sample rows can be enriched downstream.

```python
from quixstreams import Application, State
from quixstreams.models.serializers import MAStreamingDeserializer

app = Application(
    broker_address="localhost:9092",
    consumer_group="param-resolver",
)

topic = app.topic(
    name="telemetry-input",
    value_deserializer=MAStreamingDeserializer(
        filter={
            "types": ["PeriodicData", "DataFormatDefinition"],
        },
    ),
    key_deserializer="str",
)

sdf = app.dataframe(topic)
sdf = sdf.apply(lambda rows: rows, expand=True)

def store_or_pass(record: dict, state: State) -> dict:
    if "DataFormatDefinition" in record:
        state.set("format_def", record["DataFormatDefinition"])
    return record

sdf = sdf.apply(store_or_pass, stateful=True)

app.run()
```

### 4 — Register a custom (OEM extension) packet type

If your feed carries private extension packets not listed in the built-in type table, register them with `extra_content_types`. Built-in types take precedence on key collision; to override a built-in, subclass `MAStreamingDeserializer` and rebind `_content_types` after `super().__init__()`.

```python
from quixstreams.models.serializers import MAStreamingDeserializer
from my_oem_proto_pb2 import MyOemPacket

deserializer = MAStreamingDeserializer(
    extra_content_types={"MyOemType": MyOemPacket},
    filter={"types": ["PeriodicData", "MyOemType"]},
)
```

## Options reference

| Parameter | Type | Default | Description |
|---|---|---|---|
| `decode_content` | `bool` | `True` | When `True`, decode the inner payload bytes into a typed dict (Stage 2). When `False`, leave `content` as the Stage-1 base64 string — useful for envelope-only diagnostics. |
| `extra_content_types` | `dict[str, type[Message]]` | `None` | Additional `Packet.type → protobuf class` mappings for OEM extension packets. Built-in types win on key collision. |
| `use_integers_for_enums` | `bool` | `False` | When `True`, enum fields are emitted as integers instead of string names (e.g. `2` instead of `"DATA_STATUS_VALID"`). Forwarded to `MessageToDict` for both decoding stages. |
| `build_table` | list or dict or `None` | `DEFAULT_BUILD_TABLE` | Column projection for sample-bearing packets. Pass a list of source field names or a dict of output name → source alias list. Pass `None` to disable projection and return raw packet dicts. |
| `filter` | `dict` or `None` | `None` | Packet-type and signal allowlists. Two optional keys: `"types"` (list of `Packet.type` strings) and `"signals"` (list of parameter identifier strings). `{}` is accepted as a no-op alias for `None`. |
| `schema_registry_client_config` | `SchemaRegistryClientConfig` | `None` | Schema Registry connection config for the Stage-1 envelope decode. Stage 2 uses raw inner bytes with no Schema Registry framing. |
| `schema_registry_serialization_config` | `SchemaRegistrySerializationConfig` | `None` | Schema Registry serialization config for Stage 1. |

## Maintenance

### Regenerating the protobuf bindings

The vendored `open_data_pb2.py` is generated from `proto/open_data.proto`. End users never need to run this step. Library maintainers should regenerate after bumping the bundled schema to a new protocol revision:

```bash
pip install grpcio-tools
python -m quixstreams.models.serializers.ma_streaming_open_data.regenerate
```

The script uses the `grpc_tools.protoc` compiler bundled with `grpcio-tools >= 1.62` — no system-level `protoc` binary is required. The generated file is committed alongside the library so that `pip install quixstreams[protobuf]` works without a build toolchain.

After generation, check the `# Protobuf Python Version:` comment at the top of `open_data_pb2.py`. If it differs noticeably from the committed version, verify that `grpcio-tools >= 1.62` is installed before committing.

### Protocol attribution

The `open_data.proto` schema is sourced from the [MA.DataPlatforms.Protocol](https://github.com/Software-Products/MA.DataPlatforms.Protocol) repository published by Motion Applied, licensed under the Apache License 2.0. `MAStreamingDeserializer.protocol_version` is `"v1"`, mirroring the `ma.streaming.open_data.v1` package name in the schema.

## Troubleshooting

### Non-sample packets produce empty lists and never reach downstream steps

**Symptom:** You consume a topic carrying `NewSession` or `Configuration` packets. Nothing flows through the pipeline; no errors appear.

**Cause:** Without a `filter`, non-sample-bearing packets always return `[]`. The default projection is optimised for time-series data; session and configuration events are intentionally excluded.

**Fix:** Add a `filter` that includes the packet types you need. They will appear as tagged-union records:

```python
MAStreamingDeserializer(
    filter={"types": ["PeriodicData", "NewSession", "Configuration"]},
)
```

### A signal I expect is missing from the output

**Symptom:** You set `filter={"signals": ["vCar"]}` but rows for that signal never appear.

**Cause:** Signal names are matched exactly and are case-sensitive. If the `parameter_identifier` on the wire is `"v_car"` or `"VCAR"`, the filter drops it.

**Fix:** Temporarily remove the `signals` axis, run the pipeline, and print the `name` column from a few rows to see the exact wire name. Correct the name in your filter and restore it.

### `ValueError` at construction when combining `build_table=None` and `filter["signals"]`

**Symptom:** `MAStreamingDeserializer(build_table=None, filter={"signals": ["vCar"]})` raises immediately with a `ValueError`.

**Cause:** Signal filtering is implemented inside the row-projection fast path. Without a `build_table`, the deserializer has no mechanism to compare sample names against the allowlist.

**Fix:** Keep the default `build_table` (omit the parameter) or pass a custom list or dict. If you need `build_table=None`, use a `"types"`-only filter.

### Unknown packet type warning in logs

**Symptom:** Log contains `WARNING: Unknown Packet.type='MyCustomType'; cannot project to table (this warning is logged once per type)`. The deserializer returns `[]` for that type.

**Cause:** The feed carries a packet type not in the built-in type map (`CONTENT_TYPES`). Without a protobuf schema for the inner payload, the deserializer cannot decode it.

**Fix:** Obtain the protobuf schema for the custom type and register it:

```python
from my_proto_pb2 import MyCustomPacket

MAStreamingDeserializer(
    extra_content_types={"MyCustomType": MyCustomPacket},
)
```

If no schema is available, the inner bytes cannot be decoded and `[]` is the correct result.

### Message keys arrive as bytes instead of strings

**Symptom:** Keys appear as `b"run-42"` instead of `"run-42"`, or the pipeline raises a deserialization error on message keys.

**Cause:** The default `key_deserializer` is `"bytes"`. MA Streaming Open Data topics typically use string session identifiers as keys.

**Fix:** Pass `key_deserializer="str"` to `app.topic()`:

```python
topic = app.topic(
    name="telemetry-input",
    value_deserializer=MAStreamingDeserializer(),
    key_deserializer="str",
)
```
