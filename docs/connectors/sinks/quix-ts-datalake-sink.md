# Quix TS DataLake Sink

This sink writes Kafka batches to blob storage as Hive-partitioned Parquet files, with optional REST Catalog registration. It uses `quixportal` for unified blob storage access across Azure Blob, AWS S3, GCP Cloud Storage, MinIO, and local filesystems.

Supported backends (via `quixportal`):

- AWS S3
- Google Cloud Storage
- Azure Blob Storage
- MinIO
- Local filesystem

## How To Install

`QuixTSDataLakeSink` ships in the default `quixstreams` package — no extra install step.

## How To Use

Create an instance of `QuixTSDataLakeSink` and pass it to `StreamingDataFrame.sink()`.

For the full parameter description, see the [Quix TS DataLake Sink API](../../api-reference/sinks.md#quixtsdatalakesink) page.

```python
from quixstreams import Application
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink

sink = QuixTSDataLakeSink(
    s3_prefix="data-lake/time-series",
    table_name="sensor_readings",
    workspace_id="",                      # auto-injected on Quix Cloud
    hive_columns=["year", "month", "day"],
    timestamp_column="ts_ms",
    catalog_url="https://iceberg-catalog.example.com",
    catalog_auth_token="<token>",
    auto_discover=True,
)

app = Application(broker_address="localhost:9092", auto_offset_reset="earliest")
topic = app.topic("sensor_readings")

sdf = app.dataframe(topic=topic)
sdf.sink(sink)

if __name__ == "__main__":
    app.run()
```

Records must be dictionaries. If your values are not dicts, convert them before sinking.

## How It Works

`QuixTSDataLakeSink` is a batching sink. It buffers processed records in memory per topic partition, serializes each batch into Parquet with Hive-style partition paths (`year=YYYY/month=MM/day=DD/...`), uploads the file to blob storage, and — if a REST Catalog is configured — registers the file in the table manifest. Files are flushed at every checkpoint (controlled by `Application(commit_interval=...)`).

Blob credentials are read automatically from the `Quix__BlobStorage__Connection__Json` environment variable when running on Quix Cloud; for local runs, the filesystem is inferred from the `quixportal` configuration.

## Per-Key Silence Detection

The sink can detect when individual Kafka message keys go quiet and fire a callback for each one. The canonical use case is sensor drop-out detection: if `sensor-a` stops publishing while `sensor-b` continues, the callback fires only for `sensor-a`.

One **stream** in this feature equals one Kafka message key. The threshold (`stream_timeout_ms`) is uniform across all keys observed by the sink.

### Constructor parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stream_timeout_ms` | `Optional[int]` | `None` | Per-key silence threshold in milliseconds. Must be a positive integer. The feature is disabled when this is `None`, `0`, or negative. |
| `on_stream_timeout` | `Optional[Callable[[str], None]]` | `None` | Callback invoked once per silent key. Receives the Kafka message key as a UTF-8 string. Exceptions are logged and swallowed. |

Both parameters must be set to a usable value (`stream_timeout_ms` a positive int, `on_stream_timeout` callable) for the feature to activate. Any other combination disables the feature with zero overhead: no per-key dict is allocated, no background thread is started.

### Fire-and-evict semantics

When a key has been silent for at least `stream_timeout_ms`:

1. The key's tracking entry is evicted from the in-memory dict.
2. `on_stream_timeout(key)` is called synchronously.
3. An INFO line is logged: `Stream 'sensor-a' timed out (silence N ms >= threshold M ms)`.

The eviction happens before the callback, so a callback that raises still counts as fired — the same key will not fire again for the current silence period. If the key starts producing again after eviction, it is treated as a fresh stream with a new baseline and will fire again on its next silence.

A TTL safety sweep also evicts any tracking entry older than `3 × stream_timeout_ms` without firing (WARNING logged). This bounds the dict size in degenerate cases without any additional configuration.

### Fire latency

The silence check runs on a background daemon thread (started in `setup()`) plus at the end of every `flush()` as a secondary trigger. The background thread wakes every `max(100, min(1000, stream_timeout_ms // 5))` milliseconds. With a 60-second threshold, expect fire latency of roughly 60–61 seconds after the last message on a given key.

A flush-only cadence is insufficient: when the input topic goes fully silent, `BatchingSink` stops calling `flush()` because there are no batches to process. The background thread covers that gap.

### Restart and rebalance behaviour

Tracking state is in-memory only. On process restart or Kafka partition rebalance, the dict for affected keys is lost. Keys that remain dormant after a restart or rebalance will not fire for the current silence cycle; they resume normal tracking the next time they publish.

### Callback must not block

The callback runs on the sink's flush thread, which is the same thread that drives the Kafka consumer heartbeat. A blocking call inside the callback (for example, a synchronous producer `flush()`) will stop the consumer from polling, causing a heartbeat timeout and triggering a rebalance cascade. The callback must do bounded work and return promptly. If you need to produce a Kafka message from the callback, use a fire-and-forget `produce()` call — do not follow it with a synchronous `flush()`.

### Example: wiring a timeout-event producer

This pattern is how the `QuixLakeSinkEventCaller` deployment connects the sink to a Kafka side-channel topic:

```python
import json
import time
from quixstreams import Application
from quixstreams.sinks.core.quix_ts_datalake_sink import QuixTSDataLakeSink

app = Application(broker_address="localhost:9092", commit_interval=5)
side_producer = app.get_producer()

# Register the output topic so the producer knows where to deliver.
timeout_topic = app.topic(
    "stream-timeouts",
    key_serializer="bytes",
    value_serializer="bytes",
)

def on_stream_timeout(stream: str) -> None:
    # Fire-and-forget: no flush() here. The producer delivers asynchronously.
    side_producer.produce(
        topic=timeout_topic.name,           # workspace-prefixed name on Quix Cloud
        key=stream.encode(),
        value=json.dumps({
            "ts_ms": int(time.time() * 1000),
            "stream": stream,
            "event": "stream_timeout",
        }).encode(),
    )

sink = QuixTSDataLakeSink(
    s3_prefix="data-lake/time-series",
    table_name="sensor_readings",
    workspace_id="",
    hive_columns=["year", "month", "day"],
    timestamp_column="ts_ms",
    stream_timeout_ms=60_000,       # fire after 60 s of silence per key
    on_stream_timeout=on_stream_timeout,
)

sdf = app.dataframe(topic=app.topic("sensor_readings"))
sdf.sink(sink)

if __name__ == "__main__":
    with side_producer:
        app.run()
```

When `sensor-a` goes quiet for 60 seconds, one record arrives on `stream-timeouts` with Kafka key `sensor-a` (raw UTF-8 bytes) and value:

```json
{"ts_ms": 1745311234567, "stream": "sensor-a", "event": "stream_timeout"}
```

Other keys continue flowing unaffected.

## Retrying Failures

`QuixTSDataLakeSink` will surface write failures to the application's checkpoint machinery, which retries according to the configured processing guarantee.

## Delivery Guarantees

`QuixTSDataLakeSink` provides at-least-once guarantees. On retry after a partial failure, the output may contain duplicate rows.
