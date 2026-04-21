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

## Stream-Finished Detection

The sink can watch specific message keys for silence and fire a callback when a key has been quiet for longer than a per-key threshold. Useful for sensor drop-out detection, session-close notifications, and end-of-batch signalling.

Pass `stream_finished` as a mapping of `key → (timeout_ms, callback)`:

```python
def on_silence(key: str) -> None:
    print(f"Stream {key} finished")

sink = QuixTSDataLakeSink(
    s3_prefix="data-lake/time-series",
    table_name="sensor_readings",
    workspace_id="",
    hive_columns=["year", "month", "day"],
    timestamp_column="ts_ms",
    stream_finished={
        "sensor-a": (15_000, on_silence),
        "sensor-b": (60_000, on_silence),
    },
)
```

Semantics:

- **Opt-in per key.** Only keys listed in the mapping are tracked; everything else flows through with zero overhead.
- **Fires once per silence window.** When a tracked key is silent for at least `timeout_ms`, the callback runs once. Subsequent messages for that key re-arm the timer; the next silence fires again.
- **Checks at flush time.** The silence check runs during the sink's periodic flush, so actual fire latency is at most `timeout_ms + commit_interval`.
- **Callback signature.** `Callable[[str], None]` — receives the key that went silent. Exceptions raised inside the callback are logged and swallowed; they do not crash the sink.
- **Disabled by default.** Omitting the argument, passing `None`, or passing `{}` all disable tracking with no state allocated.

## Retrying Failures

`QuixTSDataLakeSink` will surface write failures to the application's checkpoint machinery, which retries according to the configured processing guarantee.

## Delivery Guarantees

`QuixTSDataLakeSink` provides at-least-once guarantees. On retry after a partial failure, the output may contain duplicate rows.
