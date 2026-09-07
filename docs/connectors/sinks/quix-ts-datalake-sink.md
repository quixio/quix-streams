# Quix TS DataLake Sink

This sink writes Kafka batches to blob storage as Hive-partitioned Parquet files, with optional REST Catalog registration. It uses `quixportal` for unified blob storage access across Azure Blob, AWS S3, GCP Cloud Storage, MinIO, and local filesystems.

Supported backends (via `quixportal`):

- AWS S3
- Google Cloud Storage
- Azure Blob Storage
- MinIO
- Local filesystem

## How To Install

The dependencies for this sink are not included to the default `quixstreams` package.

To install them, run the following command:

```commandline
pip install quixstreams[quixdatalake]
```

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

## Partition Columns

`hive_columns` accepts two kinds of entry.

A plain entry is a **physical** partition. It becomes a real `key=value/` folder in storage, it groups the batch (each distinct value gets its own file), and the column is dropped from the Parquet data because its value is already in the path.

An entry prefixed with `~` is a **virtual** partition. It appears in the partition tree and is filterable, but it gets no folder and does not split files — one file keeps every value of it. The column stays in the Parquet data so queries can still filter rows by it.

```python
sink = QuixTSDataLakeSink(
    s3_prefix="data-lake/time-series",
    table_name="telemetry",
    hive_columns=["year", "month", "~driver"],
    timestamp_column="ts_ms",
)
```

That folders by `year=YYYY/month=MM/` and exposes `driver` as a third, virtual level:

```
data-lake/time-series/telemetry/year=2024/month=01/data_<uuid>.parquet        # every driver, one file
data-lake/time-series/telemetry/year=2024/month=01/.vidx/data_<uuid>.parquet  # its virtual index
```

Use a virtual column when you want a value to be navigable and filterable but do not want it to fragment storage — high-cardinality identifiers such as a driver, device, or session are the typical case. A physical partition on the same column would produce one small file per value per batch.

### The `.vidx` sidecar index

Each data file gets a virtual-index sidecar Parquet written to a `.vidx/` subfolder of that file's own Hive partition folder. The sidecar holds one row per distinct tuple of the virtual columns present in the file, with the file's physical partition values added as constant columns — so a reader gets full (physical + virtual) tuples from the content alone, with no `hive_partitioning` needed.

The sidecar schema is fixed per table: every configured virtual column, then every physical partition column, all stored as strings (a virtual column absent from a batch is written as `NULL`). That is what makes a single glob over many files' sidecars safe — Parquet readers refuse a glob whose files disagree on a column's type or presence, so per-file inference would break the read path on the first odd batch. It also keeps an identifier stable across batches: pandas upcasts an integer column to float as soon as a null joins it, but `42` and `42.0` both land as `"42"`.

```sql
-- Navigate: which drivers appear in January 2024? Skip files that lack the column.
SELECT DISTINCT driver
FROM read_parquet('s3://bucket/data-lake/time-series/telemetry/*/*/.vidx/*.parquet')
WHERE year = '2024' AND month = '01' AND driver IS NOT NULL;
```

Or, without the null filter, the equivalent tuple listing:

```sql
SELECT DISTINCT driver
FROM read_parquet('s3://bucket/data-lake/time-series/telemetry/*/*/.vidx/*.parquet')
WHERE year = '2024' AND month = '01';
```

Sidecars are co-located with the data so compaction rewrites a partition's data and its index together, but the `.vidx` folder name carries no `key=value`, so partition discovery skips it and it never joins the data set.

The sink writes one sidecar per data file, incrementally. On reindex or compaction the lakehouse collapses a folder's sidecars into a single consolidated `.vidx/index.parquet` holding the distinct tuples deduped across all of that folder's data files, so read paths should glob `.vidx/*.parquet` rather than assume either layout.

The index is navigation-only — it is not used for query pruning, so it is a hint rather than data. Sidecar uploads are awaited alongside the data files (the tree is queryable as soon as the batch is acknowledged), but a sidecar failure is logged and never raised, and the index self-heals on the next write.

### Catalog registration

The table's `partition_spec` is the **physical** columns only. That is the folder tree: the catalog validates every `add-files` call against it, and a file's `partition_values` can only ever carry physical values — a file holds every value of a virtual column, so it has no single value to register. The virtual levels are declared in table properties instead: `properties.expected_partitions` is the full tree order (physical + virtual) and `properties.virtual_partitions` lists the virtual entries. `properties.timestamp_column` and `properties.sort_column` are recorded alongside them.

These properties are sink-owned. On every start against an existing table the sink compares them with its configuration and rewrites them if they differ, preserving the table's location, schema, and every property it does not own (`file_count`, `created_by`, …). So adding a `~virtual` level or a `sort_column` to a table that already has data takes effect on the next start, and unsetting one removes it again. The physical set is validated and cannot change: a physical ↔ virtual swap of a column is rejected because it would corrupt the folder structure.

### Caveats

- A virtual column must be a field your records actually carry. Unlike a physical partition, it is not derived and not reconstructible from the path: reads use `hive_partitioning=true`, which rebuilds physical columns from `key=value/` folders but has nothing to rebuild a virtual column from. The column has to be in the Parquet data for `WHERE driver = 'HAM'` to resolve, and for the lakehouse's sidecar reindex to read its values back out.
- For that reason `year`, `month`, `day`, and `hour` are supported as **physical** partitions only — they are derived from `timestamp_column`, so a virtual `~hour` would be a permanently empty level. The constructor rejects `~year`/`~month`/`~day`/`~hour` with a `ValueError`, as it does a bare `~` and a column listed twice (including once physical and once virtual). Use `hour`, not `~hour`; time-range pruning is already provided by the per-file statistics below.
- A virtual column missing from a given batch is not an error. Its sidecar rows carry `NULL` for that column (the schema stays fixed), so navigation queries should add `IS NOT NULL` when listing a level's values.
- The `.vidx/` folders live *inside* the data partition folders, and Parquet globs do not skip dot-folders. A whole-table glob such as `.../telemetry/**/*.parquet` will pick the sidecars up as data — either a schema error or phantom rows, depending on the reader. Query through the catalog's file list (the manifest never contains sidecars), or use a glob whose depth matches the physical tree (`.../telemetry/*/*/*.parquet` for a two-level tree) so `.vidx/` is never descended into.

## File Statistics (Zone Maps)

The sink computes per-file min/max statistics and sends them to the REST Catalog with each file as `column_stats`. The query layer uses them to skip files whose value range cannot satisfy a `WHERE` or `ORDER BY` on the column.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stats_columns` | `Optional[List[str]]` | `None` | Columns to compute statistics for. `None` computes them for every numeric and timestamp column in each written file. Pass an explicit list to restrict the set, or an empty list to disable statistics entirely. |

Statistics are computed from the in-memory Arrow batch before serialization, so they cost a vectorized min/max over data that is already in memory — the Parquet footer is never re-read from storage. They are only computed when a `catalog_url` is configured; without a catalog there is nothing to consume them, so the sink skips the work.

Each entry records `type` (`numeric` or `timestamp`), `min`, `max`, `null_count`, and `value_count` (non-null rows). Integer, float, and decimal columns are reported as `numeric` with float bounds; timestamp and date columns as `timestamp` with ISO-8601 bounds.

Skipped automatically: the internal `__key` column, anything that is neither numeric nor temporal (strings, structs), all-null columns, and numeric columns whose minimum or maximum is `±inf` or `NaN` — neither gives a usable bound (and neither can be JSON-encoded), so omitting the column leaves the file unpruned rather than wrongly pruned. When no column qualifies, the `column_stats` key is omitted from the manifest entry entirely, so older catalogs simply ignore the absent field.

Numeric bounds are widened outward to the nearest representable float (`min` rounds down, `max` rounds up). This guarantees the stored range is a superset of the real one, so float rounding of large integers — nanosecond epochs beyond 2^53, for example — can only cost some pruning and can never wrongly skip a file that holds matching rows.

The default is deliberate: statistics are nearly free to compute here, and they benefit any range query. Restrict `stats_columns` when a table is wide enough that per-file, per-column stats rows become a meaningful cost in the catalog:

```python
sink = QuixTSDataLakeSink(
    s3_prefix="data-lake/time-series",
    table_name="wide_telemetry",
    hive_columns=["year", "month", "day"],
    timestamp_column="ts_ms",
    stats_columns=["ts_ms"],          # 400-column table: only index the time column
    catalog_url="https://iceberg-catalog.example.com",
)
```

## Sort Column

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sort_column` | `Optional[str]` | `None` | Column recorded on the table as `properties.sort_column`, which compaction orders files by. When `None`, the lakehouse falls back to `timestamp_column`. |

Compaction writes files ordered by this column so that `ORDER BY` and time-range queries can skip files and stream results instead of sorting the whole table. The sink records `properties.timestamp_column` on every table and adds `properties.sort_column` only when you set it explicitly, so the fallback stays available. Both are synced to an existing table on start (see [Catalog registration](#catalog-registration)), and unsetting `sort_column` removes it from the table again.

This parameter is table metadata for the lakehouse to act on. The sink itself does not reorder rows within a file.

`sort_column` cannot be a physical partition column — its value lives in the folder path and is stripped from every file, so no file could be sorted by it — and the constructor rejects that with a `ValueError`. A `~virtual` partition or any plain data column is fine. File skipping on the sort column also needs its per-file statistics: if you restrict `stats_columns`, keep `sort_column` and `timestamp_column` in the list, or the sink logs a warning at construction.

```python
sink = QuixTSDataLakeSink(
    s3_prefix="data-lake/time-series",
    table_name="sensor_readings",
    hive_columns=["year", "month", "day"],
    timestamp_column="ts_ms",
    sort_column="seq",                # order by sequence number, not wall clock
    catalog_url="https://iceberg-catalog.example.com",
)
```

## Per-Key Silence Detection

The sink can detect when individual Kafka message keys go quiet and fire a callback for each one. The canonical use case is sensor drop-out detection: if `sensor-a` stops publishing while `sensor-b` continues, the callback fires only for `sensor-a`.

One **stream** in this feature equals one Kafka message key. The threshold (`stream_timeout_ms`) is uniform across all keys observed by the sink.

All silence-detection logic is provided by the standalone [`StreamTimeoutTracker`](stream-timeout-tracker.md) — a stdlib-only, sink-agnostic module that any sink can compose. The sink holds one instance as `sink._timeout` and wires it through `add`/`flush`/`setup`/`cleanup`.

### Constructor parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stream_timeout_ms` | `Optional[int]` | `None` | Per-key silence threshold in milliseconds. Must be a positive integer when `on_stream_timeout` is set. |
| `on_stream_timeout` | `Optional[Callable[[Any], None]]` | `None` | Callback invoked once per silent key. Receives the raw Kafka message key as-is (`bytes`, `str`, `int`, … — whatever was passed to the sink). Exceptions are logged and swallowed. |

Both parameters must be set to a usable value (`stream_timeout_ms` a positive int, `on_stream_timeout` callable) for the feature to activate. Passing both as `None` disables the feature with zero overhead: no per-key dict is allocated, no background thread is started. Any other invalid combination raises `ValueError`.

### Fire-and-evict semantics

When a key has been silent for at least `stream_timeout_ms`:

1. An INFO line is logged: `Stream 'sensor-a' timed out (silence N ms >= threshold M ms)`.
2. `on_stream_timeout(key)` is called synchronously.
3. If the callback returns successfully, the key's tracking entry is evicted from the in-memory dict.

If the callback raises, the exception is logged and the key remains tracked so the callback can be retried on the next check cycle. If the key starts producing again after successful eviction, it is treated as a fresh stream with a new baseline and will fire again on its next silence.

A TTL safety sweep also evicts any tracking entry older than `3 × stream_timeout_ms` without firing (WARNING logged). This bounds the dict size in degenerate cases without any additional configuration.

### Fire latency

The silence check runs on a background daemon thread (started in `setup()`) plus at the end of every `flush()` as a secondary trigger. The background thread wakes every `max(100, min(1000, stream_timeout_ms // 5))` milliseconds. With a 60-second threshold, expect fire latency of roughly 60–61 seconds after the last message on a given key.

A flush-only cadence is insufficient: when the input topic goes fully silent, `BatchingSink` stops calling `flush()` because there are no batches to process. The background thread covers that gap.

### Restart and rebalance behaviour

Tracking state is in-memory only. On process restart or Kafka partition rebalance, the dict for affected keys is lost. Keys that remain dormant after a restart or rebalance will not fire for the current silence cycle; they resume normal tracking the next time they publish.

### Callback must not block

The callback can run either during sink `flush()` on the thread that drives the Kafka consumer heartbeat, or on the timeout tracker's background daemon thread. A blocking call inside the callback (for example, a synchronous producer `flush()`) can stop the consumer from polling when invoked during `flush()`, causing a heartbeat timeout and triggering a rebalance cascade. The callback must do bounded work and return promptly. If you need to produce a Kafka message from the callback, use a fire-and-forget `produce()` call — do not follow it with a synchronous `flush()`.

### Example: wiring a timeout-event producer

This pattern is how the `QuixLakeSinkEventCaller` deployment connects the sink to a Kafka side-channel topic. The callback receives the **raw** key (bytes in practice when consuming from Kafka), so the record key passes through to the side-channel topic byte-for-byte:

```python
import json
import time
from typing import Any
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

def on_stream_timeout(stream: Any) -> None:
    # `stream` is the raw Kafka key (bytes in practice). Decode once for the
    # JSON payload; the Kafka record key is pass-through.
    stream_str = stream.decode("utf-8", errors="replace") if isinstance(stream, bytes) else str(stream)
    # Fire-and-forget: no flush() here. The producer delivers asynchronously.
    side_producer.produce(
        topic=timeout_topic.name,           # workspace-prefixed name on Quix Cloud
        key=stream,                         # raw pass-through
        value=json.dumps({
            "ts_ms": int(time.time() * 1000),
            "stream": stream_str,
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

When `sensor-a` goes quiet for 60 seconds, one record arrives on `stream-timeouts` with Kafka key `sensor-a` (exact bytes of the original record key) and value:

```json
{"ts_ms": 1745311234567, "stream": "sensor-a", "event": "stream_timeout"}
```

Other keys continue flowing unaffected.

For deeper coverage of the tracker itself (concurrency, TTL sweep, composing it into a third-party sink, etc.), see the standalone [StreamTimeoutTracker](stream-timeout-tracker.md) page.

## Retrying Failures

A write has two phases, each retried up to three times on its own:

1. **Upload.** Every file of the batch (and every `.vidx` sidecar) is queued and then awaited. If any data upload fails, the attempt deletes whatever it *did* land before retrying — nothing has been registered yet, so the files are safe to remove — and the retry re-uploads the whole batch under fresh names. Retries therefore never accumulate orphan files or duplicate rows from half-written attempts. If the clean-up itself fails, the orphan keys are logged at `WARNING`.
2. **Manifest registration** (catalog configured). Only the `add-files` call is retried; the data is never re-uploaded because of a catalog error. If registration still fails, the files stay in storage — the catalog may already have recorded them before the error reached the sink — and the error is raised.

Errors that survive both retry loops are surfaced to the application's checkpoint machinery, which retries according to the configured processing guarantee.

## Delivery Guarantees

`QuixTSDataLakeSink` provides at-least-once guarantees. When the checkpoint replays a batch whose files landed but whose registration failed, the output may contain duplicate rows.
