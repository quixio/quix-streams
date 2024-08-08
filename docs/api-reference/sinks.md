<a id="quixstreams.sinks.influxdb3"></a>

## quixstreams.sinks.influxdb3

<a id="quixstreams.sinks.influxdb3.InfluxDB3Sink"></a>

### InfluxDB3Sink

```python
class InfluxDB3Sink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/influxdb3.py#L24)

<a id="quixstreams.sinks.influxdb3.InfluxDB3Sink.__init__"></a>

<br><br>

#### InfluxDB3Sink.\_\_init\_\_

```python
def __init__(token: str,
             host: str,
             organization_id: str,
             database: str,
             measurement: str,
             fields_keys: Iterable[str] = (),
             tags_keys: Iterable[str] = (),
             time_key: Optional[str] = None,
             time_precision: WritePrecision = WritePrecision.MS,
             include_metadata_tags: bool = False,
             batch_size: int = 1000,
             enable_gzip: bool = True,
             request_timeout_ms: int = 10_000,
             debug: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/influxdb3.py#L25)

A connector to sink processed data to InfluxDB v3.

It batches the processed records in memory per topic partition, converts
them to the InfluxDB format, and flushes them to InfluxDB at the checkpoint.

The InfluxDB sink transparently handles backpressure if the destination instance
cannot accept more data at the moment
(e.g., when InfluxDB returns an HTTP 429 error with the "retry_after" header set).
When this happens, the sink will notify the Application to pause consuming
from the backpressured topic partition until the "retry_after" timeout elapses.

>***NOTE***: InfluxDB3Sink can accept only dictionaries.
> If the record values are not dicts, you need to convert them to dicts before
> sinking.


<br>
***Arguments:***

- `token`: InfluxDB access token
- `host`: InfluxDB host in format "https://<host>"
- `organization_id`: InfluxDB organization_id
- `database`: database name
- `fields_keys`: a list of keys to be used as "fields" when writing to InfluxDB.
If present, it must not overlap with "tags_keys".
If empty, the whole record value will be used.
>***NOTE*** The fields' values can only be strings, floats, integers, or booleans.
Default - `()`.
- `tags_keys`: a list of keys to be used as "tags" when writing to InfluxDB.
If present, it must not overlap with "fields_keys".
These keys will be popped from the value dictionary
automatically because InfluxDB doesn't allow the same keys be
both in tags and fields.
If empty, no tags will be sent.
>***NOTE***: InfluxDB client always converts tag values to strings.
Default - `()`.
- `time_key`: a key to be used as "time" when writing to InfluxDB.
By default, the record timestamp will be used with "ms" time precision.
When using a custom key, you may need to adjust the `time_precision` setting
to match.
- `time_precision`: a time precision to use when writing to InfluxDB.
- `include_metadata_tags`: if True, includes record's key, topic,
and partition as tags.
Default - `False`.
- `batch_size`: how many records to write to InfluxDB in one request.
Note that it only affects the size of one write request, and not the number
of records flushed on each checkpoint.
Default - `1000`.
- `enable_gzip`: if True, enables gzip compression for writes.
Default - `True`.
- `request_timeout_ms`: an HTTP request timeout in milliseconds.
Default - `10000`.
- `debug`: if True, print debug logs from InfluxDB client.
Default - `False`.

<a id="quixstreams.sinks.csv"></a>

## quixstreams.sinks.csv

<a id="quixstreams.sinks.csv.CSVSink"></a>

### CSVSink

```python
class CSVSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/csv.py#L9)

<a id="quixstreams.sinks.csv.CSVSink.__init__"></a>

<br><br>

#### CSVSink.\_\_init\_\_

```python
def __init__(path: str,
             dialect: str = "excel",
             key_serializer: Callable[[Any], str] = str,
             value_serializer: Callable[[Any], str] = json.dumps)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/csv.py#L11)

A base CSV sink that writes data from all assigned partitions to a single file.

It's best to be used for local debugging.

Column format:
    (key, value, timestamp, topic, partition, offset)


<br>
***Arguments:***

- `path`: a path to CSV file
- `dialect`: a CSV dialect to use. It affects quoting and delimiters.
See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
Default - `"excel"`.
- `key_serializer`: a callable to convert keys to strings.
Default - `str()`.
- `value_serializer`: a callable to convert values to strings.
Default - `json.dumps()`.

<a id="quixstreams.sinks.base.sink"></a>

## quixstreams.sinks.base.sink

<a id="quixstreams.sinks.base.sink.BaseSink"></a>

### BaseSink

```python
class BaseSink(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L11)

This is a base class for all sinks.

Subclass it and implement its methods to create your own sink.

Note that Sinks are currently in beta, and their design may change over time.

<a id="quixstreams.sinks.base.sink.BaseSink.flush"></a>

<br><br>

#### BaseSink.flush

```python
@abc.abstractmethod
def flush(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L21)

This method is triggered by the Checkpoint class when it commits.

You can use `flush()` to write the batched data to the destination (in case of
a batching sink), or confirm the delivery of the previously sent messages
(in case of a streaming sink).

If flush() fails, the checkpoint will be aborted.

<a id="quixstreams.sinks.base.sink.BaseSink.add"></a>

<br><br>

#### BaseSink.add

```python
@abc.abstractmethod
def add(value: Any, key: Any, timestamp: int,
        headers: List[Tuple[str, HeaderValue]], topic: str, partition: int,
        offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L33)

This method is triggered on every new processed record being sent to this sink.

You can use it to accumulate batches of data before sending them outside, or
to send results right away in a streaming manner and confirm a delivery later
on flush().

<a id="quixstreams.sinks.base.sink.BaseSink.on_paused"></a>

<br><br>

#### BaseSink.on\_paused

```python
def on_paused(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L51)

This method is triggered when the sink is paused due to backpressure, when
the `SinkBackpressureError` is raised.

Here you can react to the backpressure events.

<a id="quixstreams.sinks.base.sink.BatchingSink"></a>

### BatchingSink

```python
class BatchingSink(BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L60)

A base class for batching sinks, that need to accumulate the data first before
sending it to the external destinatios.

Examples: databases, objects stores, and other destinations where
writing every message is not optimal.

It automatically handles batching, keeping batches in memory per topic-partition.

You may subclass it and override the `write()` method to implement a custom
batching sink.

<a id="quixstreams.sinks.base.sink.BatchingSink.write"></a>

<br><br>

#### BatchingSink.write

```python
@abc.abstractmethod
def write(batch: SinkBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L83)

This method implements actual writing to the external destination.

It may also raise `SinkBackpressureError` if the destination cannot accept new
writes at the moment.
When this happens, the accumulated batch is dropped and the app pauses the
corresponding topic partition.

<a id="quixstreams.sinks.base.sink.BatchingSink.add"></a>

<br><br>

#### BatchingSink.add

```python
def add(value: Any, key: Any, timestamp: int,
        headers: List[Tuple[str, HeaderValue]], topic: str, partition: int,
        offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L93)

Add a new record to in-memory batch.

<a id="quixstreams.sinks.base.sink.BatchingSink.flush"></a>

<br><br>

#### BatchingSink.flush

```python
def flush(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L115)

Flush an accumulated batch to the destination and drop it afterward.

<a id="quixstreams.sinks.base.sink.BatchingSink.on_paused"></a>

<br><br>

#### BatchingSink.on\_paused

```python
def on_paused(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L135)

When the destination is already backpressure, drop the accumulated batch.

