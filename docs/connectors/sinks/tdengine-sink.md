# TDengine Sink

!!! info
    
    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

TDengine is an open source time series database optimized for IoT, connected vehicles, and industrial applications.

Quix Streams provides a sink to write processed data to TDengine.

## How To Install
The dependencies for this sink are not included in the default `quixstreams` package.

To install them, run the following command:

```commandline
pip install quixstreams[tdengine]
```

## How To Use

To sink data to TDengine, you need to create an instance of `TDengineSink` and pass it to the `StreamingDataFrame.sink()` method:

```python

app = Application(broker_address="127.0.0.1:9092")
topic = app.topic("cpu_topic")

def generate_subtable_name(row):
    return f"cpu_{row['cpu_id']}"

tdengine_sink = TDengineSink(
    host="http://localhost:6041",
    username= "root",
    password= "taosdata",
    database="test_cpu",
    supertable="cpu",
    subtable = generate_subtable_name,
    fields_keys=["percent"],
    tags_keys=["cpu_id", "host", "region"],

)

sdf = app.dataframe(topic)
sdf.sink(tdengine_sink)

if __name__ == '__main__':
    app.run()

```

## Configuration

### Parameter Overview
TDengineSink accepts the following configuration parameters:

- `host` - TDengine host in format `"http[s]://<host>[:<port>]"` (e.g., `"http://localhost:6041"`).
- `database` - Database name (must exist before use).
- `supertable` - Supertable name as a string, or a callable that receives the current message data and returns a string.
- `subtable` - Subtable name as a string, or a callable that receives the current message data and returns a string. If empty, a hash value will be generated from the data as the subtable name.
- `fields_keys` - Iterable (list) of strings used as TDengine "fields", or a callable that receives the current message data and returns an iterable of strings. If present, must not overlap with `tags_keys`. If empty, the whole record value will be used. Default: `()`.
- `tags_keys` - Iterable (list) of strings used as TDengine "tags", or a callable that receives the current message data and returns an iterable of strings. If present, must not overlap with `fields_keys`. Given keys are popped from the value dictionary since the same key cannot be both a tag and field. If empty, no tags will be sent. Default: `()`.
- `time_setter` - Optional column name to use as the timestamp for TDengine. Also accepts a callable that receives the current message data and returns either the desired time or `None` (use default). The time can be an `int`, `string` (RFC3339 format), or `datetime`. The time must match the `time_precision` argument if not a `datetime` object, else raises. Default: `None`.
- `time_precision` - Time precision for the timestamp. One of: `"ms"`, `"ns"`, `"us"`, `"s"`. Default: `"ms"`.
- `allow_missing_fields` - If `True`, skip missing field keys instead of raising `KeyError`. Default: `False`.
- `include_metadata_tags` - If `True`, includes the record's key, topic, and partition as tags. Default: `False`.
- `convert_ints_to_floats` - If `True`, converts all integer values to floats. Default: `False`.
- `batch_size` - Number of records to write to TDengine in one request. Only affects the size of one write request, not the number of records flushed on each checkpoint. Default: `1000`.
- `enable_gzip` - If `True`, enables gzip compression for writes. Default: `True`.
- `request_timeout_ms` - HTTP request timeout in milliseconds. Default: `10000`.
- `verify_ssl` - If `True`, verifies the SSL certificate. Default: `True`.
- `username` - TDengine username. Default: `""` (empty string).
- `password` - TDengine password. Default: `""` (empty string).
- `token` - TDengine cloud token. Default: `""` (empty string). Either `token` or `username` and `password` must be provided.
- `on_client_connect_success` - Optional callback after successful client authentication.
- `on_client_connect_failure` - Optional callback after failed client authentication (should accept the raised Exception as an argument).

See the [What data can be sent to TDengine](#what-data-can-be-sent-to-tdengine) section for more info on `fields_keys` and `tags_keys`.


### Parameter Examples


####  `subtable`
Accepts either:
- Static name (string)
- Dynamic generator (callable)

**Behavior:**
- If empty: Generates hash from data as subtable name
- Callable receives message data (dict) and returns string

#### Examples:
```python
# Static name
subtable = "meters"

# Dynamic name
def generate_subtable(row: dict) -> str:
    """Create subtable name from data"""
    return f"meters_{row['id']}"
    
subtable = generate_subtable
```


### supertable
Same interface as subtable:

- String for static name
- Callable for dynamic name

### Database Validation
- Verifies database exists during setup

- Raises error if missing: `Database 'your_database' does not exist`


### Expected Behavior
1. Prerequisite: Database must exist before operation

    Error if missing: Database 'your_database' does not exist

2. After successful setup:

- Message example sent to Kafka:

```json
{"host":"192.168.1.98","region":"EU","percent":12.5,"cpu_id":1}
```

- Creates the following tables in TDengine:

```text
taos> show stables;
          stable_name           |
=================================
 cpu                            |

taos> show tables;
           table_name           |
=================================
 cpu_1                          |
```

- Data verification:

```text
taos> select * from cpu;
           _ts           |  percent  |  cpu_id  |       host        |  region  |
========================================================================
 2025-06-27 15:02:17.125 |     12.5  |    1     | 192.168.1.98      |   EU     |
```



## Testing Locally  

Rather than connect to a hosted TDengine instance, you can alternatively test your 
application using a local instance of TDengine using Docker:  

1. Execute in terminal:  

    ```bash
    docker run --rm -d --name tdengine \
    -p 6030:6030 \
    -p 6041:6041 \
    -p 6043-6060:6043-6060 \
    -e TZ=America/New_York \
    -e LC_ALL=C.UTF-8 \
    tdengine/tdengine:latest
    ```

2. Use the following authentication settings for `TDengineSink` to connect:  

    ```python
    TDengineSink(
        host="http://localhost:6041",
        username="root",
        password="taosdata",
        ...
    )
    ```

3. When finished, execute in terminal:

    ```bash
    docker stop tdengine
    ```