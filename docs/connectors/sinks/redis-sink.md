# Redis Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

Redis is an in-memory database that persists on disk.

Quix Streams provides a sink to write processed data to Redis.

## How To Install

The dependencies for this sink are not included to the default `quixstreams` package.

To install them, run the following command:

```commandline
pip install quixstreams[redis]
```

## How To Use

To sink data to Redis, you need to create an instance of `RedisSink` and pass
it to the `StreamingDataFrame.sink()` method:

```python
import json

from quixstreams import Application
from quixstreams.sinks.community.redis import RedisSink

app = Application(
    broker_address="localhost:9092",
    auto_offset_reset="earliest",
    consumer_group="consumer-group",
)

topic = app.topic("topic-name")

# Initialize a sink
redis_sink = RedisSink(
    host="<Redis host>",
    port="<Redis port>",
    db="<Redis db>",
    value_serializer=json.dumps,
    key_serializer=None,
    password=None,
    socket_timeout=30.0,
)

sdf = app.dataframe(topic)
sdf.sink(redis_sink)

if __name__ == '__main__':
    app.run()
```

## How It Works

`RedisSink` is a batching sink.  
It batches processed records in memory per topic partition, and writes them to Redis
when a checkpoint has been committed.

### Data serialization

By default, `RedisSink` serializes records values to JSON and uses Kafka message keys as
Redis keys.

To change that, pass custom functions to the `key_serializer` and `value_serializer`
parameters:

```python
from quixstreams import Application
from quixstreams.sinks.community.redis import RedisSink

app = Application(
    broker_address="localhost:9092",
    auto_offset_reset="earliest",
    consumer_group="consumer-group",
)

topic = app.topic("topic-name")

redis_sink = RedisSink(
    host="<Redis host>",
    port="<Redis port>",
    db="<Redis db>",
    # Convert records' values to strings before writing to Redis
    value_serializer=lambda value: str(value),
    # Combine records' keys and values into new keys
    key_serializer=lambda key, value: f'{key}-{value}',
)

sdf = app.dataframe(topic)
sdf.sink(redis_sink)

if __name__ == '__main__':
    app.run()
```

### Atomic Writes

`RedisSink`
uses the [Redis Transactions](https://redis.io/docs/latest/develop/interact/transactions/)
feature to ensure that all updates are executed atomically.

## Delivery Guarantees

`RedisSink` provides at-least-once guarantees, and the same records may be written
multiple times in case of errors during processing.

## Configuration

Main configuration parameters:

- `host`: a Redis db host.
- `port`: a Redis db port.
- `db`: a Redis db number.
- `value_serializer`: a callable to serialize records' values.
- `key_serializer`: a callable to serialize records' keys.

For the full list of expected parameters, see
the [RedisSink API](../../api-reference/sinks.md#redissink) page
