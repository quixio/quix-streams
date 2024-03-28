# Using Producer & Consumer

## When use Producer & Consumer
Quix Streams provide access to low-level `Producer` and `Consumer` classes.

The intended use is when `StreamingDataFrame` is not enough for the given application, and you need more flexibility.   

For example:

- To produce data to the Kafka topic from the file or another non-Kafka source.
- To manually commit topic offsets.
- To batch messages in-memory before processing them.

`Producer` & `Consumer` are essentially thin wrappers on top of the [`confluent_kafka`](https://github.com/confluentinc/confluent-kafka-python) library, which is used in Quix Streams.  
The wrapping code adds additional logging, typing, and error handling, but the intention is to keep the methods API the same.

Using them will require more code than with `StreamingDataFrame` but you can implement any streaming workflows you need.


## Using Consumer

There are two ways to create a `Consumer` object:

1. Using `Application.get_consumer()` method.  
This way the `Application` will provide the already configured `Consumer` according to `Application` configs.  
It is a recommended way to create a `Consumer`.

2. Create an instance of `quixstreams.kafka.Consumer`.  
This way you will need to configure the instance yourself.

See the **[Consumer API Docs](../api-reference/kafka.md#consumer)** for more details and methods.

### Differences from `confluent_kafka.Consumer`

- The `__init__` parameters are expanded into individual params compared to `confluent_kafka.Consumer` which accepts a dictionary will all the parameters.
- The `"enable.auto.offset.store"` parameter is set to `False` by default in order to provide at-least-once processing guarantees by default.
- Rebalancing callbacks `on_assign`, `on_revoke` and `on_lost` raise Kafka errors as `PartitionAssignmentError` exceptions when they occur.
- `quixstreams.kafka.Consumer` implements a context manager interface to gracefully close itself.

**Example**:

Creating a `Consumer` object using an `Application` instance and start polling the topic.

```python
from quixstreams import Application

# Configure an Application. 
# The config params will be used for the Consumer instance too.
app = Application(
    broker_address='localhost:9092', 
    auto_offset_reset='earliest', 
    auto_commit_enable=True,
)

# Create a consumer and start a polling loop
with app.get_consumer() as consumer:
    consumer.subscribe(topics=['my-topic'])

    while True:
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        elif msg.error():
            print('Kafka error:', msg.error())
            continue

        value = msg.value()
        # Do some work with the value here ...
        
        # Store the offset of the processed message on the Consumer 
        # for the auto-commit mechanism.
        # It will send it to Kafka in the background.
        # Storing offset only after the message is processed enables at-least-once delivery
        # guarantees.
        consumer.store_offsets(message=msg)
```



## Using Producer

Similarly to `Consumer`, there are two ways to create a `Producer` object:

1. Using `Application.get_producer()` method.    
This way the `Application` will provide the already configured `Producer` according to `Application` configs.  
It is a recommended way to create a `Producer`.

2. Create an instance of `quixstreams.kafka.Producer`.  
This way you will need to configure the instance yourself.

See the **[Producer API Docs](../api-reference/kafka.md#producer)** for more details and methods.

### Differences from `confluent_kafka.Producer`

- The `__init__` parameters are expanded into individual params compared to `confluent_kafka.Producer` which accepts a dictionary will all the parameters.
- The `produce()` method automatically calls `.poll()` to empty the internal buffer for produced messages.
    - `produce()` also retries `BufferError` in case the internal producer buffer is full. 
- `quixstreams.kafka.Producer` implements a context manager interface to gracefully flush itself.


**Example**:

Creating a `Producer` object using an `Application` instance and start producing messages.

For a more complete example, you can also read the [Quickstart](../quickstart.md) page.

```python
from quixstreams import Application

# Configure an Application. 
# The config params will be used for the Consumer instance too.
app = Application(broker_address='localhost:9092')

# Create some messages to produce
messages = [
  {'key': b'key1', 'value': b'value1'},
  {'key': b'key2', 'value': b'value2'},
]

# Create a producer and start producing messages
with app.get_producer() as producer:
    for message in messages:
        producer.produce(topic='my-topic', key=message['key'], value=message['value'])
```
