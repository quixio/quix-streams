# Configuring Quix Streams Application

`quixstreams.Application` class is the main place to configure Quix Streams applications.

You can configure various aspects of your app, like:

- Kafka settings and credentials.
- State directory and RocksDB parameters.
- Error callbacks.
- Logging.
- and more.


For the full list of parameters see the [Application API docs](../api-reference/application/#application__init__).

## Main Configuration Parameters

- **`broker_address`** - Kafka broker address as a string, required.    
Example - `"localhost:9092"`

- **`consumer_group`** - Kafka consumer group name.  
Consumer group is also used in the state directory path and as a prefix for changelog topics to ensure the applications from different consumer groups don't access the same state.      
**Default** - `"quixstreams-default"`.

- **`auto_offset_reset`** - Consumer `auto.offset.reset` setting.  
It determines where the consumer should start reading messages from.  
See more `auto.offset.reset` in this [article](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls#the-auto-offset-reset-configuration).  
**Options**: `"latest"`, `"earliest"`.  
**Default** - `"latest"`.


## State
- **`state_dir`** - path to the application state directory.  
This directory contains data for each state store separated by consumer group.  
Default - `"state"`.

- **`rocksdb_options`** - options to be used for RocksDB instances.   
The default configuration is described in the class `quixstreams.state.rocksdb.options.RocksDBOptions`.  
To override it, pass an instance of `quixstreams.state.rocksdb.options.RocksDBOptions`.  
**Default** - `None` (i.e. use a default config).

- **`use_changelog_topics`** - whether the application should use changelog topics to back stateful operations.  
See the [Stateful Processing](../advanced/stateful-processing/#fault-tolerance-recovery) page to learn more about changelog topics.   
**Default** - `True`.

## Logging
- `loglevel` - a log level to use for the "quixstreams" logger.  
**Options:** `"DEBUG"`, `"INFO"`, `"ERROR"`, `"WARNING"`, `"CRITICAL"`,`"NOTSET"`, `None`.  
If `None` is passed, no logging will be configured.  
You may pass `None` and configure "quixstreams" logger externally via `logging` library.    
**Default** - `"INFO"`.


## Error callbacks

To flexibly handle errors, `Application` class accepts callbacks triggered when exceptions occur on different stages of stream processing. 
If a callback returns `True`, the exception will be ignored. 
Otherwise, the exception will be propagated and the processing will eventually stop.

By default, Application will propagate all exceptions as usual.

>***NOTE:*** Use with caution. 
> The Application will commit the message offset if an error callback decides to ignore the exception. 

- **`on_consumer_error`** -  triggered when internal Consumer fails to poll Kafka or cannot deserialize a message.
Example:

```python
def on_consumer_error(exc: Exception, message, logger) -> bool:
    """
    Handle the consumer exception and ignore it
    """
    logger.error('Ignore consumer exception exc=%s offset=%s', exc, message.offset())
    return True
```

- **`on_processing_error`** - triggered when exception is raised during processing, after the message is deserialized.
Example:

```python
def on_processing_error(exc: Exception, row, logger) -> bool:
    """
    Handle the processing exception and ignore it
    """
    logger.error('Ignore processing exception exc=%s row=%s', exc, row)
    return True
```

- **`on_producer_error`** - triggered when internal Producer fails to serialize or to produce a message to Kafka.  
Example:

```python
def on_producer_error(exc: Exception, row, logger) -> bool:
    """
    Handle the producer exception and ignore it
    """
    logger.error('Ignore producer exception exc=%s row=%s', exc, row)
    return True
```

## Processing Callbacks
- **`on_message_processed`** - a callback triggered when message is successfully processed.  
You may use it for debugging purposes (e.g. count the total processed messages).

Another use case is to stop Application after processing a certain amount of messages.

Example:

```python
from quixstreams import Application

TOTAL_PROCESSED = 0
def on_message_processed(topic:str, partition: int, offset: int):
    """
    Stop application after processing 100 messages.
    """
    global TOTAL_PROCESSED
    TOTAL_PROCESSED += 1
    if TOTAL_PROCESSED == 100:
        app.stop()

app = Application(
    broker_address='localhost:9092',
    on_message_processed=on_message_processed,
)
```


## Topic Management
- **`auto_create_topics`** - whether to create all defined topics automatically when starting the app.  
Only topics made via `Application.topic()` call are tracked.  
**Default** - `True`

## Advanced Kafka Configuration

- **`partitioner`** - a partitioner to be used to determine the outgoing message partition in Producer.
**Options**: `"random"`, `"consistent_random"`, `"murmur2"`, `"murmur2_random"`, `"fnv1a"`, `"fnv1a_random"`.  
**Default** - `murmur2`.

- **`producer_extra_config`** - a dictionary with additional Producer options in the format of librdkafka.  
Values in this dictionary cannot override settings already defined via other parameters, like `broker_address` and `partitioner`.

- **`consumer_extra_config`** - a dictionary with additional Consumer options in the format of librdkafka.  
Values in the dictionary cannot override settings already defined via other parameters, like `broker_address`, `auto_offset_reset` and `consumer_group`.

- **`consumer_poll_timeout`** - a timeout in seconds for the internal Consumer `.poll()`.  
**Default** - `1.0`

- `producer_poll_timeout` - a timeout in seconds for the internal Producer.  
**Default** - `0`.
