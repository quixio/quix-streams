# Configuring Quix Streams Application

`quixstreams.Application` class is the main place to configure Quix Streams applications.

You can configure various aspects of your app, like:

- Kafka settings and credentials.
- State directory and RocksDB parameters.
- Error callbacks.
- Logging.
- and more.


For the full list of parameters see the [Application API docs](api-reference/application.md#application__init__).

## Main Configuration Parameters

- **`broker_address` (REQUIRED)** - Kafka connection settings as either:    
  1. broker url string (`"localhost:9092"`), good for local development    
***OR***     
  2. `ConnectionConfig` (see: [Authentication](#authentication) for more details).

- **`consumer_group`** - Kafka consumer group name.  
Consumer group is also used in the state directory path and as a prefix for changelog topics to ensure the applications from different consumer groups don't access the same state.      
**Default** - `"quixstreams-default"`.

- **`commit_interval`** - How often to commit the processed offsets and state in seconds.  
**Default** - `5.0`.    
See the [Checkpointing](advanced/checkpointing.md) page for more information about 
the `commit_interval` parameter.

- **`auto_offset_reset`** - Consumer `auto.offset.reset` setting.  
It determines where the consumer should start reading messages from.  
See more `auto.offset.reset` in this [article](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls#the-auto-offset-reset-configuration).  
**Options**: `"latest"`, `"earliest"`.  
**Default** - `"latest"`.

- **`processing_guarantee`** - Use "at-least-once" or "exactly-once" processing guarantees.  
See [Processing Guarantees](#processing-guarantees) for more information.  
**Options**: `"at-least-once"` or `"exactly-once".  
**Default** - `"at-least-once"`.

## Authentication

If you need to provide authentication settings for your broker, you
can do so with the `broker_address` argument by passing it a `ConnectionConfig`
object (instead of a string), like so:

```python
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

connection = ConnectionConfig(
    bootstrap_servers="my_url",
    security_protocol="sasl_plaintext",
    sasl_mechanism="PLAIN",
    sasl_username="my_user",
    sasl_password="my_pass"
)

app = Application(broker_address=connection)
```

### Using A librdkafka Config (Alternative)

`ConnectionConfig` can also import from an already valid librdkafka dictionary:

```python
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

librdkafka_config = {
    "bootstrap.servers": "my.url",
    "security.protocol": "sasl_plaintext",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "my_user",
    "sasl.password": "my_pass"
}

# NOTE: use class directly (ConnectionConfig, NOT ConnectionConfig())
app = Application(
    broker_address=ConnectionConfig.from_librdkafka_dict(librdkafka_config)
)
```
#### Ignoring Irrelevant librdkafka Settings

`ConnectionConfig.from_librdkafka_dict(config, ignore_extras=True)` will additionally 
ignore irrelevant settings (but you will lose some validation checks).

## Processing Guarantees

This section concerns the `processing_guarantee` setting.

### What are "processing guarantees"?
Kafka broadly has three guarantee levels/semantics associated with handling messages.

From weakest to strongest: `at-most-once`, `at-least-once`, and `exactly-once`. 
Stronger guarantees generally have larger overhead, and thus reduced speed. 

These guarantees can be read literally: when consuming Kafka messages, you can 
guarantee each will be processed `X` times:

- `at-most-once` - a message will be processed not more than once.  
If the processing fails, the message will not be processed again.
- `at-least-once` - a message may be processed more than once.  
It may lead to duplicates in the output topics, but the message won’t be lost and is guaranteed to be processed.
- `exactly-once` - a message will be processed exactly once.  
The message is guaranteed to be processed without duplicated outputs but at the cost of higher latency and complexity.

### What options does Quix Streams offer?

Currently, Quix Streams offers `at-least-once` and `exactly-once`.

The default guarantee is `at-least-once`. 

### Performance comparison

It is difficult to offer hard numbers for the speed difference between `exactly-once` 
and `at-least-once` because many factors are at play, though latency is perhaps 
the main contributor since `exactly-once` requires more communication with the broker.

With default `Application` settings, `exactly-once` will likely perform at worst 
10% slower than `at-least-once`, but given the nature of infrastructure/architecture, 
even this is not a guarantee.

You can minimize the impact of latency somewhat by adjusting the [`commit_interval` of 
the `Application`](#main-configuration-parameters), but be aware of [the
performance considerations around `commit_interval`](advanced/checkpointing.md)).


### Exactly-Once producer buffering

Because `exactly-once` messages do not fully commit or produce until the 
[checkpoint](advanced/checkpointing.md) successfully completes, any resulting produced 
messages are not immediately readable like with `at-least-once`.

Basically, the [`commit_interval` of an `Application`](#main-configuration-parameters) 
can be interpreted as the maximum amount of time before a produced message can be read 
(depending on how far into the checkpoint a message is processed). If you adjust it, 
be aware of other [performance considerations around `commit_interval`](advanced/checkpointing.md)).

> NOTE: `groupby` doubles this effect since it produces to another topic under the hood. 


### What processing guarantee is right for me?

To pick the right guarantee, you need to weigh an increase in speed 
vs. the potential to double-process a result, which may require other infrastructural
considerations to handle appropriately.

In general, you may consider using `at-least-once` guarantees when:

- The latency and processing speed are the primary concerns.
- The downstream consumers can gracefully handle duplicated messages.

You may consider using `exactly-once` instead when:

- Consistency and correctness of the outputs are critical.
- Downstream consumers of the output topics cannot handle duplicated data.
- Note: you may want to pick a smaller value for the `commit_interval` to commit checkpoints more often and reduce the latency.

For more information about tuning the `commit_interval`, see the ["Configuring the Checkpointing" page](advanced/checkpointing.md#configuring-the-checkpointing). 


## State
- **`state_dir`** - path to the application state directory.  
This directory contains data for each state store separated by consumer group.  
Default - `"state"`.

- **`rocksdb_options`** - options to be used for RocksDB instances.   
The default configuration is described in the class `quixstreams.state.rocksdb.options.RocksDBOptions`.  
To override it, pass an instance of `quixstreams.state.rocksdb.options.RocksDBOptions`.  
**Default** - `None` (i.e. use a default config).

- **`use_changelog_topics`** - whether the application should use changelog topics to back stateful operations.  
See the [Stateful Processing](advanced/stateful-processing.md#fault-tolerance-recovery) page to learn more about changelog topics.   
**Default** - `True`.

## Logging
- `loglevel` - a log level to use for the "quixstreams" logger.  
**Options:** `"DEBUG"`, `"INFO"`, `"ERROR"`, `"WARNING"`, `"CRITICAL"`,`"NOTSET"`, `None`.  
If `None` is passed, no logging will be configured.  
You may pass `None` and configure "quixstreams" logger externally using `logging` library.    
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
Only topics made using `Application.topic()` call are tracked.  
**Default** - `True`.
- **`topic_create_timeout`** - timeout in seconds to wait for newly created topics to finalize. 
**Default** - `60.0`.

## Advanced Kafka Configuration

- **`producer_extra_config`** - a dictionary with additional Producer options in the format of librdkafka.  
Values in this dictionary cannot override settings already defined by other parameters, like `broker_address`.

- **`consumer_extra_config`** - a dictionary with additional Consumer options in the format of librdkafka.  
Values in the dictionary cannot override settings already defined by other parameters, like `broker_address`, `auto_offset_reset` and `consumer_group`.

- **`consumer_poll_timeout`** - a timeout in seconds for the internal Consumer `.poll()`.  
**Default** - `1.0`.

- **`producer_poll_timeout`** - a timeout in seconds for the internal Producer.  
**Default** - `0.0`.

- **`request_timeout`** - request timeout in seconds for any API-related calls, mostly 
around topic management. 
**Default** - `30.0`.