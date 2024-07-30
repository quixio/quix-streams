<a id="quixstreams.kafka.producer"></a>

## quixstreams.kafka.producer

<a id="quixstreams.kafka.producer.Producer"></a>

### Producer

```python
class Producer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L39)

<a id="quixstreams.kafka.producer.Producer.__init__"></a>

<br><br>

#### Producer.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             logger: logging.Logger = logger,
             error_callback: Callable[[KafkaError], None] = _default_error_cb,
             extra_config: Optional[dict] = None,
             flush_timeout: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L40)

A wrapper around `confluent_kafka.Producer`.

It initializes `confluent_kafka.Producer` on demand
avoiding network calls during `__init__`, provides typing info for methods
and some reasonable defaults.


<br>
***Arguments:***

- `broker_address`: Connection settings for Kafka.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
- `logger`: a Logger instance to attach librdkafka logging to
- `error_callback`: callback used for producer errors
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
Note: values passed as arguments override values in `extra_config`.
- `flush_timeout`: The time the producer is waiting for all messages to be delivered.

<a id="quixstreams.kafka.producer.Producer.produce"></a>

<br><br>

#### Producer.produce

```python
def produce(topic: str,
            value: Optional[Union[str, bytes]] = None,
            key: Optional[Union[str, bytes]] = None,
            headers: Optional[Headers] = None,
            partition: Optional[int] = None,
            timestamp: Optional[int] = None,
            poll_timeout: float = 5.0,
            buffer_error_max_tries: int = 3,
            on_delivery: Optional[DeliveryCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L78)

Produce a message to a topic.

It also polls Kafka for callbacks before producing to minimize
the probability of `BufferError`.
If `BufferError` still happens, the method will poll Kafka with timeout
to free up the buffer and try again.


<br>
***Arguments:***

- `topic`: topic name
- `value`: message value
- `key`: message key
- `headers`: message headers
- `partition`: topic partition
- `timestamp`: message timestamp
- `poll_timeout`: timeout for `poll()` call in case of `BufferError`
- `buffer_error_max_tries`: max retries for `BufferError`.
Pass `0` to not retry after `BufferError`.
- `on_delivery`: the delivery callback to be triggered on `poll()`
for the produced message.

<a id="quixstreams.kafka.producer.Producer.poll"></a>

<br><br>

#### Producer.poll

```python
def poll(timeout: float = 0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L139)

Polls the producer for events and calls `on_delivery` callbacks.


<br>
***Arguments:***

- `timeout`: poll timeout seconds; Default: 0 (unlike others)
> NOTE: -1 will hang indefinitely if there are no messages to acknowledge

<a id="quixstreams.kafka.producer.Producer.flush"></a>

<br><br>

#### Producer.flush

```python
def flush(timeout: Optional[float] = None) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L147)

Wait for all messages in the Producer queue to be delivered.


<br>
***Arguments:***

- `timeout` (`float`): time to attempt flushing (seconds).
None use producer default or -1 is infinite. Default: None


<br>
***Returns:***

number of messages remaining to flush

<a id="quixstreams.kafka.producer.TransactionalProducer"></a>

### TransactionalProducer

```python
class TransactionalProducer(Producer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L178)

A separate producer class used only internally for transactions
(transactions are only needed when using a consumer).

<a id="quixstreams.kafka.consumer"></a>

## quixstreams.kafka.consumer

<a id="quixstreams.kafka.consumer.Consumer"></a>

### Consumer

```python
class Consumer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L64)

<a id="quixstreams.kafka.consumer.Consumer.__init__"></a>

<br><br>

#### Consumer.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             consumer_group: Optional[str],
             auto_offset_reset: AutoOffsetReset,
             auto_commit_enable: bool = True,
             logger: logging.Logger = logger,
             error_callback: Callable[[KafkaError], None] = _default_error_cb,
             on_commit: Optional[Callable[
                 [Optional[KafkaError], List[TopicPartition]], None]] = None,
             extra_config: Optional[dict] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L65)

A wrapper around `confluent_kafka.Consumer`.

It initializes `confluent_kafka.Consumer` on demand
avoiding network calls during `__init__`, provides typing info for methods
and some reasonable defaults.


<br>
***Arguments:***

- `broker_address`: Connection settings for Kafka.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`
- `auto_offset_reset`: Consumer `auto.offset.reset` setting.
Available values:
<br>"earliest" - automatically reset the offset to the smallest offset
<br>"latest" - automatically reset the offset to the largest offset
<br>"error" - trigger an error (`ERR__AUTO_OFFSET_RESET`) which is
retrieved by consuming messages (used for testing)
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
- `logger`: a Logger instance to attach librdkafka logging to
- `error_callback`: callback used for consumer errors
- `on_commit`: Offset commit result propagation callback.
Passed as "offset_commit_cb" to `confluent_kafka.Consumer`.
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
Note: values passed as arguments override values in `extra_config`.

<a id="quixstreams.kafka.consumer.Consumer.poll"></a>

<br><br>

#### Consumer.poll

```python
def poll(timeout: Optional[float] = None) -> Optional[Message]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L128)

Consumes a single message, calls callbacks and returns events.

The application must check the returned :py:class:`Message`
object's :py:func:`Message.error()` method to distinguish between proper
messages (error() returns None), or an event or error.

Note: a `RebalancingCallback` may be called from this method (
`on_assign`, `on_revoke`, or `on_lost`).


<br>
***Arguments:***

- `timeout` (`float`): Maximum time in seconds to block waiting for message,
event or callback. None or -1 is infinite. Default: None.

**Raises**:

- `RuntimeError`: if called on a closed consumer


<br>
***Returns:***

`Optional[Message]`: A `Message` object or `None` on timeout

<a id="quixstreams.kafka.consumer.Consumer.subscribe"></a>

<br><br>

#### Consumer.subscribe

```python
def subscribe(topics: List[str],
              on_assign: Optional[RebalancingCallback] = None,
              on_revoke: Optional[RebalancingCallback] = None,
              on_lost: Optional[RebalancingCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L147)

Set subscription to supplied list of topics

This replaces a previous subscription.


<br>
***Arguments:***

- `topics` (`List[str]`): List of topics (strings) to subscribe to.
- `on_assign` (`Optional[RebalancingCallback]`): callback to provide handling of
customized offsets on completion of a successful partition re-assignment.
- `on_revoke` (`Optional[RebalancingCallback]`): callback to provide handling of
offset commits to a customized store on the start of a rebalance operation.
- `on_lost` (`Optional[RebalancingCallback]`): callback to provide handling in
the case the partition assignment has been lost. Partitions that have been
lost may already be owned by other members in the group and therefore
committing offsets, for example, may fail.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.unsubscribe"></a>

<br><br>

#### Consumer.unsubscribe

```python
def unsubscribe()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L231)

Remove current subscription.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.store_offsets"></a>

<br><br>

#### Consumer.store\_offsets

```python
def store_offsets(message: Optional[Message] = None,
                  offsets: Optional[List[TopicPartition]] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L240)

Store offsets for a message or a list of offsets.

`message` and `offsets` are mutually exclusive. The stored offsets
will be committed according to 'auto.commit.interval.ms' or manual
offset-less `commit`.
Note that 'enable.auto.offset.store' must be set to False when using this API.


<br>
***Arguments:***

- `message` (`confluent_kafka.Message`): Store message's offset+1.
- `offsets` (`List[TopicPartition]`): List of topic+partitions+offsets to store.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.commit"></a>

<br><br>

#### Consumer.commit

```python
def commit(message: Optional[Message] = None,
           offsets: Optional[List[TopicPartition]] = None,
           asynchronous: bool = True) -> Optional[List[TopicPartition]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L271)

Commit a message or a list of offsets.

The `message` and `offsets` parameters are mutually exclusive.
If neither is set, the current partition assignment's offsets are used instead.
Use this method to commit offsets if you have 'enable.auto.commit' set to False.


<br>
***Arguments:***

- `message` (`Message`): Commit the message's offset+1.
Note: By convention, committed offsets reflect the next message
to be consumed, **not** the last message consumed.
- `offsets` (`List[TopicPartition]`): List of topic+partitions+offsets to commit.
- `asynchronous` (`bool`): If true, asynchronously commit, returning None
immediately. If False, the commit() call will block until the commit
succeeds or fails and the committed offsets will be returned (on success).
Note that specific partitions may have failed and the .err field of
each partition should be checked for success.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.committed"></a>

<br><br>

#### Consumer.committed

```python
def committed(partitions: List[TopicPartition],
              timeout: Optional[float] = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L311)

Retrieve committed offsets for the specified partitions.


<br>
***Arguments:***

- `partitions` (`List[TopicPartition]`): List of topic+partitions to query for stored offsets.
- `timeout` (`float`): Request timeout (seconds).
None or -1 is infinite. Default: None

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer


<br>
***Returns:***

`List[TopicPartition]`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.get_watermark_offsets"></a>

<br><br>

#### Consumer.get\_watermark\_offsets

```python
def get_watermark_offsets(partition: TopicPartition,
                          timeout: Optional[float] = None,
                          cached: bool = False) -> Tuple[int, int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L329)

Retrieve low and high offsets for the specified partition.


<br>
***Arguments:***

- `partition` (`TopicPartition`): Topic+partition to return offsets for.
- `timeout` (`float`): Request timeout (seconds). None or -1 is infinite.
Ignored if cached=True. Default: None
- `cached` (`bool`): Instead of querying the broker, use cached information.
Cached values: The low offset is updated periodically
(if statistics.interval.ms is set) while the high offset is updated on each
message fetched from the broker for this partition.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer


<br>
***Returns:***

`Tuple[int, int]`: Tuple of (low,high) on success or None on timeout.
The high offset is the offset of the last message + 1.

<a id="quixstreams.kafka.consumer.Consumer.list_topics"></a>

<br><br>

#### Consumer.list\_topics

```python
def list_topics(topic: Optional[str] = None,
                timeout: Optional[float] = None) -> ClusterMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L355)

Request metadata from the cluster.

This method provides the same information as
listTopics(), describeTopics() and describeCluster() in  the Java Admin client.


<br>
***Arguments:***

- `topic` (`str`): If specified, only request information about this topic,
else return results for all topics in cluster.
Warning: If auto.create.topics.enable is set to true on the broker and
an unknown topic is specified, it will be created.
- `timeout` (`float`): The maximum response time before timing out
None or -1 is infinite. Default: None

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.Consumer.memberid"></a>

<br><br>

#### Consumer.memberid

```python
def memberid() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L376)

Return this client's broker-assigned group member id.

The member id is assigned by the group coordinator and is propagated to
the consumer during rebalance.

**Raises**:

- `RuntimeError`: if called on a closed consumer


<br>
***Returns:***

`Optional[string]`: Member id string or None

<a id="quixstreams.kafka.consumer.Consumer.offsets_for_times"></a>

<br><br>

#### Consumer.offsets\_for\_times

```python
def offsets_for_times(partitions: List[TopicPartition],
                      timeout: Optional[float] = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L389)

Look up offsets by timestamp for the specified partitions.

The returned offset for each partition is the earliest offset whose
timestamp is greater than or equal to the given timestamp in the
corresponding partition. If the provided timestamp exceeds that of the
last message in the partition, a value of -1 will be returned.


<br>
***Arguments:***

- `partitions` (`List[TopicPartition]`): topic+partitions with timestamps
in the TopicPartition.offset field.
- `timeout` (`float`): The maximum response time before timing out.
None or -1 is infinite. Default: None

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer


<br>
***Returns:***

`List[TopicPartition]`: List of topic+partition with offset field set and possibly error set

<a id="quixstreams.kafka.consumer.Consumer.pause"></a>

<br><br>

#### Consumer.pause

```python
def pause(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L415)

Pause consumption for the provided list of partitions.

Paused partitions must be tracked manually.

Does NOT affect the result of `Consumer.assignment()`.


<br>
***Arguments:***

- `partitions` (`List[TopicPartition]`): List of topic+partitions to pause.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.Consumer.resume"></a>

<br><br>

#### Consumer.resume

```python
def resume(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L428)

Resume consumption for the provided list of partitions.


<br>
***Arguments:***

- `partitions` (`List[TopicPartition]`): List of topic+partitions to resume.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.Consumer.position"></a>

<br><br>

#### Consumer.position

```python
def position(partitions: List[TopicPartition]) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L438)

Retrieve current positions (offsets) for the specified partitions.


<br>
***Arguments:***

- `partitions` (`List[TopicPartition]`): List of topic+partitions to return
current offsets for. The current offset is the offset of
the last consumed message + 1.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer


<br>
***Returns:***

`List[TopicPartition]`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.seek"></a>

<br><br>

#### Consumer.seek

```python
def seek(partition: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L452)

Set consume position for partition to offset.

The offset may be an absolute (>=0) or a
logical offset like `OFFSET_BEGINNING`.

`seek()` may only be used to update the consume offset of an
actively consumed partition (i.e., after `Consumer.assign()`),
to set the starting offset of partition not being consumed instead
pass the offset in an `assign()` call.


<br>
***Arguments:***

- `partition` (`TopicPartition`): Topic+partition+offset to seek to.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.Consumer.assignment"></a>

<br><br>

#### Consumer.assignment

```python
def assignment() -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L469)

Returns the current partition assignment.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer


<br>
***Returns:***

`List[TopicPartition]`: List of assigned topic+partitions.

<a id="quixstreams.kafka.consumer.Consumer.set_sasl_credentials"></a>

<br><br>

#### Consumer.set\_sasl\_credentials

```python
def set_sasl_credentials(username: str, password: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L482)

Sets the SASL credentials used for this client.

These credentials will overwrite the old ones, and will be used the next
time the client needs to authenticate.
This method will not disconnect existing broker connections that have been
established with the old credentials.
This method is applicable only to SASL PLAIN and SCRAM mechanisms.


<br>
***Arguments:***

- `username` (`str`): your username
- `password` (`str`): your password

<a id="quixstreams.kafka.consumer.Consumer.incremental_assign"></a>

<br><br>

#### Consumer.incremental\_assign

```python
def incremental_assign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L496)

Assign new partitions.

Can be called outside the `Consumer` `on_assign` callback (multiple times).
Partitions immediately show on `Consumer.assignment()`.

Any additional partitions besides the ones passed during the `Consumer`
`on_assign` callback will NOT be associated with the consumer group.


<br>
***Arguments:***

- `partitions` (`List[TopicPartition]`): a list of topic partitions

<a id="quixstreams.kafka.consumer.Consumer.incremental_unassign"></a>

<br><br>

#### Consumer.incremental\_unassign

```python
def incremental_unassign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L510)

Revoke partitions.

Can be called outside an on_revoke callback.


<br>
***Arguments:***

- `partitions` (`List[TopicPartition]`): a list of topic partitions

<a id="quixstreams.kafka.consumer.Consumer.close"></a>

<br><br>

#### Consumer.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L520)

Close down and terminate the Kafka Consumer.

Actions performed:

- Stops consuming.
- Commits offsets, unless the consumer property 'enable.auto.commit' is set to False.
- Leaves the consumer group.

Registered callbacks may be called from this method,
see `poll()` for more info.

<a id="quixstreams.kafka.consumer.Consumer.consumer_group_metadata"></a>

<br><br>

#### Consumer.consumer\_group\_metadata

```python
def consumer_group_metadata() -> GroupMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L537)

Used by the producer during consumer offset sending for an EOS transaction.

