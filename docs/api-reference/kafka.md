<a id="quixstreams.kafka.producer"></a>

## quixstreams.kafka.producer

<a id="quixstreams.kafka.producer.Producer"></a>

### Producer

```python
class Producer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/producer.py#L54)

<a id="quixstreams.kafka.producer.Producer.__init__"></a>

<br><br>

#### Producer.\_\_init\_\_

```python
def __init__(broker_address: str,
             partitioner: Partitioner = "murmur2",
             extra_config: Optional[dict] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/producer.py#L55)

A wrapper around `confluent_kafka.Producer`.

It initializes `confluent_kafka.Producer` on demand
avoiding network calls during `__init__`, provides typing info for methods
and some reasonable defaults.


<br>
***Arguments:***

- `broker_address`: Kafka broker host and port in format `<host>:<port>`.
Passed as `bootstrap.servers` to `confluent_kafka.Producer`.
- `partitioner`: A function to be used to determine the outgoing message
partition.
Available values: "random", "consistent_random", "murmur2", "murmur2_random",
"fnv1a", "fnv1a_random"
Default - "murmur2".
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
Note: values passed as arguments override values in `extra_config`.

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
            buffer_error_max_tries: int = 3)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/producer.py#L94)

Produce message to topic.

It also polls Kafka for callbacks before producing in order to minimize
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

<a id="quixstreams.kafka.producer.Producer.poll"></a>

<br><br>

#### Producer.poll

```python
def poll(timeout: float = 0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/producer.py#L152)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/producer.py#L160)

Wait for all messages in the Producer queue to be delivered.


<br>
***Arguments:***

- `timeout` (`float`): time to attempt flushing (seconds).
None or -1 is infinite. Default: None


<br>
***Returns:***

number of messages delivered

<a id="quixstreams.kafka.consumer"></a>

## quixstreams.kafka.consumer

<a id="quixstreams.kafka.consumer.Consumer"></a>

### Consumer

```python
class Consumer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L66)

<a id="quixstreams.kafka.consumer.Consumer.__init__"></a>

<br><br>

#### Consumer.\_\_init\_\_

```python
def __init__(broker_address: str,
             consumer_group: Optional[str],
             auto_offset_reset: AutoOffsetReset,
             auto_commit_enable: bool = True,
             assignment_strategy: AssignmentStrategy = "range",
             on_commit: Optional[Callable[
                 [Optional[KafkaError], List[TopicPartition]], None]] = None,
             extra_config: Optional[dict] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L67)

A wrapper around `confluent_kafka.Consumer`.

It initializes `confluent_kafka.Consumer` on demand
avoiding network calls during `__init__`, provides typing info for methods
and some reasonable defaults.


<br>
***Arguments:***

- `broker_address`: Kafka broker host and port in format `<host>:<port>`.
Passed as `bootstrap.servers` to `confluent_kafka.Consumer`.
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`
- `auto_offset_reset`: Consumer `auto.offset.reset` setting.
Available values:
- "earliest" - automatically reset the offset to the smallest offset
- "latest" - automatically reset the offset to the largest offset
- "error" - trigger an error (ERR__AUTO_OFFSET_RESET) which is retrieved
by consuming messages (used for testing)
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
- `assignment_strategy`: The name of a partition assignment strategy.
Available values: "range", "roundrobin", "cooperative-sticky".
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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L126)

Consumes a single message, calls callbacks and returns events.

The application must check the returned :py:class:`Message`
object's :py:func:`Message.error()` method to distinguish between proper
messages (error() returns None), or an event or error.

Note: Callbacks may be called from this method, such as
``on_assign``, ``on_revoke``, et al.


<br>
***Arguments:***

- `timeout` (`float`): Maximum time in seconds to block waiting for message,
event or callback. None or -1 is infinite. Default: None.

**Raises**:

- `None`: RuntimeError if called on a closed consumer


<br>
***Returns:***

A Message object or None on timeout

<a id="quixstreams.kafka.consumer.Consumer.subscribe"></a>

<br><br>

#### Consumer.subscribe

```python
def subscribe(topics: List[str],
              on_assign: Optional[RebalancingCallback] = None,
              on_revoke: Optional[RebalancingCallback] = None,
              on_lost: Optional[RebalancingCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L144)

Set subscription to supplied list of topics

This replaces a previous subscription.


<br>
***Arguments:***

- `topics` (`list(str)`): List of topics (strings) to subscribe to.
- `on_assign` (`callable`): callback to provide handling of customized offsets
on completion of a successful partition re-assignment.
- `on_revoke` (`callable`): callback to provide handling of offset commits to
a customized store on the start of a rebalance operation.
- `on_lost` (`callable`): callback to provide handling in the case the partition
assignment has been lost. Partitions that have been lost may already be
owned by other members in the group and therefore committing offsets,
for example, may fail.

**Raises**:

- `KafkaException`: 
- `None`: RuntimeError if called on a closed consumer
.. py:function:: on_assign(consumer, partitions)
.. py:function:: on_revoke(consumer, partitions)
.. py:function:: on_lost(consumer, partitions)

  :param Consumer consumer: Consumer instance.
  :param list(TopicPartition) partitions: Absolute list of partitions being
  assigned or revoked.


<a id="quixstreams.kafka.consumer.Consumer.unsubscribe"></a>

<br><br>

#### Consumer.unsubscribe

```python
def unsubscribe()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L238)

Remove current subscription.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.store_offsets"></a>

<br><br>

#### Consumer.store\_offsets

```python
def store_offsets(message: Optional[Message] = None,
                  offsets: Optional[List[TopicPartition]] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L246)

.. py:function:: store_offsets([message=None], [offsets=None])

Store offsets for a message or a list of offsets.

``message`` and ``offsets`` are mutually exclusive. The stored offsets
will be committed according to 'auto.commit.interval.ms' or manual
offset-less `commit`.
Note that 'enable.auto.offset.store' must be set to False when using this API.


<br>
***Arguments:***

- `message` (`confluent_kafka.Message`): Store message's offset+1.
- `offsets` (`list(TopicPartition)`): List of topic+partitions+offsets to store.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.commit"></a>

<br><br>

#### Consumer.commit

```python
def commit(message: Optional[Message] = None,
           offsets: Optional[List[TopicPartition]] = None,
           asynchronous: bool = True) -> Optional[List[TopicPartition]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L280)

Commit a message or a list of offsets.

The ``message`` and ``offsets`` parameters are mutually exclusive.
If neither is set, the current partition assignment's offsets are used instead.
Use this method to commit offsets if you have 'enable.auto.commit' set to False.


<br>
***Arguments:***

- `message` (`confluent_kafka.Message`): Commit the message's offset+1.
Note: By convention, committed offsets reflect the next message
to be consumed, **not** the last message consumed.
- `offsets` (`list(TopicPartition)`): List of topic+partitions+offsets to commit.
- `asynchronous` (`bool`): If true, asynchronously commit, returning None
immediately. If False, the commit() call will block until the commit
succeeds or fails and the committed offsets will be returned (on success).
Note that specific partitions may have failed and the .err field of
each partition should be checked for success.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.committed"></a>

<br><br>

#### Consumer.committed

```python
def committed(partitions: List[TopicPartition],
              timeout: Optional[float] = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L320)

.. py:function:: committed(partitions, [timeout=None])

Retrieve committed offsets for the specified partitions.


<br>
***Arguments:***

- `partitions` (`list(TopicPartition)`): List of topic+partitions to query for stored offsets.
- `timeout` (`float`): Request timeout (seconds).
None or -1 is infinite. Default: None

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer


<br>
***Returns:***

`list(TopicPartition)`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.get_watermark_offsets"></a>

<br><br>

#### Consumer.get\_watermark\_offsets

```python
def get_watermark_offsets(partition: TopicPartition,
                          timeout: Optional[float] = None,
                          cached: bool = False) -> Tuple[int, int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L340)

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

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer


<br>
***Returns:***

`tuple(int,int)`: Tuple of (low,high) on success or None on timeout.
The high offset is the offset of the last message + 1.

<a id="quixstreams.kafka.consumer.Consumer.list_topics"></a>

<br><br>

#### Consumer.list\_topics

```python
def list_topics(topic: Optional[str] = None,
                timeout: Optional[float] = None) -> ClusterMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L366)

.. py:function:: list_topics([topic=None], [timeout=-1])

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

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.memberid"></a>

<br><br>

#### Consumer.memberid

```python
def memberid() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L389)

Return this client's broker-assigned group member id.

The member id is assigned by the group coordinator and is propagated to
the consumer during rebalance.

 :returns: Member id string or None
 :rtype: string
 :raises: RuntimeError if called on a closed consumer


<a id="quixstreams.kafka.consumer.Consumer.offsets_for_times"></a>

<br><br>

#### Consumer.offsets\_for\_times

```python
def offsets_for_times(partitions: List[TopicPartition],
                      timeout: Optional[float] = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L402)

Look up offsets by timestamp for the specified partitions.

The returned offset for each partition is the earliest offset whose
timestamp is greater than or equal to the given timestamp in the
corresponding partition. If the provided timestamp exceeds that of the
last message in the partition, a value of -1 will be returned.

 :param list(TopicPartition) partitions: topic+partitions with timestamps
    in the TopicPartition.offset field.
 :param float timeout: The maximum response time before timing out.
    None or -1 is infinite. Default: None
 :returns: List of topic+partition with offset field set and possibly error set
 :rtype: list(TopicPartition)
 :raises: KafkaException
 :raises: RuntimeError if called on a closed consumer


<a id="quixstreams.kafka.consumer.Consumer.pause"></a>

<br><br>

#### Consumer.pause

```python
def pause(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L428)

Pause consumption for the provided list of partitions.

Paused partitions must be tracked manually.

Does NOT affect the result of Consumer.assignment().


<br>
***Arguments:***

- `partitions` (`list(TopicPartition)`): List of topic+partitions to pause.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.resume"></a>

<br><br>

#### Consumer.resume

```python
def resume(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L442)

.. py:function:: resume(partitions)

Resume consumption for the provided list of partitions.


<br>
***Arguments:***

- `partitions` (`list(TopicPartition)`): List of topic+partitions to resume.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.position"></a>

<br><br>

#### Consumer.position

```python
def position(partitions: List[TopicPartition]) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L454)

Retrieve current positions (offsets) for the specified partitions.


<br>
***Arguments:***

- `partitions` (`list(TopicPartition)`): List of topic+partitions to return
current offsets for. The current offset is the offset of
the last consumed message + 1.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer


<br>
***Returns:***

`list(TopicPartition)`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.seek"></a>

<br><br>

#### Consumer.seek

```python
def seek(partition: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L468)

Set consume position for partition to offset.

The offset may be an absolute (>=0) or a
logical offset (:py:const:`OFFSET_BEGINNING` et.al).

seek() may only be used to update the consume offset of an
actively consumed partition (i.e., after :py:const:`assign()`),
to set the starting offset of partition not being consumed instead
pass the offset in an `assign()` call.


<br>
***Arguments:***

- `partition` (`TopicPartition`): Topic+partition+offset to seek to.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.assignment"></a>

<br><br>

#### Consumer.assignment

```python
def assignment() -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L485)

Returns the current partition assignment.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer


<br>
***Returns:***

`list(TopicPartition)`: List of assigned topic+partitions.

<a id="quixstreams.kafka.consumer.Consumer.set_sasl_credentials"></a>

<br><br>

#### Consumer.set\_sasl\_credentials

```python
def set_sasl_credentials(username: str, password: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L498)

Sets the SASL credentials used for this client.
These credentials will overwrite the old ones, and will be used the next
time the client needs to authenticate.
This method will not disconnect existing broker connections that have been
established with the old credentials.
This method is applicable only to SASL PLAIN and SCRAM mechanisms.

<a id="quixstreams.kafka.consumer.Consumer.incremental_assign"></a>

<br><br>

#### Consumer.incremental\_assign

```python
def incremental_assign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L510)

Assign new partitions.

Can be called outside the `Consumer` `on_assign` callback (multiple times).
Partitions immediately show on `Consumer.assignment()`.

Any additional partitions besides the ones passed during the `Consumer`
`on_assign` callback will NOT be associated with the consumer group.

<a id="quixstreams.kafka.consumer.Consumer.incremental_unassign"></a>

<br><br>

#### Consumer.incremental\_unassign

```python
def incremental_unassign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L522)

Revoke partitions.

Can be called outside an on_revoke callback.

<a id="quixstreams.kafka.consumer.Consumer.close"></a>

<br><br>

#### Consumer.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/kafka/consumer.py#L530)

Close down and terminate the Kafka Consumer.

Actions performed:

- Stops consuming.
- Commits offsets, unless the consumer property 'enable.auto.commit' is set to False.
- Leaves the consumer group.

Registered callbacks may be called from this method,
see `poll()` for more info.


