# Horizontal scaling

Quix Streams provides horizontal scaling using [streaming context](streaming-context.md) for automatic partitioning, together with the underlying broker technology, such as Kafka.

Consider the following example:

![Horizontal scaling initial state](../images/QuixHorizontalScaling1.png)

Each car produces one stream with its own time-series data, and each stream is processed by a replica of the deployment, labelled "Process". By default, the message broker assigns each stream to one replica via the [RangeAssignor strategy](https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/consumer/RangeAssignor.html).

When the purple replica crashes, "stream 4" is assigned automatically to the blue replica.

![Purple replica crashes](../images/QuixHorizontalScaling2.png)

This situation triggers an event on the topic consumer in the blue replica indicating that "stream 4" has been received:

=== "Python"
    
    ``` python
    def on_stream_received_handler(topic_consumer: TopicConsumer, stream_received: StreamConsumer):
        print("Stream received:" + stream_received.stream_id)
    
    topic_consumer.on_stream_received = on_stream_received_handler
    ```

=== "C\#"
    
    ``` cs
    topicConsumer.OnStreamReceived += (topic, newStream) =>
    {
        Console.WriteLine($"New stream received: {newStream.StreamId}");
    };
    ```

This would result in the following output on blue replica:

``` console
New stream received: stream 4
```

When the purple replica restarts and becomes available again, it signals to the broker, and takes control of "stream 4":

![Purple replica has been restarted](../images/QuixHorizontalScaling3.png)

This will trigger two events, one in the blue replica indicating that "stream 4" has been revoked, and one in the purple replica indicating that "stream 4" has been assigned again:

=== "Python"
    
    ``` python
    def on_stream_received_handler(stream_received: StreamConsumer):
        print("Stream received:" + stream_received.stream_id)
    
    def on_streams_revoked_handler(topic_consumer: TopicConsumer, streams_revoked: [StreamConsumer]):
        for stream in streams_revoked:
            print("Stream revoked:" + stream.stream_id)
    
    topic_consumer.on_stream_received = on_stream_received_handler
    topic_consumer.on_streams_revoked = on_streams_revoked_handler
    ```

=== "C\#"
    
    ``` cs
    topicConsumer.OnStreamReceived += (topic, newStream) =>
    {
        Console.WriteLine($"New stream received: {newStream.StreamId}");
    };
    
    topicConsumer.OnStreamsRevoked += (topic, streamsRevoked) =>
    {
        foreach (var stream in streamsRevoked)
        {
            Console.WriteLine($"Stream revoked: {stream.StreamId}");
        }
    };
    ```

Results in the following output on the blue replica:

``` console
Stream revoked: stream 4
```

Results in the following output on the purple replica:

``` console
New stream received: stream 4
```

The same behavior happens if the "Process" deployment is scaled up or down, by increasing or decreasing the number of replicas. Kafka triggers the rebalancing mechanism internally and this triggers the same events on Quix Streams. 

!!! note

    Note that this example assumes ideal conditions, but in reality a rebalance event can shift all streams to different processes. The library ensures you only get revocation raised for a stream if it is not assigned back to the same consumer, not while it is rebalancing.

## Rebalancing mechanism and partitions

Kafka uses partitions and the [RangeAssignor strategy](https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/consumer/RangeAssignor.html) to decide which consumers receive which messages. 

Partitions and the Kafka rebalancing protocol are internal details of the Kafka implementation behind Quix Streams. These are abstracted within the [Streaming Context](streaming-context.md) feature of the library. The events described above remain the same, even if Quix Streams uses another message broker technology, or another rebalancing mechanism in the future.

!!! warning

    The Kafka rebalancing mechanism is such that, when subscribing to a topic, you should not have more replicas than the number of partitions in the topic, as any additional replicas will remain idle.
