# Streaming context

Broker client libraries enable you to send messages that can contain arbitrary data, by using binary content. Quix Streams takes a similar approach. The problem with these messages is there is no obvious way to group them, which is often a requirement in streaming applications. 

Sending messages using a broker client library is illustrated in the following diagram:

![Read and Write using Broker client libraries](../images/PlainBrokerMessaging.png)

Quix Streams creates a stream context for you to group all data for a source. This enables [automatic horizontal scaling](horizontal-scaling.md) of your models when you deal with multiple data sources.

![Horizontal scalability using Quix Streams](../images/QuixStreamsScaling.png)

This context simplifies processing streams by providing callbacks on the subscriber. You can keep working with each context (stream) separately or together, depending on your needs.

The library also allows you to [attach metadata](../subscribe.md) to streams, such as IDs, location, references, time, or any other type of information related to the data source.

![Attach metadata to streams using Quix Streams](../images/QuixStreamsMetadata.png)

This metadata can be read by the library in real time. You can also review historical data in the Quix Portal, if you choose to persist the topic streams.
