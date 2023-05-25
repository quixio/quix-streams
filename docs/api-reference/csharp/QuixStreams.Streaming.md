#### [QuixStreams.Streaming](index.md 'index')

## QuixStreams.Streaming Namespace

| Classes | |
| :--- | :--- |
| [App](App.md 'QuixStreams.Streaming.App') | Provides utilities to handle default streaming behaviors and automatic resource cleanup on shutdown. |
| [IStreamConsumerExtensions](IStreamConsumerExtensions.md 'QuixStreams.Streaming.IStreamConsumerExtensions') | Extensions for IStreamConsumer |
| [KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient') | A Kafka streaming client capable of creating topic consumer and producers. |
| [KafkaStreamingClientExtensions](KafkaStreamingClientExtensions.md 'QuixStreams.Streaming.KafkaStreamingClientExtensions') | Extensions for Streaming Client class |
| [PackageReceivedEventArgs](PackageReceivedEventArgs.md 'QuixStreams.Streaming.PackageReceivedEventArgs') | Provides data for the PackageReceived event. |
| [QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient') | Streaming client for Kafka configured automatically using Environment Variables and Quix platform endpoints.<br/>Use this Client when you use this library together with Quix platform. |
| [QuixStreamingClient.TokenValidationConfiguration](QuixStreamingClient.TokenValidationConfiguration.md 'QuixStreams.Streaming.QuixStreamingClient.TokenValidationConfiguration') | Token Validation configuration |
| [QuixStreamingClientExtensions](QuixStreamingClientExtensions.md 'QuixStreams.Streaming.QuixStreamingClientExtensions') | Quix Streaming Client extensions |
| [StreamClosedEventArgs](StreamClosedEventArgs.md 'QuixStreams.Streaming.StreamClosedEventArgs') | Provides data for the StreamClosed event. |
| [TopicConsumer](TopicConsumer.md 'QuixStreams.Streaming.TopicConsumer') | Implementation of [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer') to consume incoming streams |
| [TopicProducer](TopicProducer.md 'QuixStreams.Streaming.TopicProducer') | Implementation of [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer') to produce outgoing streams |

| Structs | |
| :--- | :--- |
| [TimeseriesDataTimestampValues](TimeseriesDataTimestampValues.md 'QuixStreams.Streaming.TimeseriesDataTimestampValues') | Enumerable which returns the the Parameter Values of the current [TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp') |

| Interfaces | |
| :--- | :--- |
| [IKafkaStreamingClient](IKafkaStreamingClient.md 'QuixStreams.Streaming.IKafkaStreamingClient') | |
| [IQuixStreamingClient](IQuixStreamingClient.md 'QuixStreams.Streaming.IQuixStreamingClient') | |
| [IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer') | Stream reader interface. Stands for a new stream read from the platform.<br/>Allows to read the stream data received from a topic. |
| [IStreamProducer](IStreamProducer.md 'QuixStreams.Streaming.IStreamProducer') | Stands for a new stream that we want to send to the platform.<br/>It provides you helper properties to stream data the platform like parameter values, events, definitions and all the information you can persist to the platform. |
| [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer') | Interface to subscribe to incoming streams |
| [ITopicProducer](ITopicProducer.md 'QuixStreams.Streaming.ITopicProducer') | Interface to produce outgoing streams |
