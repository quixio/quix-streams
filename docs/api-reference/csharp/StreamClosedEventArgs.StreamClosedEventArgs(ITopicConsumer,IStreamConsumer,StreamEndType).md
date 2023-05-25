#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[StreamClosedEventArgs](StreamClosedEventArgs.md 'QuixStreams.Streaming.StreamClosedEventArgs')

## StreamClosedEventArgs(ITopicConsumer, IStreamConsumer, StreamEndType) Constructor

Initializes a new instance of the StreamClosedEventArgs class.

```csharp
public StreamClosedEventArgs(QuixStreams.Streaming.ITopicConsumer topicConsumer, QuixStreams.Streaming.IStreamConsumer consumer, QuixStreams.Telemetry.Models.StreamEndType endType);
```
#### Parameters

<a name='QuixStreams.Streaming.StreamClosedEventArgs.StreamClosedEventArgs(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumer,QuixStreams.Telemetry.Models.StreamEndType).topicConsumer'></a>

`topicConsumer` [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')

The topic consumer associated with the event.

<a name='QuixStreams.Streaming.StreamClosedEventArgs.StreamClosedEventArgs(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumer,QuixStreams.Telemetry.Models.StreamEndType).consumer'></a>

`consumer` [IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')

The stream consumer associated with the event.

<a name='QuixStreams.Streaming.StreamClosedEventArgs.StreamClosedEventArgs(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumer,QuixStreams.Telemetry.Models.StreamEndType).endType'></a>

`endType` [QuixStreams.Telemetry.Models.StreamEndType](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.StreamEndType 'QuixStreams.Telemetry.Models.StreamEndType')

The mode how the stream was closed.