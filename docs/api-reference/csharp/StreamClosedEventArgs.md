#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## StreamClosedEventArgs Class

Provides data for the StreamClosed event.

```csharp
public class StreamClosedEventArgs
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamClosedEventArgs

| Constructors | |
| :--- | :--- |
| [StreamClosedEventArgs(ITopicConsumer, IStreamConsumer, StreamEndType)](StreamClosedEventArgs.StreamClosedEventArgs(ITopicConsumer,IStreamConsumer,StreamEndType).md 'QuixStreams.Streaming.StreamClosedEventArgs.StreamClosedEventArgs(QuixStreams.Streaming.ITopicConsumer, QuixStreams.Streaming.IStreamConsumer, QuixStreams.Telemetry.Models.StreamEndType)') | Initializes a new instance of the StreamClosedEventArgs class. |

| Properties | |
| :--- | :--- |
| [EndType](StreamClosedEventArgs.EndType.md 'QuixStreams.Streaming.StreamClosedEventArgs.EndType') | Gets the mode how the stream was closed. |
| [Stream](StreamClosedEventArgs.Stream.md 'QuixStreams.Streaming.StreamClosedEventArgs.Stream') | Gets the stream consumer associated with the event. |
| [TopicConsumer](StreamClosedEventArgs.TopicConsumer.md 'QuixStreams.Streaming.StreamClosedEventArgs.TopicConsumer') | Gets the topic consumer associated with the event. |
