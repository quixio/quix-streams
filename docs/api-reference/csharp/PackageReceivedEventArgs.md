#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## PackageReceivedEventArgs Class

Provides data for the PackageReceived event.

```csharp
public class PackageReceivedEventArgs
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; PackageReceivedEventArgs

| Constructors | |
| :--- | :--- |
| [PackageReceivedEventArgs(ITopicConsumer, IStreamConsumer, StreamPackage)](PackageReceivedEventArgs.PackageReceivedEventArgs(ITopicConsumer,IStreamConsumer,StreamPackage).md 'QuixStreams.Streaming.PackageReceivedEventArgs.PackageReceivedEventArgs(QuixStreams.Streaming.ITopicConsumer, QuixStreams.Streaming.IStreamConsumer, QuixStreams.Telemetry.Models.StreamPackage)') | Initializes a new instance of the PackageReceivedEventArgs class. |

| Properties | |
| :--- | :--- |
| [Package](PackageReceivedEventArgs.Package.md 'QuixStreams.Streaming.PackageReceivedEventArgs.Package') | Gets the stream package that was received. |
| [Stream](PackageReceivedEventArgs.Stream.md 'QuixStreams.Streaming.PackageReceivedEventArgs.Stream') | Gets the stream consumer associated with the event. |
| [TopicConsumer](PackageReceivedEventArgs.TopicConsumer.md 'QuixStreams.Streaming.PackageReceivedEventArgs.TopicConsumer') | Gets the topic consumer associated with the event. |
