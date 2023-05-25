#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[PackageReceivedEventArgs](PackageReceivedEventArgs.md 'QuixStreams.Streaming.PackageReceivedEventArgs')

## PackageReceivedEventArgs(ITopicConsumer, IStreamConsumer, StreamPackage) Constructor

Initializes a new instance of the PackageReceivedEventArgs class.

```csharp
public PackageReceivedEventArgs(QuixStreams.Streaming.ITopicConsumer topicConsumer, QuixStreams.Streaming.IStreamConsumer consumer, QuixStreams.Telemetry.Models.StreamPackage package);
```
#### Parameters

<a name='QuixStreams.Streaming.PackageReceivedEventArgs.PackageReceivedEventArgs(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumer,QuixStreams.Telemetry.Models.StreamPackage).topicConsumer'></a>

`topicConsumer` [ITopicConsumer](ITopicConsumer.md 'QuixStreams.Streaming.ITopicConsumer')

The topic consumer associated with the event.

<a name='QuixStreams.Streaming.PackageReceivedEventArgs.PackageReceivedEventArgs(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumer,QuixStreams.Telemetry.Models.StreamPackage).consumer'></a>

`consumer` [IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')

The stream consumer associated with the event.

<a name='QuixStreams.Streaming.PackageReceivedEventArgs.PackageReceivedEventArgs(QuixStreams.Streaming.ITopicConsumer,QuixStreams.Streaming.IStreamConsumer,QuixStreams.Telemetry.Models.StreamPackage).package'></a>

`package` [QuixStreams.Telemetry.Models.StreamPackage](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.StreamPackage 'QuixStreams.Telemetry.Models.StreamPackage')

The stream package that was received.