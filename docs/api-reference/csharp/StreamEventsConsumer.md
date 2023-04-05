#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamConsumer](QuixStreams.Streaming.Models.StreamConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer')

## StreamEventsConsumer Class

Consumer for streams, which raises [EventData](EventData.md 'QuixStreams.Streaming.Models.EventData') and [QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions') related messages

```csharp
public class StreamEventsConsumer :
System.IDisposable
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamEventsConsumer

Implements [System.IDisposable](https://docs.microsoft.com/en-us/dotnet/api/System.IDisposable 'System.IDisposable')

| Properties | |
| :--- | :--- |
| [Definitions](StreamEventsConsumer.Definitions.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamEventsConsumer.Definitions') | Gets the latest set of event definitions |

| Events | |
| :--- | :--- |
| [OnDataReceived](StreamEventsConsumer.OnDataReceived.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamEventsConsumer.OnDataReceived') | Raised when an events data package is received for the stream |
| [OnDefinitionsChanged](StreamEventsConsumer.OnDefinitionsChanged.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamEventsConsumer.OnDefinitionsChanged') | Raised when the event definitions have changed for the stream.<br/>See [Definitions](StreamEventsConsumer.Definitions.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamEventsConsumer.Definitions') for the latest set of event definitions |
