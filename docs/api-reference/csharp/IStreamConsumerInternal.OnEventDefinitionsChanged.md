#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IStreamConsumerInternal](IStreamConsumerInternal.md 'QuixStreams.Streaming.IStreamConsumerInternal')

## IStreamConsumerInternal.OnEventDefinitionsChanged Event

Event raised when the [QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions') have been changed.

```csharp
event Action<IStreamConsumer,EventDefinitions> OnEventDefinitionsChanged;
```

#### Event Type
[System.Action&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')