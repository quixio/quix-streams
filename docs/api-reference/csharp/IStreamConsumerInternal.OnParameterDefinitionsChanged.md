#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IStreamConsumerInternal](IStreamConsumerInternal.md 'QuixStreams.Streaming.IStreamConsumerInternal')

## IStreamConsumerInternal.OnParameterDefinitionsChanged Event

Event raised when the [QuixStreams.Telemetry.Models.ParameterDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.ParameterDefinitions 'QuixStreams.Telemetry.Models.ParameterDefinitions') have been changed.

```csharp
event Action<IStreamConsumer,ParameterDefinitions> OnParameterDefinitionsChanged;
```

#### Event Type
[System.Action&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[QuixStreams.Telemetry.Models.ParameterDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.ParameterDefinitions 'QuixStreams.Telemetry.Models.ParameterDefinitions')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')