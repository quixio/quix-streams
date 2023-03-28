#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IStreamConsumerInternal](IStreamConsumerInternal.md 'QuixStreams.Streaming.IStreamConsumerInternal')

## IStreamConsumerInternal.OnEventData Event

Event raised when a new package of [QuixStreams.Telemetry.Models.EventDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDataRaw 'QuixStreams.Telemetry.Models.EventDataRaw') values have been received.

```csharp
event Action<IStreamConsumer,EventDataRaw> OnEventData;
```

#### Event Type
[System.Action&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[QuixStreams.Telemetry.Models.EventDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDataRaw 'QuixStreams.Telemetry.Models.EventDataRaw')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')