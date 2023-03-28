#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## IStreamConsumerInternal Interface

Stream reader interface. Stands for a new stream read from the platform.  
Allows to read the stream data received from a topic.

```csharp
internal interface IStreamConsumerInternal :
QuixStreams.Streaming.IStreamConsumer
```

Derived  
&#8627; [StreamConsumer](StreamConsumer.md 'QuixStreams.Streaming.StreamConsumer')

Implements [IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')

| Events | |
| :--- | :--- |
| [OnEventData](IStreamConsumerInternal.OnEventData.md 'QuixStreams.Streaming.IStreamConsumerInternal.OnEventData') | Event raised when a new package of [QuixStreams.Telemetry.Models.EventDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDataRaw 'QuixStreams.Telemetry.Models.EventDataRaw') values have been received. |
| [OnEventDefinitionsChanged](IStreamConsumerInternal.OnEventDefinitionsChanged.md 'QuixStreams.Streaming.IStreamConsumerInternal.OnEventDefinitionsChanged') | Event raised when the [QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions') have been changed. |
| [OnParameterDefinitionsChanged](IStreamConsumerInternal.OnParameterDefinitionsChanged.md 'QuixStreams.Streaming.IStreamConsumerInternal.OnParameterDefinitionsChanged') | Event raised when the [QuixStreams.Telemetry.Models.ParameterDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.ParameterDefinitions 'QuixStreams.Telemetry.Models.ParameterDefinitions') have been changed. |
| [OnStreamPropertiesChanged](IStreamConsumerInternal.OnStreamPropertiesChanged.md 'QuixStreams.Streaming.IStreamConsumerInternal.OnStreamPropertiesChanged') | Event raised when the Stream Properties have changed. |
| [OnTimeseriesData](IStreamConsumerInternal.OnTimeseriesData.md 'QuixStreams.Streaming.IStreamConsumerInternal.OnTimeseriesData') | Event raised when a new package of [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw') values have been received. |
