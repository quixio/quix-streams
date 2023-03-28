#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[StreamConsumer](StreamConsumer.md 'QuixStreams.Streaming.StreamConsumer')

## StreamConsumer.OnTimeseriesData Event

Event raised when a new package of [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw') values have been received.

```csharp
public virtual event Action<IStreamConsumer,TimeseriesDataRaw> OnTimeseriesData;
```

#### Event Type
[System.Action&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')