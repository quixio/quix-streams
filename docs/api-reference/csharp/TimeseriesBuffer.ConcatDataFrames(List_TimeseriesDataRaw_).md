#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBuffer](TimeseriesBuffer.md 'QuixStreams.Streaming.Models.TimeseriesBuffer')

## TimeseriesBuffer.ConcatDataFrames(List<TimeseriesDataRaw>) Method

Concatenate list of TimeseriesDataRaws into a single TimeseriesDataRaw

```csharp
protected QuixStreams.Telemetry.Models.TimeseriesDataRaw ConcatDataFrames(System.Collections.Generic.List<QuixStreams.Telemetry.Models.TimeseriesDataRaw> TimeseriesDataRaws);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.TimeseriesBuffer.ConcatDataFrames(System.Collections.Generic.List_QuixStreams.Telemetry.Models.TimeseriesDataRaw_).TimeseriesDataRaws'></a>

`TimeseriesDataRaws` [System.Collections.Generic.List&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')[QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.List-1 'System.Collections.Generic.List`1')

List of data to concatenate

#### Returns
[QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw')  
New object with the proper length containing concatenated data