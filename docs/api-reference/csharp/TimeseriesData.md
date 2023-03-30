#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## TimeseriesData Class

Represents a collection of [TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp')

```csharp
public class TimeseriesData
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; TimeseriesData

| Constructors | |
| :--- | :--- |
| [TimeseriesData(TimeseriesDataRaw, string[], bool, bool)](TimeseriesData.TimeseriesData(TimeseriesDataRaw,string[],bool,bool).md 'QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(QuixStreams.Telemetry.Models.TimeseriesDataRaw, string[], bool, bool)') | Creates a new instance of [TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData') based on a [QuixStreams.Telemetry.Models.TimeseriesDataRaw](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.TimeseriesDataRaw 'QuixStreams.Telemetry.Models.TimeseriesDataRaw') instance |
| [TimeseriesData(List&lt;TimeseriesDataTimestamp&gt;, bool, bool)](TimeseriesData.TimeseriesData(List_TimeseriesDataTimestamp_,bool,bool).md 'QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(System.Collections.Generic.List<QuixStreams.Streaming.Models.TimeseriesDataTimestamp>, bool, bool)') | Creates a new instance of [TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData') with the provided timestamps |
| [TimeseriesData(int)](TimeseriesData.TimeseriesData(int).md 'QuixStreams.Streaming.Models.TimeseriesData.TimeseriesData(int)') | Create a new empty Timeseries Data instance to allow create new timestamps and parameters values from scratch |

| Properties | |
| :--- | :--- |
| [Timestamps](TimeseriesData.Timestamps.md 'QuixStreams.Streaming.Models.TimeseriesData.Timestamps') | Gets the data as rows of [TimeseriesDataTimestamp](TimeseriesDataTimestamp.md 'QuixStreams.Streaming.Models.TimeseriesDataTimestamp') |

| Methods | |
| :--- | :--- |
| [AddTimestamp(DateTime)](TimeseriesData.AddTimestamp(DateTime).md 'QuixStreams.Streaming.Models.TimeseriesData.AddTimestamp(System.DateTime)') | Starts adding a new set of parameter values at the given timestamp. |
| [AddTimestamp(TimeSpan)](TimeseriesData.AddTimestamp(TimeSpan).md 'QuixStreams.Streaming.Models.TimeseriesData.AddTimestamp(System.TimeSpan)') | Starts adding a new set of parameter values at the given timestamp. |
| [AddTimestampMilliseconds(long)](TimeseriesData.AddTimestampMilliseconds(long).md 'QuixStreams.Streaming.Models.TimeseriesData.AddTimestampMilliseconds(long)') | Starts adding a new set of parameter values at the given timestamp. |
| [AddTimestampNanoseconds(long)](TimeseriesData.AddTimestampNanoseconds(long).md 'QuixStreams.Streaming.Models.TimeseriesData.AddTimestampNanoseconds(long)') | Starts adding a new set of parameter values at the given timestamp. |
| [Clone(string[])](TimeseriesData.Clone(string[]).md 'QuixStreams.Streaming.Models.TimeseriesData.Clone(string[])') | Clone the Timeseries Data |
