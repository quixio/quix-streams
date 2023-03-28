#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models').[TimeseriesBufferConfiguration](TimeseriesBufferConfiguration.md 'QuixStreams.Streaming.Models.TimeseriesBufferConfiguration')

## TimeseriesBufferConfiguration.CustomTrigger Property

Gets or sets the custom function which is invoked after adding a new timestamp to the buffer. If returns true, [OnDataReleased](TimeseriesBuffer.OnDataReleased.md 'QuixStreams.Streaming.Models.TimeseriesBuffer.OnDataReleased') is invoked with the entire buffer content  
Defaults to null (disabled).

```csharp
public System.Func<QuixStreams.Streaming.Models.TimeseriesData,bool> CustomTrigger { get; set; }
```

#### Property Value
[System.Func&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')[TimeseriesData](TimeseriesData.md 'QuixStreams.Streaming.Models.TimeseriesData')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')[System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')