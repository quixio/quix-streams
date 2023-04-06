#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamConsumer](QuixStreams.Streaming.Models.StreamConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer').[StreamTimeseriesConsumer](StreamTimeseriesConsumer.md 'QuixStreams.Streaming.Models.StreamConsumer.StreamTimeseriesConsumer')

## StreamTimeseriesConsumer.OnDataReceived Event

Event raised when data is received (without buffering)  
This event does not use Buffers, and data will be raised as they arrive without any processing.

```csharp
public event EventHandler<TimeseriesDataReadEventArgs> OnDataReceived;
```

#### Event Type
[System.EventHandler&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')[TimeseriesDataReadEventArgs](TimeseriesDataReadEventArgs.md 'QuixStreams.Streaming.Models.StreamConsumer.TimeseriesDataReadEventArgs')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.EventHandler-1 'System.EventHandler`1')