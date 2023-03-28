#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[StreamConsumer](StreamConsumer.md 'QuixStreams.Streaming.StreamConsumer')

## StreamConsumer.OnStreamPropertiesChanged Event

Event raised when the Stream Properties have changed.

```csharp
public virtual event Action<IStreamConsumer,StreamProperties> OnStreamPropertiesChanged;
```

#### Event Type
[System.Action&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')[QuixStreams.Telemetry.Models.StreamProperties](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.StreamProperties 'QuixStreams.Telemetry.Models.StreamProperties')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Action-2 'System.Action`2')