#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

## StreamEventsProducer.AddTimestamp(TimeSpan) Method

Starts adding a new set of event values at the given timestamp.

```csharp
public QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder AddTimestamp(System.TimeSpan timeSpan);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddTimestamp(System.TimeSpan).timeSpan'></a>

`timeSpan` [System.TimeSpan](https://docs.microsoft.com/en-us/dotnet/api/System.TimeSpan 'System.TimeSpan')

The time since the default [Epoch](StreamEventsProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Epoch') to add the event values at

#### Returns
[EventDataBuilder](EventDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder')  
Event data builder to add event values at the provided time