#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

## StreamEventsProducer.AddTimestampMilliseconds(long) Method

Starts adding a new set of event values at the given timestamp.

```csharp
public QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder AddTimestampMilliseconds(long timeMilliseconds);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddTimestampMilliseconds(long).timeMilliseconds'></a>

`timeMilliseconds` [System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')

The time in milliseconds since the default [Epoch](StreamEventsProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Epoch') to add the event values at

#### Returns
[EventDataBuilder](EventDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder')  
Event data builder to add event values at the provided time