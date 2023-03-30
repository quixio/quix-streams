#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

## StreamEventsProducer.AddTimestampNanoseconds(long) Method

Starts adding a new set of event values at the given timestamp.

```csharp
public QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder AddTimestampNanoseconds(long timeNanoseconds);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddTimestampNanoseconds(long).timeNanoseconds'></a>

`timeNanoseconds` [System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')

The time in nanoseconds since the default [Epoch](StreamEventsProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Epoch') to add the event values at

#### Returns
[EventDataBuilder](EventDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder')  
Event data builder to add event values at the provided time