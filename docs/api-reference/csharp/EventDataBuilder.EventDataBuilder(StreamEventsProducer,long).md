#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[EventDataBuilder](EventDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder')

## EventDataBuilder(StreamEventsProducer, long) Constructor

Initializes a new instance of [EventDataBuilder](EventDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder')

```csharp
public EventDataBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer streamEventsProducer, long timestampNanoseconds);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer,long).streamEventsProducer'></a>

`streamEventsProducer` [StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

Events producer owner

<a name='QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder.EventDataBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer,long).timestampNanoseconds'></a>

`timestampNanoseconds` [System.Int64](https://docs.microsoft.com/en-us/dotnet/api/System.Int64 'System.Int64')

Timestamp assigned to the Events created by the builder