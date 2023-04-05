#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

## StreamEventsProducer.AddTimestamp(DateTime) Method

Starts adding a new set of event values at the given timestamp.  
Note, [Epoch](StreamEventsProducer.Epoch.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.Epoch') is not used when invoking with [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime')

```csharp
public QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder AddTimestamp(System.DateTime dateTime);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddTimestamp(System.DateTime).dateTime'></a>

`dateTime` [System.DateTime](https://docs.microsoft.com/en-us/dotnet/api/System.DateTime 'System.DateTime')

The datetime to use for adding new event values

#### Returns
[EventDataBuilder](EventDataBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDataBuilder')  
Event data builder to add event values at the provided time