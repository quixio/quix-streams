#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

## StreamEventsProducer.AddDefinition(string, string, string) Method

Add new Event definition to define properties like Name or Level, among others.

```csharp
public QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder AddDefinition(string eventId, string name=null, string description=null);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddDefinition(string,string,string).eventId'></a>

`eventId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Event Id. This must match the event id you use to Event values

<a name='QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddDefinition(string,string,string).name'></a>

`name` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Human friendly display name of the event

<a name='QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer.AddDefinition(string,string,string).description'></a>

`description` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Description of the event

#### Returns
[EventDefinitionBuilder](EventDefinitionBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder')  
Event definition builder to define the event properties