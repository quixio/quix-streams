#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[EventDefinitionBuilder](EventDefinitionBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder')

## EventDefinitionBuilder(StreamEventsProducer, string, EventDefinition) Constructor

Initializes a new instance of [EventDefinitionBuilder](EventDefinitionBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder')

```csharp
public EventDefinitionBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer streamEventsProducer, string location, QuixStreams.Telemetry.Models.EventDefinition properties=null);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer,string,QuixStreams.Telemetry.Models.EventDefinition).streamEventsProducer'></a>

`streamEventsProducer` [StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

Events producer owner

<a name='QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer,string,QuixStreams.Telemetry.Models.EventDefinition).location'></a>

`location` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Location selected for the Event definition builder

<a name='QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer,string,QuixStreams.Telemetry.Models.EventDefinition).properties'></a>

`properties` [QuixStreams.Telemetry.Models.EventDefinition](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinition 'QuixStreams.Telemetry.Models.EventDefinition')

Events definition instance managed by the builder