#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer')

## EventDefinitionBuilder Class

Builder for creating [QuixStreams.Telemetry.Models.EventDefinitions](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventDefinitions 'QuixStreams.Telemetry.Models.EventDefinitions') within [StreamEventsProducer](StreamEventsProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer')

```csharp
public class EventDefinitionBuilder
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; EventDefinitionBuilder

| Constructors | |
| :--- | :--- |
| [EventDefinitionBuilder(StreamEventsProducer, string, EventDefinition)](EventDefinitionBuilder.EventDefinitionBuilder(StreamEventsProducer,string,EventDefinition).md 'QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder.EventDefinitionBuilder(QuixStreams.Streaming.Models.StreamProducer.StreamEventsProducer, string, QuixStreams.Telemetry.Models.EventDefinition)') | Initializes a new instance of [EventDefinitionBuilder](EventDefinitionBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder') |

| Methods | |
| :--- | :--- |
| [AddDefinition(string, string, string)](EventDefinitionBuilder.AddDefinition(string,string,string).md 'QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder.AddDefinition(string, string, string)') | Add new Event definition, to define properties like Name or Level, among others. |
| [SetCustomProperties(string)](EventDefinitionBuilder.SetCustomProperties(string).md 'QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder.SetCustomProperties(string)') | Set custom properties of the Event |
| [SetLevel(EventLevel)](EventDefinitionBuilder.SetLevel(EventLevel).md 'QuixStreams.Streaming.Models.StreamProducer.EventDefinitionBuilder.SetLevel(QuixStreams.Telemetry.Models.EventLevel)') | Set severity level of the Event |
