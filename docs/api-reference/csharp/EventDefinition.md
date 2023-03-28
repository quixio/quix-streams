#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models](QuixStreams.Streaming.Models.md 'QuixStreams.Streaming.Models')

## EventDefinition Class

Describes additional context for the event

```csharp
public class EventDefinition
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; EventDefinition

| Properties | |
| :--- | :--- |
| [CustomProperties](EventDefinition.CustomProperties.md 'QuixStreams.Streaming.Models.EventDefinition.CustomProperties') | Gets the optional field for any custom properties that do not exist on the event.<br/>For example this could be a json string, describing all possible event values |
| [Description](EventDefinition.Description.md 'QuixStreams.Streaming.Models.EventDefinition.Description') | Gets the description of the event |
| [Id](EventDefinition.Id.md 'QuixStreams.Streaming.Models.EventDefinition.Id') | Gets the globally unique identifier of the event. |
| [Level](EventDefinition.Level.md 'QuixStreams.Streaming.Models.EventDefinition.Level') | Gets the level of the event. Defaults to [QuixStreams.Telemetry.Models.EventLevel.Information](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Models.EventLevel.Information 'QuixStreams.Telemetry.Models.EventLevel.Information') |
| [Location](EventDefinition.Location.md 'QuixStreams.Streaming.Models.EventDefinition.Location') | Gets the location of the event within the Event hierarchy. Example: "/", "car/chassis/suspension". |
| [Name](EventDefinition.Name.md 'QuixStreams.Streaming.Models.EventDefinition.Name') | Gets the display name of the event |
