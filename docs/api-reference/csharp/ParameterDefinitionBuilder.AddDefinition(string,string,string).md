#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Models.StreamProducer](QuixStreams.Streaming.Models.StreamProducer.md 'QuixStreams.Streaming.Models.StreamProducer').[ParameterDefinitionBuilder](ParameterDefinitionBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.ParameterDefinitionBuilder')

## ParameterDefinitionBuilder.AddDefinition(string, string, string) Method

Add new parameter definition to the [StreamTimeseriesProducer](StreamTimeseriesProducer.md 'QuixStreams.Streaming.Models.StreamProducer.StreamTimeseriesProducer'). Configure it with the builder methods.

```csharp
public QuixStreams.Streaming.Models.StreamProducer.ParameterDefinitionBuilder AddDefinition(string parameterId, string name=null, string description=null);
```
#### Parameters

<a name='QuixStreams.Streaming.Models.StreamProducer.ParameterDefinitionBuilder.AddDefinition(string,string,string).parameterId'></a>

`parameterId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The id of the parameter. Must match the parameter id used to send data.

<a name='QuixStreams.Streaming.Models.StreamProducer.ParameterDefinitionBuilder.AddDefinition(string,string,string).name'></a>

`name` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The human friendly display name of the parameter

<a name='QuixStreams.Streaming.Models.StreamProducer.ParameterDefinitionBuilder.AddDefinition(string,string,string).description'></a>

`description` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The description of the parameter

#### Returns
[ParameterDefinitionBuilder](ParameterDefinitionBuilder.md 'QuixStreams.Streaming.Models.StreamProducer.ParameterDefinitionBuilder')  
Parameter definition builder to define the parameter properties