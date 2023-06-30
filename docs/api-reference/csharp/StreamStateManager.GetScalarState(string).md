#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager')

## StreamStateManager.GetScalarState(string) Method

Creates a new application state of scalar type with automatically managed lifecycle for the stream

```csharp
public QuixStreams.Streaming.States.StreamScalarState GetScalarState(string stateName);
```
#### Parameters

<a name='QuixStreams.Streaming.States.StreamStateManager.GetScalarState(string).stateName'></a>

`stateName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The name of the state

#### Returns
[StreamScalarState](StreamScalarState.md 'QuixStreams.Streaming.States.StreamScalarState')  
Dictionary stream state