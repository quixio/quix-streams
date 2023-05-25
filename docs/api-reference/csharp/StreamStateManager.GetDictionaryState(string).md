#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager')

## StreamStateManager.GetDictionaryState(string) Method

Creates a new application state of dictionary type with automatically managed lifecycle for the stream

```csharp
public QuixStreams.Streaming.States.StreamState GetDictionaryState(string stateName);
```
#### Parameters

<a name='QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState(string).stateName'></a>

`stateName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The name of the state

#### Returns
[StreamState](StreamState.md 'QuixStreams.Streaming.States.StreamState')  
Dictionary stream state