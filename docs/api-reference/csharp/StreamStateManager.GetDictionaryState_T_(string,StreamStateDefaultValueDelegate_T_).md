#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager')

## StreamStateManager.GetDictionaryState<T>(string, StreamStateDefaultValueDelegate<T>) Method

Creates a new application state of dictionary type with automatically managed lifecycle for the stream

```csharp
public QuixStreams.Streaming.States.StreamDictionaryState<T> GetDictionaryState<T>(string stateName, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T> defaultValueFactory=null);
```
#### Type parameters

<a name='QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T'></a>

`T`
#### Parameters

<a name='QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).stateName'></a>

`stateName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The name of the state

<a name='QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).defaultValueFactory'></a>

`defaultValueFactory` [QuixStreams.Streaming.States.StreamStateDefaultValueDelegate&lt;](StreamStateDefaultValueDelegate_T_(string).md 'QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>(string)')[T](StreamStateManager.GetDictionaryState_T_(string,StreamStateDefaultValueDelegate_T_).md#QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T 'QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState<T>(string, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>).T')[&gt;](StreamStateDefaultValueDelegate_T_(string).md 'QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>(string)')

The value factory for the state when the state has no value for the key

#### Returns
[QuixStreams.Streaming.States.StreamDictionaryState&lt;](StreamDictionaryState_T_.md 'QuixStreams.Streaming.States.StreamDictionaryState<T>')[T](StreamStateManager.GetDictionaryState_T_(string,StreamStateDefaultValueDelegate_T_).md#QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T 'QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState<T>(string, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>).T')[&gt;](StreamDictionaryState_T_.md 'QuixStreams.Streaming.States.StreamDictionaryState<T>')  
Dictionary stream state