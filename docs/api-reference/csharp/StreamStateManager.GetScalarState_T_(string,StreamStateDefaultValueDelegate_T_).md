#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager')

## StreamStateManager.GetScalarState<T>(string, StreamStateDefaultValueDelegate<T>) Method

Creates a new application state of scalar type with automatically managed lifecycle for the stream

```csharp
public QuixStreams.Streaming.States.StreamScalarState<T> GetScalarState<T>(string stateName, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T> defaultValueFactory=null);
```
#### Type parameters

<a name='QuixStreams.Streaming.States.StreamStateManager.GetScalarState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T'></a>

`T`
#### Parameters

<a name='QuixStreams.Streaming.States.StreamStateManager.GetScalarState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).stateName'></a>

`stateName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The name of the state

<a name='QuixStreams.Streaming.States.StreamStateManager.GetScalarState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).defaultValueFactory'></a>

`defaultValueFactory` [QuixStreams.Streaming.States.StreamStateDefaultValueDelegate&lt;](StreamStateDefaultValueDelegate_T_(string).md 'QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>(string)')[T](StreamStateManager.GetScalarState_T_(string,StreamStateDefaultValueDelegate_T_).md#QuixStreams.Streaming.States.StreamStateManager.GetScalarState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T 'QuixStreams.Streaming.States.StreamStateManager.GetScalarState<T>(string, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>).T')[&gt;](StreamStateDefaultValueDelegate_T_(string).md 'QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>(string)')

The value factory for the state when the state has no value for the key

#### Returns
[QuixStreams.Streaming.States.StreamScalarState&lt;](StreamScalarState_T_.md 'QuixStreams.Streaming.States.StreamScalarState<T>')[T](StreamStateManager.GetScalarState_T_(string,StreamStateDefaultValueDelegate_T_).md#QuixStreams.Streaming.States.StreamStateManager.GetScalarState_T_(string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T 'QuixStreams.Streaming.States.StreamStateManager.GetScalarState<T>(string, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>).T')[&gt;](StreamScalarState_T_.md 'QuixStreams.Streaming.States.StreamScalarState<T>')  
Dictionary stream state