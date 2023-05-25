#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States')

## StreamStateManager Class

Manages the states of a stream.

```csharp
public class StreamStateManager
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; StreamStateManager

| Constructors | |
| :--- | :--- |
| [StreamStateManager(string, IStateStorage, ILoggerFactory, string)](StreamStateManager.StreamStateManager(string,IStateStorage,ILoggerFactory,string).md 'QuixStreams.Streaming.States.StreamStateManager.StreamStateManager(string, QuixStreams.State.Storage.IStateStorage, Microsoft.Extensions.Logging.ILoggerFactory, string)') | Initializes a new instance of the [StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager') class with the specified parameters. |

| Methods | |
| :--- | :--- |
| [DeleteState(string)](StreamStateManager.DeleteState(string).md 'QuixStreams.Streaming.States.StreamStateManager.DeleteState(string)') | Deletes the state with the specified name |
| [DeleteStates()](StreamStateManager.DeleteStates().md 'QuixStreams.Streaming.States.StreamStateManager.DeleteStates()') | Deletes all states for the current stream. |
| [GetDictionaryState(string)](StreamStateManager.GetDictionaryState(string).md 'QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState(string)') | Creates a new application state of dictionary type with automatically managed lifecycle for the stream |
| [GetDictionaryState&lt;T&gt;(string, StreamStateDefaultValueDelegate&lt;T&gt;)](StreamStateManager.GetDictionaryState_T_(string,StreamStateDefaultValueDelegate_T_).md 'QuixStreams.Streaming.States.StreamStateManager.GetDictionaryState<T>(string, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>)') | Creates a new application state of dictionary type with automatically managed lifecycle for the stream |
| [GetStates()](StreamStateManager.GetStates().md 'QuixStreams.Streaming.States.StreamStateManager.GetStates()') | Returns an enumerable collection of all available state names for the current stream. |
