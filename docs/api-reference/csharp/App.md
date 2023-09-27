#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## App Class

Provides utilities to handle default streaming behaviors and automatic resource cleanup on shutdown.

```csharp
public static class App
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; App

| Methods | |
| :--- | :--- |
| [GetStateManager()](App.GetStateManager().md 'QuixStreams.Streaming.App.GetStateManager()') | Retrieves the state manager for the application |
| [Run(CancellationToken, Action, bool)](App.Run(CancellationToken,Action,bool).md 'QuixStreams.Streaming.App.Run(System.Threading.CancellationToken, System.Action, bool)') | Helper method to handle default streaming behaviors and handle automatic resource cleanup on shutdown |
| [SetStateStorage(IStateStorage)](App.SetStateStorage(IStateStorage).md 'QuixStreams.Streaming.App.SetStateStorage(QuixStreams.State.Storage.IStateStorage)') | Sets the state storage for the app |
