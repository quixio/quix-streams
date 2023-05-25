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
| [Run(CancellationToken, Action)](App.Run(CancellationToken,Action).md 'QuixStreams.Streaming.App.Run(System.Threading.CancellationToken, System.Action)') | Helper method to handle default streaming behaviors and handle automatic resource cleanup on shutdown<br/>It also ensures topic consumers defined at the time of invocation are subscribed to receive messages. |
| [SetStateStorage(IStateStorage)](App.SetStateStorage(IStateStorage).md 'QuixStreams.Streaming.App.SetStateStorage(QuixStreams.State.Storage.IStateStorage)') | Sets the state storage for the app |
