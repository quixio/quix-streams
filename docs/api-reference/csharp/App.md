#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming')

## App Class

Helper class to handle default streaming behaviors and handle automatic resource cleanup on shutdown

```csharp
public static class App
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; App

| Methods | |
| :--- | :--- |
| [Run(CancellationToken, Action)](App.Run(CancellationToken,Action).md 'QuixStreams.Streaming.App.Run(System.Threading.CancellationToken, System.Action)') | Helper method to handle default streaming behaviors and handle automatic resource cleanup on shutdown<br/>It also ensures topic consumers defined at the time of invocation are subscribed to receive messages. |
