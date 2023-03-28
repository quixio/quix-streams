#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[App](App.md 'QuixStreams.Streaming.App')

## App.Run(CancellationToken, Action) Method

Helper method to handle default streaming behaviors and handle automatic resource cleanup on shutdown  
It also ensures topic consumers defined at the time of invocation are subscribed to receive messages.

```csharp
public static void Run(System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken), System.Action beforeShutdown=null);
```
#### Parameters

<a name='QuixStreams.Streaming.App.Run(System.Threading.CancellationToken,System.Action).cancellationToken'></a>

`cancellationToken` [System.Threading.CancellationToken](https://docs.microsoft.com/en-us/dotnet/api/System.Threading.CancellationToken 'System.Threading.CancellationToken')

The cancellation token to abort. Use when you wish to manually stop streaming for other reason that shutdown.

<a name='QuixStreams.Streaming.App.Run(System.Threading.CancellationToken,System.Action).beforeShutdown'></a>

`beforeShutdown` [System.Action](https://docs.microsoft.com/en-us/dotnet/api/System.Action 'System.Action')

The callback to invoke before shutting down