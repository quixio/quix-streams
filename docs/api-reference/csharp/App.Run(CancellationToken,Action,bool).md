#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[App](App.md 'QuixStreams.Streaming.App')

## App.Run(CancellationToken, Action, bool) Method

Helper method to handle default streaming behaviors and handle automatic resource cleanup on shutdown

```csharp
public static void Run(System.Threading.CancellationToken cancellationToken=default(System.Threading.CancellationToken), System.Action beforeShutdown=null, bool subscribe=true);
```
#### Parameters

<a name='QuixStreams.Streaming.App.Run(System.Threading.CancellationToken,System.Action,bool).cancellationToken'></a>

`cancellationToken` [System.Threading.CancellationToken](https://docs.microsoft.com/en-us/dotnet/api/System.Threading.CancellationToken 'System.Threading.CancellationToken')

The cancellation token to abort. Use when you wish to manually stop streaming for other reason that shutdown.

<a name='QuixStreams.Streaming.App.Run(System.Threading.CancellationToken,System.Action,bool).beforeShutdown'></a>

`beforeShutdown` [System.Action](https://docs.microsoft.com/en-us/dotnet/api/System.Action 'System.Action')

The callback to invoke before shutting down

<a name='QuixStreams.Streaming.App.Run(System.Threading.CancellationToken,System.Action,bool).subscribe'></a>

`subscribe` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Whether the consumer defined should be automatically subscribed to start receiving messages