#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[AppStateManager](AppStateManager.md 'QuixStreams.Streaming.States.AppStateManager')

## AppStateManager(IStateStorage, ILoggerFactory) Constructor

Initializes a new instance of the AppStateManager class.

```csharp
public AppStateManager(QuixStreams.State.Storage.IStateStorage storage, Microsoft.Extensions.Logging.ILoggerFactory loggerFactory=null);
```
#### Parameters

<a name='QuixStreams.Streaming.States.AppStateManager.AppStateManager(QuixStreams.State.Storage.IStateStorage,Microsoft.Extensions.Logging.ILoggerFactory).storage'></a>

`storage` [QuixStreams.State.Storage.IStateStorage](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.State.Storage.IStateStorage 'QuixStreams.State.Storage.IStateStorage')

The state storage the use for the application state

<a name='QuixStreams.Streaming.States.AppStateManager.AppStateManager(QuixStreams.State.Storage.IStateStorage,Microsoft.Extensions.Logging.ILoggerFactory).loggerFactory'></a>

`loggerFactory` [Microsoft.Extensions.Logging.ILoggerFactory](https://docs.microsoft.com/en-us/dotnet/api/Microsoft.Extensions.Logging.ILoggerFactory 'Microsoft.Extensions.Logging.ILoggerFactory')

The logger factory to use