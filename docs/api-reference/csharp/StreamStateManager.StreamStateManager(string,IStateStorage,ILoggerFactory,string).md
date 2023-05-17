#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States').[StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager')

## StreamStateManager(string, IStateStorage, ILoggerFactory, string) Constructor

Initializes a new instance of the [StreamStateManager](StreamStateManager.md 'QuixStreams.Streaming.States.StreamStateManager') class with the specified parameters.

```csharp
public StreamStateManager(string streamId, QuixStreams.State.Storage.IStateStorage stateStorage, Microsoft.Extensions.Logging.ILoggerFactory loggerFactory, string logPrefix);
```
#### Parameters

<a name='QuixStreams.Streaming.States.StreamStateManager.StreamStateManager(string,QuixStreams.State.Storage.IStateStorage,Microsoft.Extensions.Logging.ILoggerFactory,string).streamId'></a>

`streamId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The ID of the stream.

<a name='QuixStreams.Streaming.States.StreamStateManager.StreamStateManager(string,QuixStreams.State.Storage.IStateStorage,Microsoft.Extensions.Logging.ILoggerFactory,string).stateStorage'></a>

`stateStorage` [QuixStreams.State.Storage.IStateStorage](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.State.Storage.IStateStorage 'QuixStreams.State.Storage.IStateStorage')

The state storage to use.

<a name='QuixStreams.Streaming.States.StreamStateManager.StreamStateManager(string,QuixStreams.State.Storage.IStateStorage,Microsoft.Extensions.Logging.ILoggerFactory,string).loggerFactory'></a>

`loggerFactory` [Microsoft.Extensions.Logging.ILoggerFactory](https://docs.microsoft.com/en-us/dotnet/api/Microsoft.Extensions.Logging.ILoggerFactory 'Microsoft.Extensions.Logging.ILoggerFactory')

The logger factory used for creating loggers.

<a name='QuixStreams.Streaming.States.StreamStateManager.StreamStateManager(string,QuixStreams.State.Storage.IStateStorage,Microsoft.Extensions.Logging.ILoggerFactory,string).logPrefix'></a>

`logPrefix` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The prefix to be used in log messages.