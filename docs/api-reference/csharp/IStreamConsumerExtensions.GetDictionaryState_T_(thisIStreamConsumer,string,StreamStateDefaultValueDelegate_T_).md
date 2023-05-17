#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[IStreamConsumerExtensions](IStreamConsumerExtensions.md 'QuixStreams.Streaming.IStreamConsumerExtensions')

## IStreamConsumerExtensions.GetDictionaryState<T>(this IStreamConsumer, string, StreamStateDefaultValueDelegate<T>) Method

Gets the dictionary type stream state for the specified storage name using the provided default value factory.

```csharp
public static QuixStreams.Streaming.States.StreamState<T> GetDictionaryState<T>(this QuixStreams.Streaming.IStreamConsumer streamConsumer, string storageName, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T> defaultValueFactory);
```
#### Type parameters

<a name='QuixStreams.Streaming.IStreamConsumerExtensions.GetDictionaryState_T_(thisQuixStreams.Streaming.IStreamConsumer,string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T'></a>

`T`

The type of the stream state value.
#### Parameters

<a name='QuixStreams.Streaming.IStreamConsumerExtensions.GetDictionaryState_T_(thisQuixStreams.Streaming.IStreamConsumer,string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).streamConsumer'></a>

`streamConsumer` [IStreamConsumer](IStreamConsumer.md 'QuixStreams.Streaming.IStreamConsumer')

The stream consumer to get the state for

<a name='QuixStreams.Streaming.IStreamConsumerExtensions.GetDictionaryState_T_(thisQuixStreams.Streaming.IStreamConsumer,string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).storageName'></a>

`storageName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The name of the storage.

<a name='QuixStreams.Streaming.IStreamConsumerExtensions.GetDictionaryState_T_(thisQuixStreams.Streaming.IStreamConsumer,string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).defaultValueFactory'></a>

`defaultValueFactory` [QuixStreams.Streaming.States.StreamStateDefaultValueDelegate&lt;](StreamStateDefaultValueDelegate_T_(string).md 'QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>(string)')[T](IStreamConsumerExtensions.GetDictionaryState_T_(thisIStreamConsumer,string,StreamStateDefaultValueDelegate_T_).md#QuixStreams.Streaming.IStreamConsumerExtensions.GetDictionaryState_T_(thisQuixStreams.Streaming.IStreamConsumer,string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T 'QuixStreams.Streaming.IStreamConsumerExtensions.GetDictionaryState<T>(this QuixStreams.Streaming.IStreamConsumer, string, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>).T')[&gt;](StreamStateDefaultValueDelegate_T_(string).md 'QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>(string)')

A delegate that creates the default value for the stream state when a previously not set key is accessed.

#### Returns
[QuixStreams.Streaming.States.StreamState&lt;](StreamState_T_.md 'QuixStreams.Streaming.States.StreamState<T>')[T](IStreamConsumerExtensions.GetDictionaryState_T_(thisIStreamConsumer,string,StreamStateDefaultValueDelegate_T_).md#QuixStreams.Streaming.IStreamConsumerExtensions.GetDictionaryState_T_(thisQuixStreams.Streaming.IStreamConsumer,string,QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_).T 'QuixStreams.Streaming.IStreamConsumerExtensions.GetDictionaryState<T>(this QuixStreams.Streaming.IStreamConsumer, string, QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>).T')[&gt;](StreamState_T_.md 'QuixStreams.Streaming.States.StreamState<T>')  
The dictionary stream state for the specified storage name using the provided default value factory.