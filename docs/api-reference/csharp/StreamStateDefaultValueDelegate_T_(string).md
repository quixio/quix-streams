#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.States](QuixStreams.Streaming.States.md 'QuixStreams.Streaming.States')

## StreamStateDefaultValueDelegate<T>(string) Delegate

Represents a method that returns the default value for a given key.

```csharp
public delegate T StreamStateDefaultValueDelegate<out T>(string missingStateKey);
```
#### Type parameters

<a name='QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_(string).T'></a>

`T`

The type of the default value.
#### Parameters

<a name='QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_(string).missingStateKey'></a>

`missingStateKey` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The name of the key being accessed.

#### Returns
[T](StreamStateDefaultValueDelegate_T_(string).md#QuixStreams.Streaming.States.StreamStateDefaultValueDelegate_T_(string).T 'QuixStreams.Streaming.States.StreamStateDefaultValueDelegate<T>(string).T')  
The default value for the specified key.