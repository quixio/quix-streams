#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw')

## RawMessage Class

The message read from topic without any transformation

```csharp
public class RawMessage
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; RawMessage

| Constructors | |
| :--- | :--- |
| [RawMessage(byte[], byte[])](RawMessage.RawMessage(byte[],byte[]).md 'QuixStreams.Streaming.Raw.RawMessage.RawMessage(byte[], byte[])') | Initializes a new instance of [RawMessage](RawMessage.md 'QuixStreams.Streaming.Raw.RawMessage') |
| [RawMessage(byte[])](RawMessage.RawMessage(byte[]).md 'QuixStreams.Streaming.Raw.RawMessage.RawMessage(byte[])') | Initializes a new instance of [RawMessage](RawMessage.md 'QuixStreams.Streaming.Raw.RawMessage') without a Key |

| Fields | |
| :--- | :--- |
| [Key](RawMessage.Key.md 'QuixStreams.Streaming.Raw.RawMessage.Key') | The optional key of the message. Depending on broker and message it is not guaranteed |
| [Value](RawMessage.Value.md 'QuixStreams.Streaming.Raw.RawMessage.Value') | The value of the message |

| Properties | |
| :--- | :--- |
| [Metadata](RawMessage.Metadata.md 'QuixStreams.Streaming.Raw.RawMessage.Metadata') | The broker specific optional metadata |
