#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Utils](QuixStreams.Streaming.Utils.md 'QuixStreams.Streaming.Utils').[QuixUtils](QuixUtils.md 'QuixStreams.Streaming.Utils.QuixUtils')

## QuixUtils.TryGetWorkspaceIdPrefix(string, string) Method

Extract WorkspaceId from TopicId information

```csharp
public static bool TryGetWorkspaceIdPrefix(string topicId, out string workspaceId);
```
#### Parameters

<a name='QuixStreams.Streaming.Utils.QuixUtils.TryGetWorkspaceIdPrefix(string,string).topicId'></a>

`topicId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Topic Id

<a name='QuixStreams.Streaming.Utils.QuixUtils.TryGetWorkspaceIdPrefix(string,string).workspaceId'></a>

`workspaceId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Workspace Id (output)

#### Returns
[System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')  
Whether the function could extract the Workspace Id information