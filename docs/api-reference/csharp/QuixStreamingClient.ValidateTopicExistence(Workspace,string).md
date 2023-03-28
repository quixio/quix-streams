#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient')

## QuixStreamingClient.ValidateTopicExistence(Workspace, string) Method

Validate that topic exists within the workspace and create it if it doesn't

```csharp
private System.Threading.Tasks.Task<string> ValidateTopicExistence(QuixStreams.Streaming.QuixApi.Portal.Workspace workspace, string topicIdOrName);
```
#### Parameters

<a name='QuixStreams.Streaming.QuixStreamingClient.ValidateTopicExistence(QuixStreams.Streaming.QuixApi.Portal.Workspace,string).workspace'></a>

`workspace` [Workspace](Workspace.md 'QuixStreams.Streaming.QuixApi.Portal.Workspace')

Workspace

<a name='QuixStreams.Streaming.QuixStreamingClient.ValidateTopicExistence(QuixStreams.Streaming.QuixApi.Portal.Workspace,string).topicIdOrName'></a>

`topicIdOrName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Topic Id or Topic Name

#### Returns
[System.Threading.Tasks.Task&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Threading.Tasks.Task-1 'System.Threading.Tasks.Task`1')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Threading.Tasks.Task-1 'System.Threading.Tasks.Task`1')  
Topic Id