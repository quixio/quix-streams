#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient')

## QuixStreamingClient(string, bool, IDictionary<string,string>, bool, HttpClient) Constructor

Initializes a new instance of [KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient') that is capable of creating topic consumer and producers

```csharp
public QuixStreamingClient(string token=null, bool autoCreateTopics=true, System.Collections.Generic.IDictionary<string,string> properties=null, bool debug=false, System.Net.Http.HttpClient httpClient=null);
```
#### Parameters

<a name='QuixStreams.Streaming.QuixStreamingClient.QuixStreamingClient(string,bool,System.Collections.Generic.IDictionary_string,string_,bool,System.Net.Http.HttpClient).token'></a>

`token` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The token to use when talking to Quix. When not provided, Quix__Sdk__Token environment variable will be used

<a name='QuixStreams.Streaming.QuixStreamingClient.QuixStreamingClient(string,bool,System.Collections.Generic.IDictionary_string,string_,bool,System.Net.Http.HttpClient).autoCreateTopics'></a>

`autoCreateTopics` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Whether topics should be auto created if they don't exist yet

<a name='QuixStreams.Streaming.QuixStreamingClient.QuixStreamingClient(string,bool,System.Collections.Generic.IDictionary_string,string_,bool,System.Net.Http.HttpClient).properties'></a>

`properties` [System.Collections.Generic.IDictionary&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2')

Additional broker properties

<a name='QuixStreams.Streaming.QuixStreamingClient.QuixStreamingClient(string,bool,System.Collections.Generic.IDictionary_string,string_,bool,System.Net.Http.HttpClient).debug'></a>

`debug` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Whether debugging should be enabled

<a name='QuixStreams.Streaming.QuixStreamingClient.QuixStreamingClient(string,bool,System.Collections.Generic.IDictionary_string,string_,bool,System.Net.Http.HttpClient).httpClient'></a>

`httpClient` [System.Net.Http.HttpClient](https://docs.microsoft.com/en-us/dotnet/api/System.Net.Http.HttpClient 'System.Net.Http.HttpClient')

The http client to use