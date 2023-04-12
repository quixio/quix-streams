#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient')

## KafkaStreamingClient(string, SecurityOptions, IDictionary<string,string>, bool) Constructor

Initializes a new instance of [KafkaStreamingClient](KafkaStreamingClient.md 'QuixStreams.Streaming.KafkaStreamingClient')

```csharp
public KafkaStreamingClient(string brokerAddress, QuixStreams.Streaming.Configuration.SecurityOptions securityOptions=null, System.Collections.Generic.IDictionary<string,string> properties=null, bool debug=false);
```
#### Parameters

<a name='QuixStreams.Streaming.KafkaStreamingClient.KafkaStreamingClient(string,QuixStreams.Streaming.Configuration.SecurityOptions,System.Collections.Generic.IDictionary_string,string_,bool).brokerAddress'></a>

`brokerAddress` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Address of Kafka cluster.

<a name='QuixStreams.Streaming.KafkaStreamingClient.KafkaStreamingClient(string,QuixStreams.Streaming.Configuration.SecurityOptions,System.Collections.Generic.IDictionary_string,string_,bool).securityOptions'></a>

`securityOptions` [SecurityOptions](SecurityOptions.md 'QuixStreams.Streaming.Configuration.SecurityOptions')

Optional security options.

<a name='QuixStreams.Streaming.KafkaStreamingClient.KafkaStreamingClient(string,QuixStreams.Streaming.Configuration.SecurityOptions,System.Collections.Generic.IDictionary_string,string_,bool).properties'></a>

`properties` [System.Collections.Generic.IDictionary&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.IDictionary-2 'System.Collections.Generic.IDictionary`2')

Additional broker properties

<a name='QuixStreams.Streaming.KafkaStreamingClient.KafkaStreamingClient(string,QuixStreams.Streaming.Configuration.SecurityOptions,System.Collections.Generic.IDictionary_string,string_,bool).debug'></a>

`debug` [System.Boolean](https://docs.microsoft.com/en-us/dotnet/api/System.Boolean 'System.Boolean')

Whether debugging should be enabled