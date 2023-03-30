#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Raw](QuixStreams.Streaming.Raw.md 'QuixStreams.Streaming.Raw').[RawTopicProducer](RawTopicProducer.md 'QuixStreams.Streaming.Raw.RawTopicProducer')

## RawTopicProducer(string, string, Dictionary<string,string>) Constructor

Initializes a new instance of [RawTopicProducer](RawTopicProducer.md 'QuixStreams.Streaming.Raw.RawTopicProducer')

```csharp
public RawTopicProducer(string brokerAddress, string topicName, System.Collections.Generic.Dictionary<string,string> brokerProperties=null);
```
#### Parameters

<a name='QuixStreams.Streaming.Raw.RawTopicProducer.RawTopicProducer(string,string,System.Collections.Generic.Dictionary_string,string_).brokerAddress'></a>

`brokerAddress` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Address of Kafka cluster.

<a name='QuixStreams.Streaming.Raw.RawTopicProducer.RawTopicProducer(string,string,System.Collections.Generic.Dictionary_string,string_).topicName'></a>

`topicName` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Name of the topic.

<a name='QuixStreams.Streaming.Raw.RawTopicProducer.RawTopicProducer(string,string,System.Collections.Generic.Dictionary_string,string_).brokerProperties'></a>

`brokerProperties` [System.Collections.Generic.Dictionary&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Collections.Generic.Dictionary-2 'System.Collections.Generic.Dictionary`2')

Additional broker properties