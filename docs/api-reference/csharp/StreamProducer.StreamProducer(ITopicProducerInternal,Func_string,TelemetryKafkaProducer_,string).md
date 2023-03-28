#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[StreamProducer](StreamProducer.md 'QuixStreams.Streaming.StreamProducer')

## StreamProducer(ITopicProducerInternal, Func<string,TelemetryKafkaProducer>, string) Constructor

Initializes a new instance of [StreamProducer](StreamProducer.md 'QuixStreams.Streaming.StreamProducer')

```csharp
internal StreamProducer(QuixStreams.Streaming.ITopicProducerInternal topicProducer, System.Func<string,QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer> createKafkaProducer, string streamId=null);
```
#### Parameters

<a name='QuixStreams.Streaming.StreamProducer.StreamProducer(QuixStreams.Streaming.ITopicProducerInternal,System.Func_string,QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer_,string).topicProducer'></a>

`topicProducer` [ITopicProducerInternal](ITopicProducerInternal.md 'QuixStreams.Streaming.ITopicProducerInternal')

The producer which owns the [StreamProducer](StreamProducer.md 'QuixStreams.Streaming.StreamProducer')

<a name='QuixStreams.Streaming.StreamProducer.StreamProducer(QuixStreams.Streaming.ITopicProducerInternal,System.Func_string,QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer_,string).createKafkaProducer'></a>

`createKafkaProducer` [System.Func&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')[QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer 'QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')

Function factory to create a Kafka producer from Telemetry layer.

<a name='QuixStreams.Streaming.StreamProducer.StreamProducer(QuixStreams.Streaming.ITopicProducerInternal,System.Func_string,QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer_,string).streamId'></a>

`streamId` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Optional. Stream Id of the stream created