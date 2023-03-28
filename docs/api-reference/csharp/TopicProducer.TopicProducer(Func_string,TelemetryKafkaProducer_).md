#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[TopicProducer](TopicProducer.md 'QuixStreams.Streaming.TopicProducer')

## TopicProducer(Func<string,TelemetryKafkaProducer>) Constructor

Initializes a new instance of [TopicProducer](TopicProducer.md 'QuixStreams.Streaming.TopicProducer')

```csharp
public TopicProducer(System.Func<string,QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer> createKafkaProducer);
```
#### Parameters

<a name='QuixStreams.Streaming.TopicProducer.TopicProducer(System.Func_string,QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer_).createKafkaProducer'></a>

`createKafkaProducer` [System.Func&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')[System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')[,](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')[QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer](https://docs.microsoft.com/en-us/dotnet/api/QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer 'QuixStreams.Telemetry.Kafka.TelemetryKafkaProducer')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Func-2 'System.Func`2')

Function factory to create a Kafka producer from Telemetry layer.