using System;
using Quix.Streams.Telemetry.Common.Test;
using Quix.Streams.Telemetry.Kafka;
using Quix.Streams.Telemetry.Models;

namespace Quix.Streams.Streaming.UnitTests
{
    public class TestStreamingClient
    {
        private readonly TestBroker testBroker;
        private TelemetryKafkaConsumer telemetryKafkaConsumer;
        private Func<string, TelemetryKafkaProducer> createKafkaProducer;

        public TestStreamingClient(CodecType codec = CodecType.Protobuf)
        {
            this.testBroker = new TestBroker();

            CodecRegistry.Register(codec);
        }

        public ITopicConsumer GetTopicConsumer()
        {
            return GetTopiConsumer();
        }

        public ITopicConsumer GetTopiConsumer()
        {
            this.telemetryKafkaConsumer = new TestTelemetryKafkaConsumer(this.testBroker);

            var topicConsumer = new TopicConsumer(this.telemetryKafkaConsumer);

            return topicConsumer;
        }

        public ITopicProducer GetTopicProducer()
        {
            this.createKafkaProducer = streamId => new TestTelemetryKafkaProducer(this.testBroker, streamId);

            var topicProducer = new TopicProducer(this.createKafkaProducer);

            return topicProducer;
        }
    }
}
