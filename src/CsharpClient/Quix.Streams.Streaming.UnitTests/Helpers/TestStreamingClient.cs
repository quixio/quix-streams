using System;
using Quix.Streams.Process.Common.Test;
using Quix.Streams.Process.Kafka;
using Quix.Streams.Process.Models;

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

        public ITopicConsumer CreateTopicConsumer()
        {
            return CreateTopiConsumer();
        }

        public ITopicConsumer CreateTopiConsumer()
        {
            this.telemetryKafkaConsumer = new TestTelemetryKafkaConsumer(this.testBroker);

            var topicConsumer = new TopicConsumer(this.telemetryKafkaConsumer);

            return topicConsumer;
        }

        public ITopicProducer CreateTopicProducer()
        {
            this.createKafkaProducer = streamId => new TestTelemetryKafkaProducer(this.testBroker, streamId);

            var topicProducer = new TopicProducer(this.createKafkaProducer);

            return topicProducer;
        }
    }
}
