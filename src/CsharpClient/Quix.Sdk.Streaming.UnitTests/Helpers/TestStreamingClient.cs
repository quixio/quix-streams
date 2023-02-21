using Quix.Sdk.Process;
using Quix.Sdk.Process.Kafka;
using Quix.Sdk.Process.Common.Test;
using System;
using Quix.Sdk.Streaming;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Transport.Fw;

namespace Quix.Sdk.Streaming.UnitTests
{
    public class TestStreamingClient
    {
        private readonly TestBroker testBroker;
        private TelemetryKafkaConsumer telemetryKafkaConsumer;
        private Func<string, TelemetryKafkaProducer> createKafkaWriter;

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
            this.createKafkaWriter = streamId => new TestTelemetryKafkaProducer(this.testBroker, streamId);

            var topicProducer = new TopicProducer(this.createKafkaWriter);

            return topicProducer;
        }
    }
}
