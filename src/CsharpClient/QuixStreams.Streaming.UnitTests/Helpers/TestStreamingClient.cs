using System;
using System.Collections.Generic;
using QuixStreams.Streaming.Raw;
using QuixStreams.Telemetry.Common.Test;
using QuixStreams.Telemetry.Kafka;
using QuixStreams.Telemetry.Models;
using QuixStreams.Transport.Fw;

namespace QuixStreams.Streaming.UnitTests
{
    public class TestStreamingClient : IQuixStreamingClient, IKafkaStreamingClient
    {
        private readonly TimeSpan publishDelay;
        private TelemetryKafkaConsumer telemetryKafkaConsumer;
        private Func<string, TelemetryKafkaProducer> createKafkaProducer;
        private Dictionary<string, TestBroker> brokers = new Dictionary<string, TestBroker>();


        public TestStreamingClient(CodecType codec = CodecType.Protobuf, TimeSpan publishDelay = default)
        {
            this.publishDelay = publishDelay;
            CodecRegistry.Register(codec);
        }

        public ITopicConsumer GetTopicConsumer()
        {
            return GetTopicConsumer("DEFAULT");
        }

        public ITopicConsumer GetTopicConsumer(string topic)
        {
            var broker = GetBroker(topic);
            this.telemetryKafkaConsumer = new TestTelemetryKafkaConsumer(broker);

            var topicConsumer = new TopicConsumer(this.telemetryKafkaConsumer);

            return topicConsumer;
        }
        
        public ITopicProducer GetTopicProducer()
        {
            return GetTopicProducer("DEFAULT");
        }

        public ITopicProducer GetTopicProducer(string topic)
        {
            var broker = GetBroker(topic);
            this.createKafkaProducer = streamId => new TestTelemetryKafkaProducer(broker, streamId);

            var topicProducer = new TopicProducer(this.createKafkaProducer);
            

            return topicProducer;
        }

        private TestBroker GetBroker(string topic)
        {
            if (this.brokers.TryGetValue(topic, out var broker)) return broker;
            broker = new TestBroker(publishDelay: publishDelay);
            this.brokers[topic] = broker;
            return broker;
        }

        ITopicConsumer IQuixStreamingClient.GetTopicConsumer(string topicIdOrName, string consumerGroup, CommitOptions options,
            AutoOffsetReset autoOffset)
        {
            return GetTopicConsumer(topicIdOrName);
        }

        IRawTopicConsumer IKafkaStreamingClient.GetRawTopicConsumer(string topic, string consumerGroup, AutoOffsetReset? autoOffset)
        {
            throw new NotImplementedException();
        }

        IRawTopicProducer IKafkaStreamingClient.GetRawTopicProducer(string topic)
        {
            throw new NotImplementedException();
        }

        ITopicProducer IKafkaStreamingClient.GetTopicProducer(string topic)
        {
            return GetTopicProducer(topic);
        }

        ITopicConsumer IKafkaStreamingClient.GetTopicConsumer(string topic, string consumerGroup, CommitOptions options,
            AutoOffsetReset autoOffset)
        {
            return GetTopicConsumer(topic);
        }

        IRawTopicConsumer IQuixStreamingClient.GetRawTopicConsumer(string topicIdOrName, string consumerGroup,
            AutoOffsetReset? autoOffset)
        {
            throw new NotImplementedException();
        }

        IRawTopicProducer IQuixStreamingClient.GetRawTopicProducer(string topicIdOrName)
        {
            throw new NotImplementedException();
        }

        ITopicProducer IQuixStreamingClient.GetTopicProducer(string topicIdOrName)
        {
            return GetTopicProducer(topicIdOrName);
        }
    }
}
