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
        private KafkaReader kafkaReader;
        private Func<string, KafkaWriter> createKafkaWriter;

        public TestStreamingClient(CodecType codec = CodecType.Protobuf)
        {
            this.testBroker = new TestBroker();

            CodecRegistry.Register(codec);
        }

        public IInputTopic CreateInputTopic()
        {
            return OpenInputTopic();
        }

        public IInputTopic OpenInputTopic()
        {
            this.kafkaReader = new TestKafkaReader(this.testBroker);

            var inputTopic = new InputTopic(this.kafkaReader);

            return inputTopic;
        }

        public IOutputTopic CreateOutputTopic()
        {
            return OpenOutputTopic();
        }

        public IOutputTopic OpenOutputTopic()
        {
            this.createKafkaWriter = streamId => new TestKafkaWriter(this.testBroker, streamId);

            var outputTopic = new OutputTopic(this.createKafkaWriter);

            return outputTopic;
        }
    }
}
