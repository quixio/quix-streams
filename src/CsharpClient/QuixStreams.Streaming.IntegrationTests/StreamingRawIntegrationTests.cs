using System;
using System.Collections.Generic;
using System.Threading;
using QuixStreams.Telemetry.Kafka;
using Quix.TestBase.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Streaming.IntegrationTests
{
    [Collection("Kafka Container Collection")]
    public class StreamingRawIntegrationTests
    {
        private readonly ITestOutputHelper output;
        private readonly KafkaStreamingClient client;

        public StreamingRawIntegrationTests(ITestOutputHelper output, KafkaDockerTestFixture kafkaDockerTestFixture)
        {
            this.output = output;
            QuixStreams.Logging.Factory = output.CreateLoggerFactory();
            client = new KafkaStreamingClient(kafkaDockerTestFixture.BrokerList, kafkaDockerTestFixture.SecurityOptions);
        }



        [Fact(Skip = "Pending fix")]
        public void StreamReadAndWrite()
        {
            var topicName = "streaming-raw-integration-test";
            
                        
            var justCreateMeMyTopic = client.GetRawTopicProducer(topicName);
            justCreateMeMyTopic.Dispose(); // should cause a flush
            Thread.Sleep(5000); // This is only necessary because the container we use for kafka and how a topic creation is handled for the unit test


            var topicConsumer = client.GetRawTopicConsumer(topicName, "Default", AutoOffsetReset.Latest);

            var toSend = new byte[] { 1, 2, 0, 4, 6, 123, 54, 2 };
            var received = new List<byte[]>();


            topicConsumer.OnMessageReceived += (sender, message) =>
            {
                received.Add(message.Value);
            };

            topicConsumer.Subscribe();

            var topicProducer = client.GetRawTopicProducer(topicName);
            topicProducer.Publish(new Raw.RawMessage(toSend));

            SpinWait.SpinUntil(() => received.Count > 0, 5000);

            Console.WriteLine($"received {received.Count} items");
            Assert.Single(received);
            Assert.Equal(toSend, received[0]);


            topicConsumer.Dispose();
            topicProducer.Dispose();
        }


        [Fact(Skip = "Pending fix")]
        public void StreamReadAndWriteWithKey()
        {
            var topicName = "streaming-raw-integration-test2";
            
            var justCreateMeMyTopic = client.GetRawTopicProducer(topicName);
            justCreateMeMyTopic.Dispose(); // should cause a flush
            Thread.Sleep(5000); // This is only necessary because the container we use for kafka and how a topic creation is handled for the unit test

            var topicConsumer = client.GetRawTopicConsumer(topicName, "Default", AutoOffsetReset.Latest);

            var toSend = new byte[] { 1, 2, 0, 4, 6, 123, 54, 2 };
            var received = new List<byte[]>();


            topicConsumer.OnMessageReceived += (sender, message) =>
            {
                received.Add(message.Value);
            };

            topicConsumer.Subscribe();
            var topicProducer = client.GetRawTopicProducer(topicName);
            topicProducer.Publish(new Raw.RawMessage(toSend));

            SpinWait.SpinUntil(() => received.Count > 0, 5000);

            Assert.Single(received);
            Assert.Equal(toSend, received[0]);


            topicConsumer.Dispose();
            topicProducer.Dispose();
        }
    }
}