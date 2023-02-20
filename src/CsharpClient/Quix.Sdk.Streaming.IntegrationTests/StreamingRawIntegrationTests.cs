using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using FluentAssertions;
using Quix.Sdk.Process.Configuration;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming.Configuration;
using Quix.Sdk.Streaming;
using Quix.Sdk.Process.Models.Utility;
using Xunit;
using System;
using System.Text;
using System.Text.Unicode;
using System.Threading.Tasks;
using Quix.Sdk.Process.Kafka;
using Quix.TestBase.Extensions;
using Xunit.Abstractions;
using AutoOffsetReset = Quix.Sdk.Process.Kafka.AutoOffsetReset;
using System.Collections.ObjectModel;

namespace Quix.Sdk.Streaming.IntegrationTests
{
    [Collection("Kafka Container Collection")]
    public class StreamingRawIntegrationTests
    {
        private readonly ITestOutputHelper output;
        private readonly KafkaStreamingClient client;

        public StreamingRawIntegrationTests(ITestOutputHelper output, KafkaDockerTestFixture kafkaDockerTestFixture)
        {
            this.output = output;
            Quix.Sdk.Logging.Factory = output.CreateLoggerFactory();
            client = new KafkaStreamingClient(kafkaDockerTestFixture.BrokerList, kafkaDockerTestFixture.SecurityOptions);
        }



        [Fact(Skip = "Pending fix")]
        public void StreamReadAndWrite()
        {
            var topicName = "streaming-raw-integration-test";
            
                        
            var justCreateMeMyTopic = client.OpenRawOutputTopic(topicName);
            justCreateMeMyTopic.Dispose(); // should cause a flush
            Thread.Sleep(5000); // This is only necessary because the container we use for kafka and how a topic creation is handled for the unit test


            var inputTopic = client.OpenRawInputTopic(topicName, "Default", AutoOffsetReset.Latest);

            var toSend = new byte[] { 1, 2, 0, 4, 6, 123, 54, 2 };
            var received = new List<byte[]>();


            inputTopic.OnMessageRead += (sender, message) =>
            {
                received.Add(message.Value);
            };

            inputTopic.StartReading();

            var outputTopic = client.OpenRawOutputTopic(topicName);
            outputTopic.Write(new Raw.RawMessage(toSend));

            SpinWait.SpinUntil(() => received.Count > 0, 5000);

            Console.WriteLine($"received {received.Count} items");
            Assert.Single(received);
            Assert.Equal(toSend, received[0]);


            inputTopic.Dispose();
            outputTopic.Dispose();
        }


        [Fact(Skip = "Pending fix")]
        public void StreamReadAndWriteWithKey()
        {
            var topicName = "streaming-raw-integration-test2";
            
            var justCreateMeMyTopic = client.OpenRawOutputTopic(topicName);
            justCreateMeMyTopic.Dispose(); // should cause a flush
            Thread.Sleep(5000); // This is only necessary because the container we use for kafka and how a topic creation is handled for the unit test

            var inputTopic = client.OpenRawInputTopic(topicName, "Default", AutoOffsetReset.Latest);

            var toSend = new byte[] { 1, 2, 0, 4, 6, 123, 54, 2 };
            var received = new List<byte[]>();


            inputTopic.OnMessageRead += (sender, message) =>
            {
                received.Add(message.Value);
            };

            inputTopic.StartReading();
            var outputTopic = client.OpenRawOutputTopic(topicName);
            outputTopic.Write(new Raw.RawMessage(toSend));

            SpinWait.SpinUntil(() => received.Count > 0, 5000);

            Assert.Single(received);
            Assert.Equal(toSend, received[0]);


            inputTopic.Dispose();
            outputTopic.Dispose();
        }
    }
}