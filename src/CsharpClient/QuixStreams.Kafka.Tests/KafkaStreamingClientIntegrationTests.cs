using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using Quix.TestBase.Extensions;
using QuixStreams.Kafka;
using QuixStreams;
using Xunit;
using Xunit.Abstractions;

namespace QuixStreams.Transport.Kafka.Tests
{
    [Collection("Kafka Container Collection")]
    public class KafkaStreamingClientIntegrationTests
    {
        private readonly ITestOutputHelper output;
        private readonly KafkaDockerTestFixture kafkaDockerTestFixture;
        private int MaxTestRetry = 3;

        public KafkaStreamingClientIntegrationTests(ITestOutputHelper output, KafkaDockerTestFixture kafkaDockerTestFixture)
        {
            this.output = output;
            this.kafkaDockerTestFixture = kafkaDockerTestFixture;
            QuixStreams.Logging.Factory = output.CreateLoggerFactory();
            output.WriteLine($"Created client with brokerlist '{kafkaDockerTestFixture.BrokerList}'");
        }
        

        [Theory]
        [InlineData(1, 1000)]
        public async Task ReadAndWriteMessage_MessagesReceivedInOrder(int repeatCount, int messageCount)
        {
            for (var testCount = 0; testCount < repeatCount; testCount++)
            {
                this.output.WriteLine($"--------------- TEST {testCount} ---------------");
                var topic = nameof(ReadAndWriteMessage_MessagesReceivedInOrder) + testCount;
                await EnsureTopic(topic, 1);
                var messagesReceived = new List<KafkaMessage>();
                var messagesToSend = new List<KafkaMessage>();
                for (int i = 0; i < messageCount; i++)
                {
                    var key = Encoding.UTF8.GetBytes($"Key{i}");
                    var value = Encoding.UTF8.GetBytes($"Value{i}");
                    var header = new Dictionary<string, byte[]>() { { "ModelKey", Encoding.UTF8.GetBytes("Test") } };
                    messagesToSend.Add(new KafkaMessage(key, value, header));
                }

                using (var consumer = new KafkaConsumer(new ConsumerConfiguration(this.kafkaDockerTestFixture.BrokerList), new ConsumerTopicConfiguration(topic)))
                {
                    int lastMsgRead = -1;
                    var msgReceivedOutOfOrder = false;
                    consumer.MessageReceived += message =>
                    {
                        messagesReceived.Add(message);
                        var msgKey = Encoding.UTF8.GetString(message.Key).Replace("Key", "");
                        this.output.WriteLine($"Received Message {msgKey}");
                        var parsedValue = int.Parse(msgKey);
                        if (parsedValue != lastMsgRead + 1)
                        {
                            msgReceivedOutOfOrder = true;
                            this.output.WriteLine("Message received out of order!");
                            throw new Exception("Message received out of order.");
                        }

                        lastMsgRead = parsedValue;

                        return Task.CompletedTask;
                    };
                    consumer.Open();

                    using (var producer = new KafkaProducer(new ProducerConfiguration(this.kafkaDockerTestFixture.BrokerList), new ProducerTopicConfiguration(topic)))
                    {
                        producer.Open();
                        for (var index = 0; index < messagesToSend.Count; index++)
                        {
                            var kafkaMessage = messagesToSend[index];
                            var indexCached = index;
                            producer.Publish(kafkaMessage).ContinueWith(y =>
                            {
                                this.output.WriteLine($"Produced message {indexCached}");
                            });
                        }

                        producer.Flush(CancellationToken.None);
                    }


                    var maxWait = DateTime.UtcNow.AddSeconds(20);

                    var sw = Stopwatch.StartNew();
                    while (!msgReceivedOutOfOrder && messagesReceived.Count != messagesToSend.Count && DateTime.UtcNow <= maxWait)
                    {
                        await Task.Delay(100);
                    }

                    this.output.WriteLine("Waited {0}, saw {1} messages", sw.Elapsed, messagesReceived.Count);
                }

                messagesReceived.Should().BeEquivalentTo(messagesToSend,
                    o =>
                    {
                        return o.WithStrictOrdering().Including(y => y.Key).Including(y => y.Value)
                            .Including(y => y.Headers);
                    });
            }
        }

        [Fact]
        public async Task Commit_LastReadShouldBeCommitted()
        {
            var topic = nameof(Commit_LastReadShouldBeCommitted);
            await EnsureTopic(topic, 1);
            KafkaMessage lastMessageReceived = null;
            var messagesToSend = new List<KafkaMessage>();
            for (int i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"Key{i}");
                var value = Encoding.UTF8.GetBytes($"Value{i}");
                var header = new Dictionary<string, byte[]>() { { "ModelKey", Encoding.UTF8.GetBytes("Test") } };
                messagesToSend.Add(new KafkaMessage(key, value, header));
            }

            using (var consumer = new KafkaConsumer(new ConsumerConfiguration(this.kafkaDockerTestFixture.BrokerList, consumerProperties: new Dictionary<string, string>() {{"enable.auto.commit", "false"}}), new ConsumerTopicConfiguration(topic)))
            {
                var msgReceived = 0;
                consumer.MessageReceived += message =>
                {
                    lastMessageReceived = message;
                    msgReceived++;
                    return Task.CompletedTask;
                };
                consumer.Open();

                using (var producer = new KafkaProducer(new ProducerConfiguration(this.kafkaDockerTestFixture.BrokerList), new ProducerTopicConfiguration(topic)))
                {
                    producer.Open();
                    for (var index = 0; index < messagesToSend.Count; index++)
                    {
                        var kafkaMessage = messagesToSend[index];
                        var indexCached = index;
                        producer.Publish(kafkaMessage).ContinueWith(y =>
                        {
                            this.output.WriteLine($"Produced message {indexCached}");
                        });
                    }

                    producer.Flush(CancellationToken.None);
                }


                var maxWait = DateTime.UtcNow.AddSeconds(20);

                var sw = Stopwatch.StartNew();
                while (msgReceived != messagesToSend.Count && DateTime.UtcNow <= maxWait)
                {
                    await Task.Delay(100);
                }

                this.output.WriteLine("Waited {0}, saw {1} messages", sw.Elapsed, msgReceived);

                msgReceived.Should().Be(messagesToSend.Count);

                CommittingEventArgs committingEventArgs = null;
                CommittedEventArgs committedEventArgs = null;
                consumer.Committing += (sender, args) =>
                {
                    committingEventArgs = args;
                };
                
                consumer.Committed += (sender, args) =>
                {
                    committedEventArgs = args;
                };

                var expectedOffSet = new TopicPartitionOffset(lastMessageReceived.TopicPartitionOffset.TopicPartition, lastMessageReceived.TopicPartitionOffset.Offset + 1);
                
                consumer.Commit();
                committingEventArgs.Should().NotBeNull();
                committingEventArgs.Committing.Count.Should().Be(1);
                committingEventArgs.Committing.First().Should().BeEquivalentTo(expectedOffSet);
                committedEventArgs.Should().NotBeNull();
                committedEventArgs.Committed.Offsets.Count.Should().Be(1);
                committedEventArgs.Committed.Error.IsError.Should().BeFalse();
                committedEventArgs.Committed.Offsets.First().TopicPartitionOffset.Should().BeEquivalentTo(expectedOffSet);
            }
        }


        [Fact]
        public async Task CreateProducer_ShouldHaveMaxMessageSizeSet()
        {
            var topic = nameof(CreateProducer_ShouldHaveMaxMessageSizeSet);
            await EnsureTopic(topic, 1);
            using (var producer = new KafkaProducer(new ProducerConfiguration(this.kafkaDockerTestFixture.BrokerList), new ProducerTopicConfiguration(topic)))
            {
                this.output.WriteLine("Max message size is {0}", producer.MaxMessageSizeBytes);
                producer.MaxMessageSizeBytes.Should().BeGreaterThan(0);
            }
        }

        private async Task EnsureTopic(string topic, int partitionCount)
        {
            try
            {
                await kafkaDockerTestFixture.AdminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification() { Name = topic, NumPartitions = partitionCount } });
                this.output.WriteLine($"Created topic {topic} with desired {partitionCount} partitions");
            }
            catch (Exception ex)
            {
                // it exists
            }

            var metadata = kafkaDockerTestFixture.AdminClient.GetMetadata(topic, TimeSpan.FromSeconds(5));
            var existingTopic = metadata.Topics.First();

            if (existingTopic.Partitions.Count == partitionCount)
            {
                this.output.WriteLine($"Found topic {topic} with desired {partitionCount} partitions");
                return;
            }
            
            if (existingTopic.Partitions.Count > partitionCount) throw new InvalidOperationException("The topic has more partitions than required, you need to manually delete it first");
            
            try
            {
                this.output.WriteLine($"Found topic {topic} with less than desired {partitionCount} partitions, creating up to {partitionCount}");
                await kafkaDockerTestFixture.AdminClient.CreatePartitionsAsync(new PartitionsSpecification[] { new PartitionsSpecification() { Topic = topic, IncreaseTo = partitionCount } });
                await Task.Delay(100); // apparently waiting for the task above is not enough, so introducing some artificial delay
            }
            catch (CreatePartitionsException ex)
            {
                if (!ex.Message.Contains($"Topic already has {partitionCount} partitions")) throw;
            }
            await EnsureTopic(topic, partitionCount);
        }

    }
}