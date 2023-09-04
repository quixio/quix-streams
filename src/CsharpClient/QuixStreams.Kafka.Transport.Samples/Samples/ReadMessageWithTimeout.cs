using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Timer = System.Timers.Timer;

namespace QuixStreams.Kafka.Transport.Samples.Samples
{
    /// <summary>
    /// Example for simple raw message read (lowest level possible)
    /// Note: Works well with WriteMessages example
    /// </summary>
    public class ReadMessageWithTimeout
    {
        private const string TopicName = Const.MessagesTopic;
        private const string ConsumerGroup = "Test-Subscriber#1";
        private long subscribedCounter; // this is purely here for statistics
        private DateTime nextError = DateTime.UtcNow.AddSeconds(10);

        /// <summary>
        /// Start the reading which is an asynchronous process. See <see cref="NewMessageHandler" />
        /// </summary>
        /// <param name="useConsumerGroup">Whether to use consumer group for testing</param>
        /// <param name="offset">The offset to use (if any)</param>
        /// <returns>Disposable subscriber</returns>
        public IKafkaTransportConsumer Start(bool useConsumerGroup = true, Offset? offset = null)
        {
            var consConfig = new ConsumerConfiguration(Const.BrokerList, useConsumerGroup ? ConsumerGroup : null, new Dictionary<string, string>()
            {
                {"max.poll.interval.ms", "10000"}
            });
            var topicConfig = offset.HasValue ? 
                new ConsumerTopicConfiguration(TopicName, offset.Value) :
                new ConsumerTopicConfiguration(TopicName);
            var kafkaConsumer = new KafkaConsumer(consConfig, topicConfig);
            kafkaConsumer.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            var transportConsumer = new KafkaTransportConsumer(kafkaConsumer);
            this.HookUpStatistics();
            transportConsumer.OnPackageReceived = this.NewMessageHandler;
            kafkaConsumer.Open();

            return transportConsumer;
        }

        private Task NewMessageHandler(TransportPackage args)
        {
            Console.WriteLine("KafkaOutput: {0}", args.KafkaMessage.TopicPartitionOffset);
            if (nextError <= DateTime.UtcNow)
            {
                nextError = DateTime.UtcNow.AddSeconds(40);
                Console.WriteLine("Fake some delay");
                Thread.Sleep(20000);
                Console.WriteLine("Fake delay over");
            }
            Interlocked.Increment(ref this.subscribedCounter);
            return Task.CompletedTask;
        }

        private void HookUpStatistics()
        {
            var sw = Stopwatch.StartNew();

            var timer = new Timer
            {
                AutoReset = false,
                Interval = 1000
            };

            timer.Elapsed += (s, e) =>
            {
                var elapsed = sw.Elapsed;
                var published = Interlocked.Read(ref this.subscribedCounter);


                var publishedPerMin = published / elapsed.TotalMilliseconds * 60000;

                Console.WriteLine($"Subscribed Messages: {published:N0}, {publishedPerMin:N2}/min");
                timer.Start();
            };

            timer.Start();
        }
    }
}