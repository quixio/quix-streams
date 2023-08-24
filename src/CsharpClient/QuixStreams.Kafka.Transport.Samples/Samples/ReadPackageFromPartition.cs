using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Timer = System.Timers.Timer;

namespace QuixStreams.Kafka.Transport.Samples.Samples
{
    /// <summary>
    /// Read telemetry data and monitor the output
    /// </summary>
    public class ReadPackageFromPartition
    {
        private const string TopicName = Const.PartitionedPackageTestTopic;
        private const string ConsumerGroup = "Test-Subscriber#3";

        private long consumedCounter; // this is purely here for statistics

        /// <summary>
        /// Start the reading stream which is an asynchronous process.
        /// </summary>
        /// <returns>Disposable output</returns>
        public IKafkaTransportConsumer Start(Partition partition, Offset offset)
        {
            var (transportConsumer, kafkaConsumer) = this.CreateKafkaOutput(partition, offset);
            this.HookUpStatistics();
            transportConsumer.OnPackageReceived = this.NewPackageHandler;
            kafkaConsumer.Open();
            return transportConsumer;
        }

        private Task NewPackageHandler(TransportPackage obj)
        {
            Interlocked.Increment(ref this.consumedCounter);
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
                var consumed = Interlocked.Read(ref this.consumedCounter);


                var consumedPerMin = consumed / elapsed.TotalMilliseconds * 60000;

                Console.WriteLine($"Consumed Packages: {consumed:N0}, {consumedPerMin:N2}/min");
                timer.Start();
            };

            timer.Start();
        }

        private (IKafkaTransportConsumer, IKafkaConsumer) CreateKafkaOutput(Partition partition, Offset offset)
        {
            Console.WriteLine($"Reading from {TopicName}, partition 2");
            var consConfig = new ConsumerConfiguration(Const.BrokerList, ConsumerGroup);
            var topicConfig = new ConsumerTopicConfiguration(TopicName, partition, offset);
            var kafkaOutput = new KafkaConsumer(consConfig, topicConfig);
            kafkaOutput.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            var transportConsumer = new KafkaTransportConsumer(kafkaOutput);
            return (transportConsumer, kafkaOutput);
        }
    }
}