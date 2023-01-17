using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.Kafka;
using Timer = System.Timers.Timer;

namespace Quix.Sdk.Transport.Samples.Samples
{
    /// <summary>
    ///     Read telemetry data and monitor the output
    /// </summary>
    public class ReadPackageFromPartition
    {
        private const string TopicName = Const.PartitionedPackageTestTopic;
        private const string InputGroup = "Test-Subscriber#3";

        private long consumedCounter; // this is purely here for statistics

        /// <summary>
        ///     Start the reading stream which is an asynchronous process.
        /// </summary>
        /// <returns>Disposable output</returns>
        public IOutput Start(Partition partition, Offset offset)
        {
            var output = this.CreateKafkaOutput(partition, offset);
            this.HookUpStatistics();
            output.OnNewPackage = this.NewPackageHandler;
            return output;
        }

        private Task NewPackageHandler(Package obj)
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

        private IOutput CreateKafkaOutput(Partition partition, Offset offset)
        {
            Console.WriteLine($"Reading from {TopicName}, partition 2");
            var subConfig = new SubscriberConfiguration(Const.BrokerList, InputGroup);
            var topicConfig = new OutputTopicConfiguration(TopicName, partition, offset);
            var kafkaOutput = new KafkaOutput(subConfig, topicConfig);
            kafkaOutput.ErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            kafkaOutput.Open();
            var output = new TransportOutput(kafkaOutput);
            return output;
        }
    }
}