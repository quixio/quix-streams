using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Timer = System.Timers.Timer;

namespace QuixStreams.Kafka.Transport.Samples.Samples
{
    public class ReadPackageManualCommit
    {
        private const string TopicName = Const.PackageTopic;
        private const string ConsumerGroup = "ManualCommit-Subscriber#1";

        private long consumedCounter; // this is purely here for statistics

        /// <summary>
        /// Start the reading stream which is an asynchronous process.
        /// </summary>
        /// <returns>Disposable output</returns>
        public IKafkaTransportConsumer Start()
        {
            var (transportConsumer, kafkaConsumer) = this.CreateConsumer();
            this.HookUpStatistics();

            var previousRef = transportConsumer.OnPackageReceived;

            transportConsumer.OnPackageReceived = e => previousRef(e).ContinueWith(t => this.NewPackageHandler(e));
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

        private (IKafkaTransportConsumer, IKafkaConsumer) CreateConsumer()
        {
            var consConfig = new ConsumerConfiguration(Const.BrokerList, ConsumerGroup);
            var topicConfig = new ConsumerTopicConfiguration(TopicName);
            var kafkaOutput = new KafkaConsumer(consConfig, topicConfig);
            kafkaOutput.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            var transportConsumer = new KafkaTransportConsumer(kafkaOutput, o=> o.CommitOptions.AutoCommitEnabled = false);
            transportConsumer.OnPackageReceived = e => PublishNewPackageOffset(transportConsumer, e);
            return (transportConsumer, kafkaOutput);
        }
        
        public Task PublishNewPackageOffset(IKafkaTransportConsumer consumer, TransportPackage package)
        {
            if (package.KafkaMessage.TopicPartitionOffset.Offset == 0) return Task.CompletedTask; // no need to commit anything
            Console.WriteLine($"Package found with topic partition offset: {package.KafkaMessage.TopicPartitionOffset}");
            consumer.Commit(new List<TopicPartitionOffset>() {package.KafkaMessage.TopicPartitionOffset});
            return Task.CompletedTask;
        }
    }
}