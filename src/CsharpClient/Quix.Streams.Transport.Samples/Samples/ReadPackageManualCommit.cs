using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.Kafka;
using Timer = System.Timers.Timer;

namespace Quix.Streams.Transport.Samples.Samples
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
        public IConsumer Start()
        {
            var consumer = this.CreateConsumer();
            this.HookUpStatistics();

            var previousRef = consumer.OnNewPackage;

            consumer.OnNewPackage = e => previousRef(e).ContinueWith(t => this.NewPackageHandler(e));
            return consumer;
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

        private IConsumer CreateConsumer()
        {
            var consConfig = new ConsumerConfiguration(Const.BrokerList, ConsumerGroup);
            var topicConfig = new ConsumerTopicConfiguration(TopicName);
            var kafkaOutput = new KafkaConsumer(consConfig, topicConfig);
            kafkaOutput.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            kafkaOutput.Open();
            var transportConsumer = new TransportConsumer(kafkaOutput, o=> o.CommitOptions.AutoCommitEnabled = false);
            var newStreamCommiter = new NewStreamOffSetCommiter(kafkaOutput);
            transportConsumer.OnNewPackage = e => newStreamCommiter.Publish(e);
            return transportConsumer;
        }

        private class NewStreamOffSetCommiter : IProducer
        {
            private readonly KafkaConsumer kafkaConsumer;

            public NewStreamOffSetCommiter(KafkaConsumer kafkaConsumer)
            {
                this.kafkaConsumer = kafkaConsumer;
            }

            public Task Publish(Package package, CancellationToken cancellationToken = default)
            {
                var packageOffSet = (long) package.TransportContext[KnownKafkaTransportContextKeys.Offset];
                var partition = (int) package.TransportContext[KnownKafkaTransportContextKeys.Partition];
                var topic = (string) package.TransportContext[KnownKafkaTransportContextKeys.Topic];
                if (packageOffSet == 0) return Task.CompletedTask; // no need to commit anything
                Console.WriteLine($"Package found on Topic '{topic}', partition {partition} and offset {packageOffSet}. Committing offset {packageOffSet}");
                this.kafkaConsumer.CommitOffset(new TopicPartitionOffset(topic, new Partition(partition), new Offset(packageOffSet)));
                return Task.CompletedTask;
            }
        }
    }
}