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
    public class ReadPackageManualCommit
    {
        private const string TopicName = Const.PackageTopic;
        private const string InputGroup = "ManualCommit-Subscriber#1";

        private long consumedCounter; // this is purely here for statistics

        /// <summary>
        ///     Start the reading stream which is an asynchronous process.
        /// </summary>
        /// <returns>Disposable output</returns>
        public IOutput Start()
        {
            var output = this.CreateOutput();
            this.HookUpStatistics();

            var previousRef = output.OnNewPackage;

            output.OnNewPackage = e => previousRef(e).ContinueWith(t => this.NewPackageHandler(e));
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

        private IOutput CreateOutput()
        {
            var subConfig = new SubscriberConfiguration(Const.BrokerList, InputGroup);
            var topicConfig = new OutputTopicConfiguration(TopicName);
            var kafkaOutput = new KafkaOutput(subConfig, topicConfig);
            kafkaOutput.ErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            kafkaOutput.Open();
            var output = new TransportOutput(kafkaOutput, o=> o.CommitOptions.AutoCommitEnabled = false);
            var newStreamCommiter = new NewStreamOffSetCommiter(kafkaOutput);
            output.OnNewPackage = e => newStreamCommiter.Send(e);
            return output;
        }

        private class NewStreamOffSetCommiter : IInput
        {
            private readonly KafkaOutput kafkaOutput;

            public NewStreamOffSetCommiter(KafkaOutput kafkaOutput)
            {
                this.kafkaOutput = kafkaOutput;
            }

            public Task Send(Package package, CancellationToken cancellationToken = default)
            {
                var packageOffSet = (long) package.TransportContext[KnownKafkaTransportContextKeys.Offset];
                var partition = (int) package.TransportContext[KnownKafkaTransportContextKeys.Partition];
                var topic = (string) package.TransportContext[KnownKafkaTransportContextKeys.Topic];
                if (packageOffSet == 0) return Task.CompletedTask; // no need to commit anything
                Console.WriteLine($"Package found on Topic '{topic}', partition {partition} and offset {packageOffSet}. Committing offset {packageOffSet}");
                this.kafkaOutput.CommitOffset(new TopicPartitionOffset(topic, new Partition(partition), new Offset(packageOffSet)));
                return Task.CompletedTask;
            }
        }
    }
}