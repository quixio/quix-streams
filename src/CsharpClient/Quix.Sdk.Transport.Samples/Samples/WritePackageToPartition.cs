using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.Fw.Codecs;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.Kafka;
using Quix.Sdk.Transport.Registry;

namespace Quix.Sdk.Transport.Samples.Samples
{
    public class WritePackageToPartition
    {
        private const string TopicName = Const.PartitionedPackageTestTopic;
        private static readonly ModelKey modelKey = new ModelKey("EM");
        public int MillisecondsInterval = 1000; // interval between messages sent 0 = none
        private long producedCounter; // this is purely here for statistics

        public void Run(Partition partition, CancellationToken cancellationToken)
        {
            CodecRegistry.RegisterCodec(modelKey, new DefaultJsonCodec<ExampleModel>());
            using (var input = this.CreateKafkaInput(partition, out var splitter))
            {
                var transportInput = new TransportInput(input, splitter);
                this.SendDataUsingInput(transportInput, cancellationToken);

                cancellationToken.WaitHandle.WaitOne();
            }
        }


        private void SendDataUsingInput(IInput input, CancellationToken ct)
        {
            var counter = 0;
            var random = new Random();
            while (!ct.IsCancellationRequested)
            {
                counter++;

                // Timestamp is optional. Code below will randomly set it sometimes
                var metaData = MetaData.Empty;
                if (random.Next(2) == 1)
                {
                    metaData = new MetaData(new Dictionary<string, string>
                    {
                        {"DateTime", DateTime.UtcNow.ToString("O")}
                    });
                }

                var package = new Package<ExampleModel>(new Lazy<ExampleModel>(new ExampleModel()), metaData);
                package.SetKey($"DataSet {counter}");

                var sendTask = input.Send(package, ct);
                sendTask.ContinueWith(t => Console.WriteLine($"Exception on send: {t.Exception}"), TaskContinuationOptions.OnlyOnFaulted);
                sendTask.ContinueWith(t => Interlocked.Increment(ref this.producedCounter), TaskContinuationOptions.OnlyOnRanToCompletion);
                if (this.MillisecondsInterval > 0)
                {
                    try
                    {
                        Task.Delay(this.MillisecondsInterval, ct).Wait(ct);
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore
                    }
                }
            }
        }

        private IKafkaInput CreateKafkaInput(Partition partition, out ByteSplitter byteSplitter)
        {
            Console.WriteLine($"Write to {TopicName}, partition 2");
            var pubConfig = new PublisherConfiguration(Const.BrokerList);
            byteSplitter = new ByteSplitter(pubConfig.MaxMessageSize);
            var topicConfig = new InputTopicConfiguration(TopicName, partition);
            var input = new KafkaInput(pubConfig, topicConfig);
            input.Open();
            return input;
        }
    }
}