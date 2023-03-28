using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using QuixStreams.Transport.Fw;
using QuixStreams.Transport.Fw.Codecs;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.Kafka;
using QuixStreams.Transport.Registry;

namespace QuixStreams.Transport.Samples.Samples
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
            using (var producer = this.CreateKafkaProducer(partition, out var splitter))
            {
                var transportProducer = new TransportProducer(producer, splitter);
                this.SendDataUsingProducer(transportProducer, cancellationToken);

                cancellationToken.WaitHandle.WaitOne();
            }
        }


        private void SendDataUsingProducer(IProducer producer, CancellationToken ct)
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

                var package = new Package<ExampleModel>(new ExampleModel(), metaData);
                package.SetKey(Encoding.UTF8.GetBytes($"DataSet {counter}"));

                var sendTask = producer.Publish(package, ct);
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

        private IKafkaProducer CreateKafkaProducer(Partition partition, out ByteSplitter byteSplitter)
        {
            Console.WriteLine($"Write to {TopicName}, partition 2");
            var pubConfig = new PublisherConfiguration(Const.BrokerList);
            byteSplitter = new ByteSplitter(pubConfig.MaxMessageSize);
            var topicConfig = new ProducerTopicConfiguration(TopicName, partition);
            var kafkaProducer = new KafkaProducer(pubConfig, topicConfig);
            kafkaProducer.Open();
            return kafkaProducer;
        }
    }
}