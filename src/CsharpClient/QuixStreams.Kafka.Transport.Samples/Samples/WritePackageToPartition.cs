using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;

namespace QuixStreams.Kafka.Transport.Samples.Samples
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
            using (var producer = this.CreateKafkaProducer(partition))
            {
                var transportProducer = new KafkaTransportProducer(producer);
                this.SendDataUsingProducer(transportProducer, cancellationToken);

                cancellationToken.WaitHandle.WaitOne();
            }
        }


        private void SendDataUsingProducer(IKafkaTransportProducer producer, CancellationToken ct)
        {
            var counter = 0;
            var random = new Random();
            while (!ct.IsCancellationRequested)
            {
                counter++;

                var package = new TransportPackage<ExampleModel>($"DataSet {counter}", new ExampleModel());

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

        private IKafkaProducer CreateKafkaProducer(Partition partition)
        {
            Console.WriteLine($"Write to {TopicName}, partition 2");
            var prodConfig = new ProducerConfiguration(Const.BrokerList);
            var topicConfig = new ProducerTopicConfiguration(TopicName, partition);
            var kafkaProducer = new KafkaProducer(prodConfig, topicConfig);
            return kafkaProducer;
        }
    }
}