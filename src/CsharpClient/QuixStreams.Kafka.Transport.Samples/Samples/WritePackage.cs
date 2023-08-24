using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using Timer = System.Timers.Timer;

namespace QuixStreams.Kafka.Transport.Samples.Samples
{
    /// <summary>
    /// Example for simple package (Typed messages) sending
    /// Note: Works well with ReadPackages example
    /// </summary>
    public class WritePackages
    {
        private const string TopicName = Const.PackageTopic;
        private static readonly ModelKey ModelKey = new ModelKey("EM");
        private const int MillisecondsInterval = 1000; // interval between messages sent 0 = none
        private long producedCounter; // this is purely here for statistics

        /// <summary>
        /// Run the test synchronously
        /// </summary>
        /// <param name="ct"></param>
        public void Run(CancellationToken ct)
        {
            using (var kafkaProducer = this.CreateKafkaProducer())
            {
                this.HookUpStatistics();
                // See method comments for differences
                //var producer = this.CreateSimpleProducer(kafkaProducer);
                //var producer = CreateWithUserDefinedCodec(kafkaProducer);
                var producer = this.CreateEfficientProducer(kafkaProducer);

                // please keep in mind you can use as many outputs as you wish, each of them dealing with a single type

                this.SendDataUsingProducer(producer, ct);
            }
        }

        private void SendDataUsingProducer(IKafkaTransportProducer producer, CancellationToken ct)
        {
            var counter = 0;
            while (!ct.IsCancellationRequested)
            {
                counter++;
                
                var package = new TransportPackage<ExampleModel>($"DataSet {counter}", new ExampleModel());

                var sendTask = producer.Publish(package, ct);
                sendTask.ContinueWith(t => Console.WriteLine($"Exception on send: {t.Exception}"), TaskContinuationOptions.OnlyOnFaulted);
                sendTask.ContinueWith(t => Interlocked.Increment(ref this.producedCounter), TaskContinuationOptions.OnlyOnRanToCompletion);
                if (MillisecondsInterval > 0)
                {
                    try
                    {
                        Task.Delay(MillisecondsInterval, ct).Wait(ct);
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore
                    }
                }
            }
        }

        /// <summary>
        /// This uses default codec, with default type key
        /// </summary>
        /// <returns></returns>
        private IKafkaTransportProducer CreateSimpleProducer(IKafkaProducer producer)
        {
            var tProducer = new KafkaTransportProducer(producer);
            return tProducer;
        }

        /// <summary>
        /// This uses a registered codec
        /// </summary>
        /// <returns></returns>
        private IKafkaTransportProducer CreateWithUserDefinedCodec(IKafkaProducer producer)
        {
            CodecRegistry.RegisterCodec(typeof(ExampleModel), new DefaultJsonCodec<ExampleModel>());
            var tProducer = new KafkaTransportProducer(producer);
            return tProducer;
        }

        /// <summary>
        /// This uses a registered codec with a shortened model key
        /// </summary>
        /// <returns></returns>
        private IKafkaTransportProducer CreateEfficientProducer(IKafkaProducer producer)
        {
            var codec = new DefaultJsonCodec<ExampleModel>();
            CodecRegistry.RegisterCodec(ModelKey, codec);
            var tProducer = new KafkaTransportProducer(producer);
            return tProducer;
        }

        private IKafkaProducer CreateKafkaProducer()
        {
            var prodConfig = new ProducerConfiguration(Const.BrokerList);
            var topicConfig = new ProducerTopicConfiguration(TopicName);
            var kafkaProducer = new KafkaProducer(prodConfig, topicConfig);
            return kafkaProducer;
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
                var published = Interlocked.Read(ref this.producedCounter);


                var publishedPerMin = published / elapsed.TotalMilliseconds * 60000;

                Console.WriteLine($"Produced Packages: {published:N0}, {publishedPerMin:N2}/min");
                timer.Start();
            };

            timer.Start();
        }
    }
}