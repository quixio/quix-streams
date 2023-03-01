using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Quix.Streams.Transport.Fw;
using Quix.Streams.Transport.Fw.Codecs;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.Kafka;
using Quix.Streams.Transport.Registry;
using Timer = System.Timers.Timer;

namespace Quix.Streams.Transport.Samples.Samples
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
            using (var kafkaProducer = this.CreateKafkaProducer(out var splitter))
            {
                this.HookUpStatistics();
                // See method comments for differences
                //var producer = this.CreateSimpleProducer(kafkaProducer, splitter);
                //var producer = CreateWithUserDefinedCodec(kafkaProducer, splitter);
                var producer = this.CreateEfficientProducer(kafkaProducer, splitter);

                // please keep in mind you can use as many outputs as you wish, each of them dealing with a single type

                this.SendDataUsingProducer(producer, ct);
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

                var package = new Package<ExampleModel>(new Lazy<ExampleModel>(new ExampleModel()), metaData);
                package.SetKey(Encoding.UTF8.GetBytes($"DataSet {counter}"));

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
        private IProducer CreateSimpleProducer(IProducer producer, ByteSplitter splitter)
        {
            var tProducer = new TransportProducer(producer, splitter);
            return tProducer;
        }

        /// <summary>
        /// This uses a registered codec
        /// </summary>
        /// <returns></returns>
        private IProducer CreateWithUserDefinedCodec(IProducer producer, ByteSplitter splitter)
        {
            CodecRegistry.RegisterCodec(typeof(ExampleModel), new DefaultJsonCodec<ExampleModel>());
            var tProducer = new TransportProducer(producer, splitter);
            return tProducer;
        }

        /// <summary>
        /// This uses a registered codec with a shortened model key
        /// </summary>
        /// <returns></returns>
        private IProducer CreateEfficientProducer(IProducer producer, ByteSplitter splitter)
        {
            var codec = new DefaultJsonCodec<ExampleModel>();
            CodecRegistry.RegisterCodec(ModelKey, codec);
            var tProducer = new TransportProducer(producer, splitter);
            return tProducer;
        }

        private IKafkaProducer CreateKafkaProducer(out ByteSplitter byteSplitter)
        {
            var pubConfig = new PublisherConfiguration(Const.BrokerList);
            byteSplitter = new ByteSplitter(pubConfig.MaxMessageSize);
            var topicConfig = new ProducerTopicConfiguration(TopicName);
            var kafkaProducer = new KafkaProducer(pubConfig, topicConfig);
            kafkaProducer.Open();
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