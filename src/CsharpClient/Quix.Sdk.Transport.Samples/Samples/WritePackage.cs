using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quix.Sdk.Transport.Fw;
using Quix.Sdk.Transport.Fw.Codecs;
using Quix.Sdk.Transport.IO;
using Quix.Sdk.Transport.Kafka;
using Quix.Sdk.Transport.Registry;
using Timer = System.Timers.Timer;

namespace Quix.Sdk.Transport.Samples.Samples
{
    /// <summary>
    ///     Example for simple package (Typed messages) sending
    ///     Note: Works well with ReadPackages example
    /// </summary>
    public class WritePackages
    {
        private const string TopicName = Const.PackageTopic;
        private static readonly ModelKey modelKey = new ModelKey("EM");
        public int MillisecondsInterval = 1000; // interval between messages sent 0 = none
        private long producedCounter; // this is purely here for statistics

        /// <summary>
        ///     Run the test synchronously
        /// </summary>
        /// <param name="ct"></param>
        public void Run(CancellationToken ct)
        {
            using (var kafkaInput = this.CreateKafkaInput(out var splitter))
            {
                this.HookUpStatistics();
                // See method comments for differences
                // var input = this.CreateSimpleInput(kafkaInput, splitter);
                // var input = CreateWithUserDefinedCodec(kafkaInput, splitter);
                var input = this.CreateEfficientInput(kafkaInput, splitter);

                // please keep in mind you can use as many outputs as you wish, each of them dealing with a single type

                this.SendDataUsingInput(input, ct);
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

        /// <summary>
        ///     This uses default codec, with default type key
        /// </summary>
        /// <returns></returns>
        private IInput CreateSimpleInput(IInput input, ByteSplitter splitter)
        {
            var tinput = new TransportInput(input, splitter);
            return tinput;
        }

        /// <summary>
        ///     This uses a registered codec
        /// </summary>
        /// <returns></returns>
        private IInput CreateWithUserDefinedCodec(IInput input, ByteSplitter splitter)
        {
            CodecRegistry.RegisterCodec(typeof(ExampleModel), new DefaultJsonCodec<ExampleModel>());
            var tinput = new TransportInput(input, splitter);
            return tinput;
        }

        /// <summary>
        ///     This uses a registered codec with a shortened model key
        /// </summary>
        /// <returns></returns>
        private IInput CreateEfficientInput(IInput input, ByteSplitter splitter)
        {
            var codec = new DefaultJsonCodec<ExampleModel>();
            CodecRegistry.RegisterCodec(modelKey, codec);
            var tinput = new TransportInput(input, splitter);
            return tinput;
        }

        private IKafkaInput CreateKafkaInput(out ByteSplitter byteSplitter)
        {
            var pubConfig = new PublisherConfiguration(Const.BrokerList);
            byteSplitter = new ByteSplitter(pubConfig.MaxMessageSize);
            var topicConfig = new InputTopicConfiguration(TopicName);
            var input = new KafkaInput(pubConfig, topicConfig);
            input.Open();
            return input;
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