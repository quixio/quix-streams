using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Transport.Fw.Codecs;
using QuixStreams.Transport.IO;
using QuixStreams.Transport.Kafka;
using QuixStreams.Transport.Registry;
using Timer = System.Timers.Timer;

namespace QuixStreams.Transport.Samples.Samples
{
    /// <summary>
    /// Example for simple package (Typed messages) reading
    /// Note: Works well with WritePackages example
    /// </summary>
    public class ReadPackages
    {
        private const string TopicName = Const.PackageTopic;
        private const string ConsumerGroup = "Test-Subscriber#2";
        private long consumedCounter; // this is purely here for statistics

        /// <summary>
        /// Start the reading which is an asynchronous process. See <see cref="NewPackageHandler" />
        /// </summary>
        /// <returns>Disposable output</returns>
        public IConsumer Start()
        {
            this.RegisterCodecs();
            var consumer = this.CreateConsumer();
            this.HookUpStatistics();
            consumer.OnNewPackage = this.NewPackageHandler;
            return consumer;
        }

        private Task NewPackageHandler(Package package)
        {
            if (!package.TryConvertTo<ExampleModel>(out var mPackage)) return Task.CompletedTask;
            Interlocked.Increment(ref this.consumedCounter);
            var key = mPackage.GetKey();
            // keep in mind value is lazily evaluated, so this is a position where one can decide whether to use it
            var value = mPackage.Value;
            var packageMetaData = mPackage.MetaData;
            var timestamp = mPackage.MetaData.TryGetValue("DateTime", out var dts) ? (DateTime?) DateTime.Parse(dts) : null;
            return Task.CompletedTask;
        }

        private void RegisterCodecs()
        {
            // Regardless of how the example model is sent, this will let us read them
            CodecRegistry.RegisterCodec(typeof(ExampleModel), new DefaultJsonCodec<ExampleModel>());
            CodecRegistry.RegisterCodec("EM", new DefaultJsonCodec<ExampleModel>());
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
            var transportConsumer = new TransportConsumer(kafkaOutput);
            return transportConsumer;
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
    }
}