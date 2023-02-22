using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quix.Streams.Transport.Fw.Codecs;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.Kafka;
using Quix.Streams.Transport.Registry;
using Timer = System.Timers.Timer;

namespace Quix.Streams.Transport.Samples.Samples
{
    /// <summary>
    /// Example for package (Typed messages) reading
    /// Note: Works well with WritePackages example
    /// </summary>
    public class ReadFilteredPackages
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

        private Task NewPackageHandler(Package e)
        {
            Interlocked.Increment(ref this.consumedCounter);
            // keep in mind value is lazily evaluated, so this is a position where one can decide whether to use it
            var key = e.GetKey();
            var value = e.Value.Value;
            var packageMetaData = e.MetaData;
            var timestamp = e.MetaData.TryGetValue("DateTime", out var dts) ? (DateTime?) DateTime.Parse(dts) : null;
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
            var filteredKafkaOutput = new PackageFilterConsumer(kafkaOutput, this.PackageFilter);
            var transportConsumer = new TransportConsumer(filteredKafkaOutput);
            return transportConsumer;
        }

        private bool PackageFilter(Package newPackageArgs)
        {
            return new Random().Next(10) == 4; // keep 10% of the packages. This logic can be anything
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
                var published = Interlocked.Read(ref this.consumedCounter);


                var publishedPerMin = published / elapsed.TotalMilliseconds * 60000;

                Console.WriteLine($"Consumed Packages: {published:N0}, {publishedPerMin:N2}/min");
                timer.Start();
            };

            timer.Start();
        }
    }
}