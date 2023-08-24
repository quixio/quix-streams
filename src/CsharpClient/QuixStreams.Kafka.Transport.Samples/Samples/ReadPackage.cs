using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Kafka.Transport.SerDes.Codecs;
using QuixStreams.Kafka.Transport.SerDes.Codecs.DefaultCodecs;
using Timer = System.Timers.Timer;

namespace QuixStreams.Kafka.Transport.Samples.Samples
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
        public IKafkaTransportConsumer Start()
        {
            this.RegisterCodecs();
            var (transportConsumer, kafkaConsumer) = this.CreateConsumer();
            this.HookUpStatistics();
            transportConsumer.OnPackageReceived = this.NewPackageHandler;
            kafkaConsumer.Open();
            return transportConsumer;
        }

        private Task NewPackageHandler(TransportPackage package)
        {
            if (!package.TryConvertTo<ExampleModel>(out var mPackage)) return Task.CompletedTask;
            Interlocked.Increment(ref this.consumedCounter);
            var key = mPackage.Key;
            var value = mPackage.Value;
            return Task.CompletedTask;
        }

        private void RegisterCodecs()
        {
            // Regardless of how the example model is sent, this will let us read them
            CodecRegistry.RegisterCodec(typeof(ExampleModel), new DefaultJsonCodec<ExampleModel>());
            CodecRegistry.RegisterCodec("EM", new DefaultJsonCodec<ExampleModel>());
        }

        private (IKafkaTransportConsumer, IKafkaConsumer) CreateConsumer()
        {
            var consConfig = new ConsumerConfiguration(Const.BrokerList, ConsumerGroup);
            var topicConfig = new ConsumerTopicConfiguration(TopicName);
            var kafkaOutput = new KafkaConsumer(consConfig, topicConfig);
            kafkaOutput.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            var transportConsumer = new KafkaTransportConsumer(kafkaOutput);
            return (transportConsumer, kafkaOutput);
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