using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Quix.Streams.Transport.Fw;
using Quix.Streams.Transport.IO;
using Quix.Streams.Transport.Kafka;
using Timer = System.Timers.Timer;

namespace Quix.Streams.Transport.Samples.Samples
{
    /// <summary>
    /// Example for simple raw message write (lowest level possible)
    /// Note: Works well with ReadMessages example
    /// </summary>
    public class WriteMessage
    {
        private const string TopicName = Const.MessagesTopic;
        public int MaxMessageSizeInKafka = 1010;
        public int MaxKafkaKeySize = 100;
        public int MessageSizeInBytes = 2048;
        public int MillisecondsInterval = 1000; // interval between messages sent 0 = none
        private long publishedCounter; // this is purely here for statistics

        /// <summary>
        /// Run the test synchronously
        /// </summary>
        /// <param name="ct"></param>
        public void Run(CancellationToken ct)
        {
            using (var producer = this.CreateProducer(out var splitter))
            {
                var transportProducer = new TransportProducer(producer, splitter);
                this.HookUpStatistics();
                try
                {
                    this.SendMessage(transportProducer, ct);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }
        }

        private void SendMessage(IProducer producer, CancellationToken ct)
        {
            var counter = 0;
            var random = new Random();
            while (!ct.IsCancellationRequested)
            {
                var bytes = new byte[this.MessageSizeInBytes];
                random.NextBytes(bytes);
                var currentCounter = counter;
                var value = new Lazy<byte[]>(bytes);
                var msg = new Package<byte[]>(value, null);
                msg.SetKey(Encoding.UTF8.GetBytes($"CustomSize {currentCounter}"));

                var sendTask = producer.Publish(msg, ct);
                sendTask.ContinueWith(t => Console.WriteLine($"Exception on send: {t.Exception}"), TaskContinuationOptions.OnlyOnFaulted);
                sendTask.ContinueWith(t => Interlocked.Increment(ref this.publishedCounter), TaskContinuationOptions.OnlyOnRanToCompletion);
                counter++;
                if (this.MillisecondsInterval > 0)
                {
                    try
                    {
                        Task.Delay(this.MillisecondsInterval, ct).Wait(ct);
                    }
                    catch (OperationCanceledException)
                    {
                        //ignore;
                    }
                }
            }
        }

        private IKafkaProducer CreateProducer(out ByteSplitter byteSplitter)
        {
            var pubConfig = new PublisherConfiguration(Const.BrokerList)
            {
                MaxMessageSize = this.MaxMessageSizeInKafka
            };
            byteSplitter = new ByteSplitter(pubConfig.MaxMessageSize - MaxKafkaKeySize);
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
                var published = Interlocked.Read(ref this.publishedCounter);


                var publishedPerMin = published / elapsed.TotalMilliseconds * 60000;

                Console.WriteLine($"Published Messages: {published:N0}, {publishedPerMin:N2}/min");
                timer.Start();
            };

            timer.Start();
        }
    }
}