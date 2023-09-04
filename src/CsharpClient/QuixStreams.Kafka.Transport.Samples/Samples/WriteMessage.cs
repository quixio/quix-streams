using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Kafka.Transport.SerDes;
using Timer = System.Timers.Timer;

namespace QuixStreams.Kafka.Transport.Samples.Samples
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
            using (var producer = this.CreateProducer())
            {
                var transportProducer = new KafkaTransportProducer(producer);
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

        private void SendMessage(IKafkaTransportProducer producer, CancellationToken ct)
        {
            var counter = 0;
            var random = new Random();
            while (!ct.IsCancellationRequested)
            {
                var bytes = new byte[this.MessageSizeInBytes];
                random.NextBytes(bytes);
                var currentCounter = counter;
                var msg = new TransportPackage<byte[]>($"CustomSize {currentCounter}", bytes);

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

        private IKafkaProducer CreateProducer()
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
                var published = Interlocked.Read(ref this.publishedCounter);


                var publishedPerMin = published / elapsed.TotalMilliseconds * 60000;

                Console.WriteLine($"Published Messages: {published:N0}, {publishedPerMin:N2}/min");
                timer.Start();
            };

            timer.Start();
        }
    }
}