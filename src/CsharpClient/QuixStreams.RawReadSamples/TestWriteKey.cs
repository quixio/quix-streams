using System;
using System.Text;
using System.Threading;
using QuixStreams.Kafka;
using QuixStreams.Streaming;

namespace QuixStreams.RawReadSamples
{
    class TestWriteKey
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawWriter = streamingClient.GetRawTopicProducer("RawWriteKey");

            var nanos = DateTime.Now.ToString("HH:mm:ss:fff") + (DateTime.Now.Ticks / 10);

            for (var i = 0; i < 100; i++)
            {
                DateTime thisDay = DateTime.Now;
                var data = Encoding.ASCII.GetBytes($"current time is {thisDay.ToString()}");

                Console.WriteLine("Wrote 1 package");
                rawWriter.Publish(new KafkaMessage(
                    Encoding.UTF8.GetBytes($"{nanos+i}"),
                    data, null
                ));

                Thread.Sleep(1000);
            }


            rawWriter.Dispose();
        }
    }
}
