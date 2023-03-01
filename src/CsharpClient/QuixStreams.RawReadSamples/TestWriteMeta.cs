using System;
using System.Text;
using System.Threading;
using QuixStreams.Streaming;

namespace QuixStreams.RawReadSamples
{
    class TestWriteMeta
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawWriter = streamingClient.GetRawTopicProducer("RawSampleKey");

            for (var i = 0; i < 100; i++)
            {
                DateTime thisDay = DateTime.Now;
                var data = Encoding.ASCII.GetBytes(thisDay.ToString());

                rawWriter.Publish(new QuixStreams.Streaming.Raw.RawMessage(
                    data
                ));

                Thread.Sleep(1000);
            }


            rawWriter.Dispose();
        }
    }
}
