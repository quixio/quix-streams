using System;
using System.Text;
using System.Threading;
using Quix.Sdk.Streaming;

namespace Quix.Sdk.RawReadSamples
{
    class TestWriteMeta
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawWriter = streamingClient.CreateRawTopicProducer("RawSampleKey");

            for (var i = 0; i < 100; i++)
            {
                DateTime thisDay = DateTime.Now;
                var data = Encoding.ASCII.GetBytes(thisDay.ToString());

                rawWriter.Publish(new Streaming.Raw.RawMessage(
                    data
                ));

                Thread.Sleep(1000);
            }


            rawWriter.Dispose();
        }
    }
}
