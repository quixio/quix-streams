using Quix.Sdk.Streaming;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Threading;

namespace Quix.Sdk.RawReadSamples
{
    class TestWriteMeta
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawWriter = streamingClient.OpenRawOutputTopic("RawSampleKey");

            for (var i = 0; i < 100; i++)
            {
                DateTime thisDay = DateTime.Now;
                var data = Encoding.ASCII.GetBytes(thisDay.ToString());

                rawWriter.Write(new Streaming.Raw.RawMessage(
                    data
                ));

                Thread.Sleep(1000);
            }


            rawWriter.Dispose();
        }
    }
}
