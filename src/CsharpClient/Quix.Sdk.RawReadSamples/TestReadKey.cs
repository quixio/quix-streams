using System;
using Quix.Sdk.Streaming;
using System.Collections.Generic;
using System.Text;

namespace Quix.Sdk.RawReadSamples
{
    class TestReadKey
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawReader = streamingClient.OpenRawInputTopic("RawSampleKey");


            rawReader.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Expception occurred: {e}");
            };
            rawReader.OnMessageRead += (message) =>
            {
                var text = Encoding.UTF8.GetString((byte[])message.Value);
                var key = message.Key != null ? message.Key : "???`";
                Console.WriteLine($"received -> {key} = {text}");
            };


            rawReader.StartReading();
            Console.WriteLine("Litening to Kafka!");

            Console.WriteLine("\npress any key to exit the process...");
            // basic use of "Console.ReadKey()" method
            Console.ReadKey();

        }
    }
}
