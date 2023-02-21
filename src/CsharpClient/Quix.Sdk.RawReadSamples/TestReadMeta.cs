using System;
using Quix.Sdk.Streaming;
using System.Collections.Generic;
using System.Text;

namespace Quix.Sdk.RawReadSamples
{
    class TestReadMeta
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawReader = streamingClient.CreateRawTopicConsumer("RawSampleMeta");


            rawReader.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Expception occurred: {e}");
            };
            rawReader.OnMessageRead += (sender, message) =>
            {
                var text = Encoding.UTF8.GetString((byte[])message.Value);
                Console.WriteLine($"received -> {text}");

                foreach(var key in message.Metadata) {
                    Console.WriteLine($"---- {key.Key} = >>{key.Value}<<");

                }
            };


            rawReader.Subscribe();
            Console.WriteLine("Litening to Kafka!");

            Console.WriteLine("\npress any key to exit the process...");
            // basic use of "Console.ReadKey()" method
            Console.ReadKey();

        }
    }
}
