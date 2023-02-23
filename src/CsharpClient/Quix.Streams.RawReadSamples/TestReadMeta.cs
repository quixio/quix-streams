﻿using System;
using System.Text;
using Quix.Streams.Streaming;

namespace Quix.Streams.RawReadSamples
{
    class TestReadMeta
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawReader = streamingClient.CreateRawTopicConsumer("RawSampleMeta");


            rawReader.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            rawReader.OnMessageReceived += (sender, message) =>
            {
                var text = Encoding.UTF8.GetString((byte[])message.Value);
                Console.WriteLine($"received -> {text}");

                foreach(var key in message.Metadata) {
                    Console.WriteLine($"---- {key.Key} = >>{key.Value}<<");

                }
            };


            rawReader.Subscribe();
            Console.WriteLine("Listening to Kafka!");

            Console.WriteLine("\npress any key to exit the process...");
            // basic use of "Console.ReadKey()" method
            Console.ReadKey();

        }
    }
}
