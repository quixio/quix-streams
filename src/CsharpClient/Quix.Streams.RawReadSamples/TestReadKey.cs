using System;
using System.Text;
using Quix.Streams.Streaming;

namespace Quix.Streams.RawReadSamples
{
    class TestReadKey
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawTopicConsumer = streamingClient.CreateRawTopicConsumer("RawSampleKey");


            rawTopicConsumer.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            rawTopicConsumer.OnMessageRead += (sender, message) =>
            {
                var text = Encoding.UTF8.GetString((byte[])message.Value);
                var key = message.Key != null ? message.Key : "???`";
                Console.WriteLine($"received -> {key} = {text}");
            };


            rawTopicConsumer.Subscribe();
            Console.WriteLine("Listening to Kafka!");

            Console.WriteLine("\npress any key to exit the process...");
            // basic use of "Console.ReadKey()" method
            Console.ReadKey();

        }
    }
}
