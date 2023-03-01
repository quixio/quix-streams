using System;
using System.Text;
using QuixStreams.Streaming;

namespace QuixStreams.RawReadSamples
{
    class TestReadKey
    {
        public static void Run()
        {
            var streamingClient = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var rawTopicConsumer = streamingClient.GetRawTopicConsumer("RawSampleKey");


            rawTopicConsumer.OnErrorOccurred += (s, e) =>
            {
                Console.WriteLine($"Exception occurred: {e}");
            };
            rawTopicConsumer.OnMessageReceived += (sender, message) =>
            {
                var text = Encoding.UTF8.GetString(message.Value);
                var key = Encoding.UTF8.GetString(message.Key);
                if (string.IsNullOrEmpty(key)) key = "???";
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
