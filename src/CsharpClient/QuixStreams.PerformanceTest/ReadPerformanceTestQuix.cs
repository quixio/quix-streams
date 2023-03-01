using System;
using System.Linq;
using System.Threading;
using QuixStreams.Streaming;
using QuixStreams.Streaming.Models;

namespace QuixStreams.PerformanceTest
{
    public class ReadPerformanceTestQuix
    {
        long receivedCount = 0;
        long sentCount = 0;

        public void Run(int paramCount, int bufferSize, CancellationToken ct, bool onlyReceive = false, bool showIntermediateResults = false)
        {
            long result = 0;
            var iteration = 0;

            DateTime lastUpdate = DateTime.UtcNow;

            // Create a client which holds generic details for creating topic consumer and producers
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            using var topicConsumer = client.GetTopicConsumer("test");

            // Hook up events before initiating read to avoid losing out on any data
            topicConsumer.OnStreamReceived += (topic, streamConsumer) =>
            {
                Console.WriteLine($"New stream read: {streamConsumer.StreamId}");

                var bufferConfiguration = new TimeseriesBufferConfiguration
                {
                    PacketSize = bufferSize,
                };

                var buffer = streamConsumer.Timeseries.CreateBuffer(bufferConfiguration);

                buffer.OnDataReleased += (sender, args) =>
                {
                    //var paramCount = data.NumericValues.Count + data.StringValues.Count + data.BinaryValues.Count;

                    receivedCount += args.Data.Timestamps.Count() * paramCount;

                    if ((DateTime.UtcNow - lastUpdate).TotalSeconds >= 1)
                    {
                        if (showIntermediateResults)
                        {
                            Console.WriteLine($"Timestamps - SEND {sentCount} - RECEIVED: {receivedCount}");
                        }

                        result += receivedCount;

                        sentCount = 0;
                        receivedCount = 0;
                        lastUpdate = DateTime.UtcNow;

                        iteration++;
                    }

                };
            };

            topicConsumer.Subscribe(); // initiate read
            Console.WriteLine("Listening for streams");

            // Hook up to termination signal (for docker image) and CTRL-C
            var exitEvent = new ManualResetEventSlim();
            Console.CancelKeyPress += (s, e) =>
            {
                e.Cancel = true; // In order to allow the application to cleanly exit instead of terminating it
                exitEvent.Set();
            };
            // Wait for CTRL-C
            exitEvent.Wait();

            Console.WriteLine($"ParamCount = {paramCount}, BufferSize = {bufferSize}, Result = {((double)result / iteration) / 1000000}");
        }

    }
}