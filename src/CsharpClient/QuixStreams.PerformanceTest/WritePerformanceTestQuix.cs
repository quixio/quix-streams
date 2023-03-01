using System;
using System.Threading;
using QuixStreams.Streaming;
using QuixStreams.Streaming.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.PerformanceTest
{
    public class WritePerformanceTestQuix
    {
        long receivedCount = 0;
        long sentCount = 0;

        public void Run(int paramCount, int bufferSize, CancellationToken ct, bool onlySent = false, bool showIntermediateResults = false)
        {

            TimeseriesData data = null;
            var iteration = 0;
            long result = 0;

            var timeIteration = 0;
            DateTime lastUpdate = DateTime.UtcNow;
            var datetime = DateTime.UtcNow.ToUnixNanoseconds();

            // Create a client which holds generic details for creating topic consumer and producers
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            using var topicProducer = client.GetTopicProducer("test");

            var stream = topicProducer.CreateStream();
            stream.Timeseries.Buffer.PacketSize = bufferSize;
            Console.WriteLine("Stream Created.");

            while (!ct.IsCancellationRequested && iteration <= 20)
            {
                var time = datetime + (timeIteration * 100);

                // New Timeseries Data
                if (!onlySent || iteration == 0)
                {
                    data = new TimeseriesData(100);
                    for (var i = 0; i < 100; i++)
                    {
                        var timestamp = data.AddTimestampNanoseconds(time + i);

                        for (var j = 0; j < paramCount; j++)
                        {
                            timestamp.AddValue("param" + j.ToString(), j);
                        }
                        timestamp.AddTag("tagTest", "Test" + timeIteration.ToString());

                    }
                }


                stream.Timeseries.Buffer.Publish(data);

                sentCount += paramCount * data.Timestamps.Count;
                timeIteration++;

                if ((DateTime.UtcNow - lastUpdate).TotalSeconds >= 1)
                {
                    if (showIntermediateResults)
                    {
                        Console.WriteLine($"Timestamps - SEND {sentCount} - RECEIVED: {receivedCount}");
                    }

                    result += sentCount;

                    sentCount = 0;
                    receivedCount = 0;
                    lastUpdate = DateTime.UtcNow;

                    iteration++;
                }
            }

            Console.WriteLine($"ParamCount = {paramCount}, BufferSize = {bufferSize}, Result = {((double)result / iteration) / 1000000}");

        }
    }
}
