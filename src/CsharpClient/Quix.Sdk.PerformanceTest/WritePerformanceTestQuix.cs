using System;
using System.Threading;
using Quix.Sdk.Streaming.Utils;
using Quix.Sdk.Streaming.Configuration;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Process.Models.Utility;
using Quix.Sdk.Streaming;

namespace Quix.Sdk.PerformanceTest
{
    public class WritePerformanceTestQuix
    {
        long receivedCount = 0;
        long sentCount = 0;

        public void Run(int paramCount, int bufferSize, CancellationToken ct, bool onlySent = false, bool showIntermediateResults = false)
        {

            ParameterData data = null;
            var iteration = 0;
            long result = 0;

            var timeIteration = 0;
            DateTime lastUpdate = DateTime.UtcNow;
            var datetime = DateTime.UtcNow.ToUnixNanoseconds();

            // Create a client which holds generic details for creating input and output topics
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            using var outputTopic = client.OpenOutputTopic("test");

            var stream = outputTopic.CreateStream();
            stream.Parameters.Buffer.PacketSize = bufferSize;
            Console.WriteLine("Stream Created.");

            while (!ct.IsCancellationRequested && iteration <= 20)
            {
                var time = datetime + (timeIteration * 100);

                // New Parameter Data
                if (!onlySent || iteration == 0)
                {
                    data = new ParameterData(100);
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


                stream.Parameters.Buffer.Write(data);

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
