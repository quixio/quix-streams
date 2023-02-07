using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Quix.Sdk.Process.Models.Utility;
using Quix.Sdk.Streaming;
using Quix.Sdk.Streaming.Configuration;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Streaming.Models.StreamReader;
using Quix.Sdk.Streaming.Models.StreamWriter;
using Quix.Sdk.Streaming.UnitTests;

namespace Quix.Sdk.PerformanceTest
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

            // Create a client which holds generic details for creating input and output topics
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            using var inputTopic = client.OpenInputTopic("test");

            // Hook up events before initiating read to avoid losing out on any data
            inputTopic.OnStreamReceived += (s, streamReader) =>
            {
                Console.WriteLine($"New stream read: {streamReader.StreamId}");

                var bufferConfiguration = new ParametersBufferConfiguration
                {
                    PacketSize = bufferSize,
                };

                var buffer = streamReader.Parameters.CreateBuffer(bufferConfiguration);

                buffer.OnRead += data =>
                {
                    //var paramCount = data.NumericValues.Count + data.StringValues.Count + data.BinaryValues.Count;

                    receivedCount += data.Timestamps.Count() * paramCount;

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

            inputTopic.StartReading(); // initiate read
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