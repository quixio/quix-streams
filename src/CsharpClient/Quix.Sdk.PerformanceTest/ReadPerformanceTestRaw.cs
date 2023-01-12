using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Quix.Sdk.Process.Models.Utility;
using Quix.Sdk.Streaming;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Streaming.Models.StreamReader;
using Quix.Sdk.Streaming.Models.StreamWriter;
using Quix.Sdk.Streaming.UnitTests;

namespace Quix.Sdk.PerformanceTest
{
    public class ReadPerformanceTestRaw
    {
        long receivedCount = 0;
        long sentCount = 0;

        public void Run(int paramCount, int bufferSize, CancellationToken ct, bool onlyReceive = false, bool showIntermediateResults = false)
        {

            var buffer = new ParametersBuffer(null, null, true, false);
            buffer.PacketSize = bufferSize;
            buffer.OnReadRaw += (data) =>
            {
                if (onlyReceive)
                {
                    receivedCount += data.Timestamps.Length * paramCount;
                    return;
                }

                for (var t=0; t<data.Timestamps.Length; t++)
                {
                    foreach (var kv in data.NumericValues)
                    {
                        var h = kv.Value[t];
                        receivedCount++;
                    }
                    foreach (var kv in data.StringValues)
                    {
                        var h = kv.Value[t];
                        receivedCount++;
                    }
                    foreach (var kv in data.BinaryValues)
                    {
                        var h = kv.Value[t];
                        receivedCount++;
                    }
                }

            };

            DateTime lastUpdate = DateTime.UtcNow;


            // Prepare data 
            var data = new ParameterData();
            for(var i = 0; i < 100; i++)
            {
                var timestamp = data.AddTimestampNanoseconds(i);

                for (var j = 0; j < paramCount; j++)
                {
                    timestamp.AddValue($"param{j}", j);
                }
                timestamp.AddTag("tagTest", "Test");

            }
            var raw = data.ConvertToProcessData(false, false);

            var iteration = 0;
            long result = 0;
            while (!ct.IsCancellationRequested && iteration <= 20)
            {
                buffer.WriteChunk(raw);

                //var dataSimulated = new ParameterData(raw);

                //dataSimulated.Timestamps.ForEach(t => 
                //{
                //    t.TimestampNanoseconds = t.TimestampNanoseconds + DateTime.UtcNow.ToUnixNanoseconds();
                //    buffer.WriteChunk(t);
                //});

                sentCount += paramCount * raw.Timestamps.Length;

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
            }

            Console.WriteLine($"ParamCount = {paramCount}, BufferSize = {bufferSize}, Result = {((double)result / iteration) / 1000000}");
        }

    }
}