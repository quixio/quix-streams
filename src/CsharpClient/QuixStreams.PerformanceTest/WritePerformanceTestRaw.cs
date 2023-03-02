using System;
using System.Collections.Generic;
using System.Threading;
using QuixStreams.Streaming.Models;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.PerformanceTest
{
    public class WritePerformanceTestRaw
    {
        long receivedCount = 0;
        long sentCount = 0;

        public void Run(int paramCount, int bufferSize, CancellationToken ct, bool onlySent = false, bool showIntermediateResults = false)
        {

            var buffer = new TimeseriesBuffer(null, null, true, true);
            buffer.PacketSize = bufferSize;
            buffer.OnRawReleased += (sender, args) =>
            {
                receivedCount += args.Data.Timestamps.Length * paramCount;
            };

            DateTime lastUpdate = DateTime.UtcNow;


            TimeseriesDataRaw raw = null;
            var iteration = 0;
            long result = 0;

            var timeIteration = 0;
            var datetime = DateTime.UtcNow.ToUnixNanoseconds();
            while (!ct.IsCancellationRequested && iteration <= 20)
            {
                var time = datetime + (timeIteration * 100);

                //Timeseries Data Raw
                if (!onlySent || iteration == 0)
                {
                    raw = new TimeseriesDataRaw();
                    raw.Timestamps = new long[100];
                    raw.NumericValues = new Dictionary<string, double?[]>();
                    raw.StringValues = new Dictionary<string, string[]>();
                    raw.BinaryValues = new Dictionary<string, byte[][]>();
                    raw.TagValues = new Dictionary<string, string[]>();

                    var tags = new string[100];
                    raw.TagValues.Add("tagTest", tags);

                    for (var i = 0; i < 100; i++)
                    {
                        raw.Timestamps[i] = time + i;

                        for (var j = 0; j < paramCount; j++)
                        {
                            if (!raw.NumericValues.TryGetValue("param" + j.ToString(), out var values))
                            {
                                values = new double?[100];
                                raw.NumericValues.Add("param" + j.ToString(), values);
                            }

                            values[i] = j;
                        }

                        tags[i] = "Test" + timeIteration.ToString();
                    }
                }

                buffer.WriteChunk(raw);

                sentCount += paramCount * raw.Timestamps.Length;
                timeIteration++;

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