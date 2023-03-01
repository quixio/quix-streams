using System;
using System.Threading;
using QuixStreams.Streaming.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.PerformanceTest
{
    public class WritePerformanceTest
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


            TimeseriesData data = null;
            var iteration = 0;
            long result = 0;

            var timeIteration = 0;
            var datetime = DateTime.UtcNow.ToUnixNanoseconds();
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

                var raw = data.ConvertToTimeseriesDataRaw(false, false);

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