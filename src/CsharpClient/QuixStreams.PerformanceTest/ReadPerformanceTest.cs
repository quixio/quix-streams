using System;
using System.Threading;
using QuixStreams.Streaming.Models;

namespace QuixStreams.PerformanceTest
{
    public class ReadPerformanceTest
    {
        long receivedCount = 0;
        long sentCount = 0;

        public void Run(int paramCount, int bufferSize, CancellationToken ct, bool onlyReceive = false, bool showIntermediateResults = false)
        {

            var buffer = new TimeseriesBuffer(null, null, true, false);
            buffer.PacketSize = bufferSize;
            buffer.OnDataReleased += (sender, args) =>
            {
                if (onlyReceive)
                {
                    receivedCount += args.Data.Timestamps.Count * paramCount;
                    return;
                }

                foreach (var t in args.Data.Timestamps)
                {
                    //for (var i = 0; i < t.Parameters.Count; i++)
                    //{
                    //    var h = t.Parameters["param" + i.ToString()].NumericValue;
                    //    receivedCount++;
                    //}
                    foreach (var p in t.Parameters.Values)
                    {
                        var h = p.NumericValue;
                        var h2 = p.StringValue;
                        var h3 = p.BinaryValue;
                        receivedCount++;
                    }
                }

            };

            DateTime lastUpdate = DateTime.UtcNow;


            // Prepare data 
            var data = new TimeseriesData(100);
            for(var i = 0; i < 100; i++)
            {
                var timestamp = data.AddTimestampNanoseconds(i);

                for (var j = 0; j < paramCount; j++)
                {
                    timestamp.AddValue($"param{j}", j);
                }
                timestamp.AddTag("tagTest", "Test");

            }
            var raw = data.ConvertToTimeseriesDataRaw(false, false);

            var iteration = 0;
            long result = 0;
            while (!ct.IsCancellationRequested && iteration <= 20)
            {
                buffer.WriteChunk(raw);

                //var dataSimulated = new TimeseriesData(raw);

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