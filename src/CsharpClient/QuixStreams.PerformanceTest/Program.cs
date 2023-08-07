using System;
using System.Threading;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Kafka.Transport.SerDes.Legacy.MessageValue;
using QuixStreams.Streaming.Utils;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.PerformanceTest
{
    class Program
    {
        //long receivedCount = 0;
        //long sentCount = 0;

        static void Main(string[] args)
        {
            CodecSettings.SetGlobalCodecType(CodecType.Json);
            PackageSerializationSettings.Mode = PackageSerializationMode.LegacyValue;
            
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (s, e) =>
            {
                if (cts.IsCancellationRequested) return;
                Console.WriteLine("Cancelling....");
                e.Cancel = true;
                cts.Cancel();
            };

            //Console.WriteLine($"Only Conversion - Performance test ...");
            //new ReadPerformanceTest().Run(100, 100, cts.Token, true);
            //new WritePerformanceTest().Run(100, 100, cts.Token, true);

            //Console.WriteLine($"Reading performance test ...");
            //for (var paramCount = 1; paramCount <= 1000; paramCount *= 10)
            //{
            //    for (var bufferSize = 1; bufferSize <= 100; bufferSize *= 10)
            //    {
            //        new ReadPerformanceTest().Run(paramCount, bufferSize, cts.Token);
            //    }
            //}

            //Console.WriteLine($"Writing performance test ...");
            //for (var paramCount = 1; paramCount <= 1000; paramCount *= 10)
            //{
            //    for (var bufferSize = 1; bufferSize <= 100; bufferSize *= 10)
            //    {
            //        new WritePerformanceTest().Run(paramCount, bufferSize, cts.Token);
            //    }
            //}

            //Console.WriteLine($"Only Conversion - Performance test - Raw ...");
            //new ReadPerformanceTestRaw().Run(100, 100, cts.Token, true);
            //new WritePerformanceTestRaw().Run(100, 100, cts.Token, true);

            //Console.WriteLine($"Reading performance test - Raw  ...");
            //for (var paramCount = 1; paramCount <= 1000; paramCount *= 10)
            //{
            //    for (var bufferSize = 1; bufferSize <= 100; bufferSize *= 10)
            //    {
            //        new ReadPerformanceTestRaw().Run(paramCount, bufferSize, cts.Token);
            //    }
            //}

            //Console.WriteLine($"Writing performance test - Raw  ...");
            //for (var paramCount = 1; paramCount <= 1000; paramCount *= 10)
            //{
            //    for (var bufferSize = 1; bufferSize <= 100; bufferSize *= 10)
            //    {
            //        new WritePerformanceTestRaw().Run(paramCount, bufferSize, cts.Token);
            //    }
            //}

            new ReadPerformanceTestRaw().Run(1000, 1000, cts.Token, false, true);
            new WritePerformanceTestRaw().Run(1000, 1000, cts.Token, false, true);

            //new WritePerformanceTestQuix().Run(10, 1000, cts.Token, false, true);
            //new ReadPerformanceTestQuix().Run(10, 1000, cts.Token, false, true);

            //Console.WriteLine($"Writing performance test on Quix ...");
            //for (var paramCount = 1; paramCount <= 1000; paramCount *= 10)
            //{
            //    for (var bufferSize = 1; bufferSize <= 100; bufferSize *= 10)
            //    {
            //        new WritePerformanceTestQuix().Run(paramCount, bufferSize, cts.Token, true, true);
            //    }
            //}

        }
    }
}
