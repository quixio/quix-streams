using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using QuixStreams.Streaming;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.ThroughputTest
{
    /*
    public class StreamingTest
    {
        public void Run(CancellationToken ct)
        {
            var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
            // usage stuff
            var card = new PerformanceCounterCategory("Network Interface").GetInstanceNames()[0];
            var networkBytesSent = new PerformanceCounter("Network Interface", "Bytes Sent/sec", card, true);
            var networkBytesReceived = new PerformanceCounter("Network Interface", "Bytes Received/sec", card,true);
            var networkBytesTotal = new PerformanceCounter("Network Interface", "Bytes Total/sec", card,true);
            
            
            TimeSpan checkPeriod = TimeSpan.FromSeconds(30);
            var dequeued = new List<Entry>();
            var readData = new ConcurrentQueue<Entry>();
            
            CodecRegistry.Register(CodecType.CompactJsonForBetterPerformance);
            
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            var topicConsumer = client.GetTopicConsumer(Configuration.Config.Topic);
            var topicProducer = client.GetTopicProducer(Configuration.Config.Topic);

            var stream = topicProducer.CreateStream();
            Console.WriteLine("Test stream: " + stream.StreamId);

            var timer = new System.Timers.Timer()
            {
                Interval = 1000, Enabled = false, AutoReset = false
            };
            var sw = Stopwatch.StartNew();
            timer.Elapsed += (s, e) =>
            {
                try
                {
                    while (readData.TryDequeue(out var entry))
                    {
                        dequeued.Add(entry);
                    }

                    if (!dequeued.Any()) return;
                    var last = dequeued[^1];
                    dequeued = dequeued.Where(x => last.ReceivedTime - x.ReceivedTime <= checkPeriod).ToList();

                    if (!dequeued.Any()) Console.WriteLine("Avg: No data in period");

                    var min = dequeued.Min(x => x.ReceivedTime);
                    var max = dequeued.Max(x => x.ReceivedTime);
                    var elapsed = max - min;
                    Console.WriteLine(
                        "Avg: " + Math.Round(dequeued.Sum(x => x.Amount) / elapsed.TotalMilliseconds * 1000, 2) +
                        $"/s, over {elapsed:g}");
                    Console.WriteLine($"  CPU: {Math.Round(currentProcess.TotalProcessorTime.TotalMilliseconds / (double)sw.Elapsed.TotalMilliseconds * 100, 3)}%");
                    Console.WriteLine($"  Mem MB: {Math.Round(currentProcess.WorkingSet64 /1024D/1024, 2)}");
                    Console.WriteLine($"  Sent MBits: {Math.Round(networkBytesSent.NextValue()/1024D/1024 * 8, 2)}");
                    Console.WriteLine($"  Received MBits: {Math.Round(networkBytesReceived.NextValue()/1024D/1024 *8, 2)}");
                    Console.WriteLine($"  Total MBits: {Math.Round(networkBytesTotal.NextValue()/1024D/1024 *8, 2)}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
                finally
                {
                    timer.Start();
                }
            };
            var mre = new ManualResetEvent(false);
            topicConsumer.OnStreamReceived += (sender, reader) =>
            {
                if (reader.StreamId != stream.StreamId)
                {
                    Console.WriteLine("Ignoring " + reader.StreamId);
                    return;
                }

                mre.Set();

                var buffer = reader.Timeseries.CreateBuffer();
                buffer.TimeSpanInMilliseconds = 0; // this will cause it to give me batches for roughly each loop

                buffer.OnDataReleased += (sender, args) =>
                {
                    var amount = args.Data.Timestamps.Sum(x => x.Parameters.Count);
                    readData.Enqueue(new Entry
                    {
                        ReceivedTime = DateTime.UtcNow, Amount = amount
                    });

                };
                
            };
            topicConsumer.Subscribe();

            
            stream.Timeseries.Buffer.PacketSize = 1000;
            stream.Timeseries.Buffer.TimeSpanInMilliseconds = 1000;
            stream.Timeseries.Buffer.BufferTimeout = 1000;

            var generator = new Generator();
            var stringParameters = generator.GenerateParameters(10).ToList();
            var numericParameters = generator.GenerateParameters(90).ToList();
            long index = 0;
            stream.Epoch = DateTime.UtcNow;
            timer.Start();
            stream.Properties.Name = "Throughput test Stream"; // this is here to avoid sending data until reader is ready
            while (!ct.IsCancellationRequested)
            {
                if (mre.WaitOne(TimeSpan.FromSeconds(1))) break;
            }

            while (!ct.IsCancellationRequested)
            {
                var data = new QuixStreams.Streaming.Models.TimeseriesData();
                for (var loopCount = 0; loopCount < 15; loopCount++)
                {
                    var builder = data.AddTimestampMilliseconds(index);
                    foreach (var stringParameter in stringParameters)
                    {
                        if (!generator.HasValue()) continue;
                        builder.AddValue(stringParameter, generator.GenerateStringValue(8));
                    }
                    foreach (var numericParameter in numericParameters)
                    {
                        if (!generator.HasValue()) continue;
                        builder.AddValue(numericParameter, generator.GenerateNumericValue());
                    }
                    index++;
                }
                stream.Timeseries.Buffer.Publish(data);
            }
            
            stream.Close();
            topicConsumer.Dispose();
        }

        private class Entry
        {
            public DateTime ReceivedTime;
            public long Amount;
        }
    }
    */
}