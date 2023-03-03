using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using QuixStreams.Streaming;
using QuixStreams.Streaming.UnitTests;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.ThroughputTest
{
    public class StreamingTest
    {
        public void Run(CancellationToken ct)
        {
            var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
            
            TimeSpan checkPeriod = TimeSpan.FromSeconds(30);
            
            CodecRegistry.Register(CodecType.ImprovedJson);
            
            var client = new TestStreamingClient();

            var topicConsumer = client.GetTopicConsumer();
            var topicProducer = client.GetTopicProducer();

            var stream = topicProducer.CreateStream();
            Console.WriteLine("Test stream: " + stream.StreamId);

            var timer = new System.Timers.Timer()
            {
                Interval = 1000, Enabled = false, AutoReset = false
            };
            var sw = Stopwatch.StartNew();
            var totalAmount = 0;
            var parameterTimer = Stopwatch.StartNew();
            timer.Elapsed += (s, e) =>
            {
                try
                {
                    if (totalAmount == 0)
                    {
                        parameterTimer.Restart();
                        return;
                    }
                    var avg = Math.Round(totalAmount / parameterTimer.Elapsed.TotalSeconds);
                    var cpu = Math.Round(currentProcess.TotalProcessorTime.TotalMilliseconds / (double)sw.Elapsed.TotalMilliseconds * 100, 3);
                    var mem = Math.Round(currentProcess.WorkingSet64 / 1024D / 1024, 2);
                    
                    Console.Clear();

                    Console.WriteLine("| Params/s  | CPU | Mem MB |");
                    Console.WriteLine("| ---------:| ---:| ------:|");
                    Console.WriteLine($"| {avg:n0} | {cpu:n0}  | {mem:n0}    |");
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
                    totalAmount += amount;
                };
                
            };
            topicConsumer.Subscribe();

            
            stream.Timeseries.Buffer.PacketSize = 1000;
            stream.Timeseries.Buffer.TimeSpanInMilliseconds = 1000;
            stream.Timeseries.Buffer.BufferTimeout = 1000;

            var generator = new Generator();
            var stringParameters = generator.GenerateParameters(1).ToList();
            var numericParameters = generator.GenerateParameters(1).ToList();
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
                for (var loopCount = 0; loopCount < 3250; loopCount++)
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
    }
}