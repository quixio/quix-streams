using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using ConsoleTables;
using MathNet.Numerics.Statistics;
using QuixStreams.Streaming;
using QuixStreams.Streaming.UnitTests;
using QuixStreams.Telemetry.Models;
using QuixStreams.ThroughputTest;

namespace QuixStreams.ThroughputTest
{
    public class StreamingTestRaw
    {
        public const string TestName = "Baseline";
        public void Run(CancellationToken ct, bool useBuffer = false)
        {
            var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
            // usage stuff
            
            // CodecRegistry.Register(CodecType.ImprovedJson);
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
            ulong totalAmount = 0;
            var parameterTimer = Stopwatch.StartNew();            
            var parameters = new List<double>();

            
            timer.Elapsed += (s, e) =>
            {
                try
                {
                    if (totalAmount == 0)
                    {
                        parameterTimer.Restart();
                        return;
                    }
                    
                    var cpu = Math.Round(currentProcess.TotalProcessorTime.TotalMilliseconds / sw.Elapsed.TotalMilliseconds * 100.0, 3);
                    var mem = Math.Round(currentProcess.WorkingSet64 / 1024.0 / 1024, 2);
                    
                    
                    var avg = Math.Round(totalAmount / parameterTimer.Elapsed.TotalSeconds);
                    totalAmount = 0;
                    parameterTimer.Restart();
                    parameters.Add(avg);
                    
                    var mean = parameters.Mean();
                    var std = parameters.StandardDeviation();
                    
                    Console.Clear();
                    
                    // remove outliers
                    var outliersRemovedMessage = "";
                    if (!double.IsNaN(std) && parameters.Count % 10 == 0)
                    {
                        var lowThreshold = mean - std * 2;
                        var highThreshold = mean + std * 2;
                        var initialCount = parameters.Count;
                        parameters = parameters.Where(x => lowThreshold < x && x < highThreshold).ToList();
                        var afterCount = parameters.Count;
                        if (initialCount != afterCount)
                        {
                            outliersRemovedMessage = $"{initialCount - afterCount} outliers were removed";
                        }
                        
                    }

                    var table = new ConsoleTable("Test", "Buffer", "Params/s", "Std", "CPU", "Mem MB");
                    table.AddRow(TestName, useBuffer, mean.ToString("n0"), std.ToString("n0"), cpu.ToString("n0"), mem.ToString("n0"));
                    table.Write(Format.MarkDown);
                    
                    Console.WriteLine(outliersRemovedMessage);
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
                timer.Start();

                var buffer = reader.Timeseries.CreateBuffer();
                buffer.PacketSize = 1;
                
                if (useBuffer)
                {
                    buffer.PacketSize = 1000;
                }
                //reader.Timeseries.OnRawReceived += (sender2, args) =>
                buffer.OnRawReleased += (sender2, args) => 
                {
                    var amount = args.Data.NumericValues.Keys.Count;
                    amount += args.Data.StringValues.Keys.Count;
                    amount *= args.Data.Timestamps.Length;
                    totalAmount += (ulong)amount;
                };
            };
            topicConsumer.Subscribe();

            
            stream.Timeseries.Buffer.PacketSize = 1000;
            // stream.Timeseries.Buffer.TimeSpanInMilliseconds = 1000;
            // stream.Timeseries.Buffer.BufferTimeout = 1000;
            //stream.Timeseries.Buffer.PacketSize = 1; // To not keep messages around and send immediately 

            
            stream.Epoch = DateTime.UtcNow;
            stream.Properties.Name = "Throughput test Stream"; // this is here to avoid sending data until reader is ready
            while (!ct.IsCancellationRequested)
            {
                if (mre.WaitOne(TimeSpan.FromSeconds(1))) break;
            }
            
            var index = 0;
            var totalSamples = 30;
            var datalist = GenerateData().Take(totalSamples).ToList();

            index = 0;
            while (!ct.IsCancellationRequested)
            {
                stream.Timeseries.Publish(datalist[index]);
                index = (index + 1) % totalSamples;
            }
            
            stream.Close();
            topicConsumer.Dispose();
        }

        private TimeseriesDataRaw GenerateDataRaw(Generator generator, List<string> stringParameters, List<string> numericParameters)
        {
            var totalLength = 3250;

            var timestamps = new long[totalLength];

            var numericValues = new Dictionary<string, double?[]>();
            var stringValues = new Dictionary<string, string[]>();

            var data = new TimeseriesDataRaw();
            for (var loopCount = 0; loopCount < totalLength; loopCount++)
            {
                timestamps[loopCount] = DateTime.UtcNow.ToBinary();
                foreach (var stringParameter in stringParameters)
                {
                    if (!generator.HasValue())
                    {
                        continue;
                    }

                    if (!stringValues.TryGetValue(stringParameter, out var stringArray))
                    {
                        stringArray = new string[totalLength];
                        stringValues[stringParameter] = stringArray;
                    }

                    stringArray[loopCount] = generator.GenerateStringValue(8);
                }

                foreach (var numericParameter in numericParameters)
                {
                    if (!generator.HasValue())
                    {
                        continue;
                    }

                    if (!numericValues.TryGetValue(numericParameter, out var numericArray))
                    {
                        numericArray = new double?[totalLength];
                        numericValues[numericParameter] = numericArray;
                    }

                    numericArray[loopCount] = generator.GenerateNumericValue();
                }
            }

            data.StringValues = stringValues;
            data.NumericValues = numericValues;
            data.Timestamps = timestamps;

            return data;
        }

        private IEnumerable<TimeseriesDataRaw> GenerateData()
        {
            var generator = new Generator();
            var stringParameters = generator.GenerateParameters(10).ToList();
            var numericParameters = generator.GenerateParameters(90).ToList();


            while (true)
            {
                yield return GenerateDataRaw(generator, stringParameters, numericParameters);
            }
        }

    }
}