using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ConsoleTables;
using FluentAssertions.Extensions;
using MathNet.Numerics.Statistics;
using Microsoft.Extensions.Logging;
using QuixStreams;
using QuixStreams.Kafka.Transport.SerDes;
using QuixStreams.Streaming.UnitTests.Helpers;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.ThroughputTest
{
    public class StreamingTestRaw
    {   
        public const string TestName = "Baseline";
        public void Run(CancellationToken ct, bool useBuffer = false)
        {
            var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
            // usage stuff
            
            QuixStreams.Logging.UpdateFactory(LogLevel.Debug);
            QuixStreams.Kafka.Transport.SerDes.PackageSerializationSettings.Mode = PackageSerializationMode.Header;
            var client = new TestStreamingClient(CodecType.Protobuf);

            var topicConsumer = client.GetTopicConsumer();
            var topicProducer = client.GetTopicProducer();

            var timer = new System.Timers.Timer()
            {
                Interval = 1000, Enabled = false, AutoReset = false
            };
            var sw = Stopwatch.StartNew();
            long totalAmount = 0;
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

                    var amount = totalAmount;
                    Interlocked.Add(ref totalAmount, -amount);
                    
                    var cpu = Math.Round(currentProcess.TotalProcessorTime.TotalMilliseconds / sw.Elapsed.TotalMilliseconds * 100.0, 3);
                    var mem = Math.Round(currentProcess.WorkingSet64 / 1024.0 / 1024, 2);
                    
                    
                    var avg = Math.Round(amount / parameterTimer.Elapsed.TotalSeconds);
                    parameterTimer.Restart();
                    parameters.Add(avg);
                    
                    var mean = parameters.Mean();
                    var std = parameters.StandardDeviation();

                    var removedMessage = "";
                    if (parameters.Count > 110)
                    {
                        // keep last 100 measurements
                        var before = parameters.Count;
                        parameters = parameters.Skip(parameters.Count - 100).ToList();
                        removedMessage = $"Trimmed {before-parameters.Count} measurements";
                    }
                    
                    Console.Clear();
                    
                    // remove outliers
                    var outliersRemovedMessage = "";
                    if (!double.IsNaN(std) || parameters.Count % 10 == 0)
                    {
                        var lowThreshold = mean - std * 2;
                        var highThreshold = mean + std * 2;
                        var initialCount = parameters.Count;
                        var considered = parameters.Where(x => lowThreshold < x && x < highThreshold).ToList();
                        
                        var afterCount = considered.Count;
                        if (initialCount != afterCount)
                        {
                            outliersRemovedMessage = $"{initialCount - afterCount} outliers were removed";
                            mean = parameters.Mean();
                            std = parameters.StandardDeviation();
                        }

                    }

                    var table = new ConsoleTable("Test", "Buffer", "Params/s", "Std", "Sample Count", "CPU", "Mem MB");
                    table.AddRow(TestName, useBuffer, mean.ToString("n0"), std.ToString("n0"), parameters.Count.ToString("n0"), cpu.ToString("n0"), mem.ToString("n0"));
                    table.Write(Format.MarkDown);
                    
                    Console.WriteLine(outliersRemovedMessage);
                    Console.WriteLine(removedMessage);

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

            topicConsumer.OnStreamReceived += (sender, reader) =>
            {

                timer.Start();
                if (useBuffer)
                {
                    var buffer = reader.Timeseries.CreateBuffer();
                    buffer.PacketSize = 100;
                    buffer.OnRawReleased += (sender2, args) => 
                    {
                        var amount = args.Data.NumericValues?.Keys.Count ?? 0;
                        amount += args.Data.StringValues?.Keys.Count ?? 0;
                        amount *= args.Data.Timestamps?.Length ?? 0;
                        Interlocked.Add(ref totalAmount, amount);
                    };
                }
                else
                {
                    reader.Timeseries.OnRawReceived += (sender2, args) => 
                    {
                        var amount = args.Data.NumericValues?.Keys.Count ?? 0;
                        amount += args.Data.StringValues?.Keys.Count ?? 0;
                        amount *= args.Data.Timestamps?.Length ?? 0;
                        Interlocked.Add(ref totalAmount, amount);
                    };
                }
            };
            topicConsumer.Subscribe();

            var density = 100;
            var totalSamples = 3000;
            var takeCount = (int)Math.Ceiling((double)totalSamples / density);
            Console.WriteLine("Generating data...");
            var datalist = GenerateData(density).Take(takeCount).ToList();
            bool doPerParameterSample = true;
            if (doPerParameterSample)
            {
                datalist = ConvertToPerParameter(datalist);
            }

            Parallel.For(0, 1, (i) =>
            {
                var stream = topicProducer.CreateStream();
                Console.WriteLine("Test stream: " + stream.StreamId);
                stream.Timeseries.Buffer.PacketSize = 100;
                // stream.Timeseries.Buffer.TimeSpanInMilliseconds = 2000;
                // stream.Timeseries.Buffer.BufferTimeout = 1000;
                // stream.Timeseries.Buffer.PacketSize = 1; // To not keep messages around and send immediately 

                stream.Epoch = DateTime.UtcNow;
                stream.Properties.Name = "Throughput test Stream"; // this is here to avoid sending data until reader is ready

                var index = 0;
                while (!ct.IsCancellationRequested)
                {
                    stream.Timeseries.Buffer.Publish(datalist[index]);
                    index = (index + 1) % datalist.Count;
                }
                
                stream.Close();
            });
            
            topicConsumer.Dispose();
        }

        private static List<TimeseriesDataRaw> ConvertToPerParameter(List<TimeseriesDataRaw> datalist)
        {
            // ignoring tags for now, as original data is not expected to have it
            
            var newList = new List<TimeseriesDataRaw>();
            foreach (var data in datalist)
            {
                for (var index = 0; index < data.Timestamps.Length; index++)
                {
                    var ts = data.Timestamps[index];
                    foreach (var pair in data.NumericValues)
                    {
                        var value = pair.Value[index];
                        if (!value.HasValue) continue;
                        var newData = new TimeseriesDataRaw(data.Epoch, new[] { ts },
                            new Dictionary<string, double?[]>() { { pair.Key, new[] { value } } }, null, null, null);
                        newList.Add(newData);
                    }
                    
                    foreach (var pair in data.StringValues)
                    {
                        var value = pair.Value[index];
                        if (value == null) continue;
                        var newData = new TimeseriesDataRaw(data.Epoch, new[] { ts }, null,
                            new Dictionary<string, string[]>() { { pair.Key, new[] { value } } }, null, null);
                        newList.Add(newData);
                    }
                    
                    foreach (var pair in data.BinaryValues)
                    {
                        var value = pair.Value[index];
                        if (value == null) continue;
                        var newData = new TimeseriesDataRaw(data.Epoch, new[] { ts }, null, null,
                            new Dictionary<string, byte[][]>() { { pair.Key, new[] { value } } }, null);
                        newList.Add(newData);
                    }
                }
            }

            return newList;
        }

        private TimeseriesDataRaw GenerateDataRaw(Generator generator, List<string> stringParameters, List<string> numericParameters, long timestampStart, long delta, int count)
        {

            var timestamps = new long[count];

            var numericValues = new Dictionary<string, double?[]>();
            var stringValues = new Dictionary<string, string[]>();

            var data = new TimeseriesDataRaw();
            for (var loopCount = 0; loopCount < count; loopCount++)
            {
                timestamps[loopCount] = timestampStart + delta * loopCount;
                foreach (var stringParameter in stringParameters)
                {
                    if (!generator.HasValue())
                    {
                        continue;
                    }

                    if (!stringValues.TryGetValue(stringParameter, out var stringArray))
                    {
                        stringArray = new string[count];
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
                        numericArray = new double?[count];
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

        private IEnumerable<TimeseriesDataRaw> GenerateData(int samplePerRaw)
        {
            var generator = new Generator();
            var stringParameters = generator.GenerateParameters(10).ToList();
            var numericParameters = generator.GenerateParameters(90).ToList();


            var time = DateTime.UtcNow.ToBinary();
            var delta = (long)TimeSpan.FromSeconds(1).TotalNanoseconds();

            while (true)
            {
                var count = samplePerRaw;
                var result = GenerateDataRaw(generator, stringParameters, numericParameters, time, delta, count);
                time += result.Timestamps.Last() + delta;
                yield return result;
            }
        }

    }
}