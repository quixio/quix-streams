using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using QuixStreams.Streaming;
using QuixStreams.Telemetry.Models;
using QuixStreams.ThroughputTest;

namespace QuixStreams.ThroughputTest
{
    public class StreamingTestRaw
    {
        public void Run(CancellationToken ct)
        {
            var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
            // usage stuff
            
            // CodecRegistry.Register(CodecType.ImprovedJson);
            
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

                buffer.OnRawReleased += (sender2, args) =>
                {
                    var amount = args.Data.NumericValues.Keys.Count;
                    amount += args.Data.StringValues.Keys.Count;
                    amount *= args.Data.Timestamps.Length;
                    totalAmount += amount;
                };
            };
            topicConsumer.Subscribe();

            
            // stream.Timeseries.Buffer.PacketSize = 1000;
            // stream.Timeseries.Buffer.TimeSpanInMilliseconds = 1000;
            // stream.Timeseries.Buffer.BufferTimeout = 1000;
            stream.Timeseries.Buffer.PacketSize = 1; // To not keep messages around and send immediately 

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
                var data = new TimeseriesDataRaw();
                data.Epoch = 0;
                var totalLength = 3250;

                var timestamps = new long[totalLength];
                
                var numericValues = new Dictionary<string, double?[]>();
                var stringValues = new Dictionary<string, string[]>();
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
                    
                    index++;
                }

                foreach (var numericParameter in numericParameters)
                {
                    data.NumericValues.Add(numericParameter, numericValues[numericParameter]);
                }
                foreach (var stringParameter in stringParameters)
                {
                    data.StringValues.Add(stringParameter, stringValues[stringParameter]);
                }
                data.Timestamps = timestamps;
                stream.Timeseries.Publish(data);
            }
            
            stream.Close();
            topicConsumer.Dispose();
        }
        
    }
}