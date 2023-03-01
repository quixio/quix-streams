using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using QuixStreams.Streaming;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.Speedtest
{
    public class BufferTest
    {
        const string parameterName = "TimeParameter";
        public void Run(CancellationToken ct)
        {
            var times = new List<double>();
            //var timesTotal = 0;
            var timesLock = new object();
            
            CodecRegistry.Register(CodecType.Protobuf);

            
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            var topicConsumer = client.GetTopicConsumer(Configuration.Config.Topic, Configuration.Config.ConsumerId);
            var topicProducer = client.GetTopicProducer(Configuration.Config.Topic);

            var stream = topicProducer.CreateStream();
            Console.WriteLine("Test stream: " + stream.StreamId);
            
            topicConsumer.OnStreamReceived += (sender, reader) =>
            {
                if (reader.StreamId != stream.StreamId)
                {
                    Console.WriteLine("Ignoring " + reader.StreamId);
                    return;
                }

                Console.WriteLine("START READING " + reader.StreamId);
                var buffer = reader.Timeseries.CreateBuffer();
//                buffer.PacketSize = 111;
                buffer.TimeSpanInMilliseconds = buffer.BufferTimeout = 1000;

                buffer.OnRawReleased += (sender, args) =>
                {
//                    var binaryTime = (long) data.Timestamps[0].Parameters[parameterName].NumericValue;
                    
//                    var sentAt = DateTime.FromBinary(binaryTime);
//                    var elapsed = (DateTime.UtcNow - sentAt).TotalMilliseconds;
                    Console.WriteLine("Released "+args.Data.Timestamps.Count());
//                    Console.WriteLine("Released "+data.Timestamps.Count());

/*                    lock (timesLock)
                    {
                        times.Add(elapsed);
                        timesTotal++;
                        times = times.TakeLast(50).ToList();

                        Console.WriteLine("Avg: " + Math.Round(times.Average(), 2) + ", Max: " +
                                          Math.Round(times.Max(), 2) + ", Min: " + Math.Round(times.Min(), 2) +
                                          ", over last " + times.Count + " out of " + timesTotal);
                    }
                    */
                };
            };
            topicConsumer.Subscribe();

            const int size = 500;
            int totalcnt = 0;
            while (!ct.IsCancellationRequested)
            {
                var dataraw = this.generateRawChunk(size, totalcnt+=size);
                stream.Timeseries.Publish(dataraw);
                stream.Timeseries.Flush();
                Thread.Sleep(5);
            }
            
            stream.Close();
            topicConsumer.Dispose();
        }

        protected TimeseriesDataRaw generateRawChunk(int size, int startTime)
        {
            long[] timestamps = new long[size];
            double?[] numerics1 = new double?[size];
            long curtm = DateTime.UtcNow.ToUnixNanoseconds();
            for (var i = 0; i < size; i++)
            {
                numerics1[i] = i;
                timestamps[i] = curtm + i;
            }

            var numericValues = new Dictionary<string, double?[]>();
            numericValues.Add(parameterName, numerics1);
            
            return new TimeseriesDataRaw(
                0, 
                timestamps, 
                numericValues,
                new Dictionary<string, string[]>(),
                new Dictionary<string, byte[][]>(),
                new Dictionary<string, string[]>()
            );
        }
    }
}