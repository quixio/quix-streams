using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using QuixStreams.Streaming;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Speedtest
{
    public class StreamingTestRaw
    {
        public void Run(CancellationToken ct)
        {
            var times = new List<double>();
            var timesTotal = 0;
            var timesLock = new object();
            
//            CodecRegistry.Register(CodecType.ImprovedJson);

            const string parameterName = "TimeParameter";
            
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

                var buffer = reader.Timeseries.CreateBuffer();
                buffer.PacketSize = 1;

                buffer.OnRawReleased += (sender, args) =>
                {
                    var binaryTime = (long) args.Data.Timestamps[0];
                    var sentAt = DateTime.FromBinary(binaryTime);
                    var elapsed = (DateTime.UtcNow - sentAt).TotalMilliseconds;
                    lock (timesLock)
                    {
                        times.Add(elapsed);
                        timesTotal++;
                        times = times.Skip(Math.Min(0,times.Count-50)).ToList();

                        Console.WriteLine("Avg: " + Math.Round(times.Average(), 2) + ", Max: " +
                                          Math.Round(times.Max(), 2) + ", Min: " + Math.Round(times.Min(), 2) +
                                          ", over last " + times.Count + " out of " + timesTotal);
                    }
                };
            };
            topicConsumer.Subscribe();

            stream.Timeseries.Buffer.PacketSize = 1; // To not keep messages around and send immediately 

            var PACKET_SIZE = 100000;
            
            while (!ct.IsCancellationRequested)
            {
                var data = new TimeseriesDataRaw();
                data.Epoch = 0;

                var timestamps = new long[PACKET_SIZE];
                var values = new double?[PACKET_SIZE];

                for (var i = 0; i < PACKET_SIZE; ++i)
                {
                    var tm = DateTime.UtcNow.ToBinary();
                    timestamps[i] = tm;
                    values[i] = tm;
                }
                data.Timestamps = timestamps;
                data.NumericValues.Add(parameterName, values);
                stream.Timeseries.Publish(data);
                Thread.Sleep(1);
            }
            
            stream.Close();
            topicConsumer.Dispose();
        }
    }
}