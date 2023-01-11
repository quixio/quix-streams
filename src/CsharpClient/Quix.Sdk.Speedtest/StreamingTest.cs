using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming;

namespace Quix.Sdk.Speedtest
{
    public class StreamingTest
    {
        public void Run(CancellationToken ct)
        {
            var times = new List<double>();
            var timesTotal = 0;
            var timesLock = new object();
            
//            CodecRegistry.Register(CodecType.ImprovedJson);

            const string parameterName = "TimeParameter";
            
            var client = new StreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            var inputTopic = client.OpenInputTopic(Configuration.Config.Topic, Configuration.Config.ConsumerId);
            var outputTopic = client.OpenOutputTopic(Configuration.Config.Topic);

            var stream = outputTopic.CreateStream();
            Console.WriteLine("Test stream: " + stream.StreamId);
            
            inputTopic.OnStreamReceived += (sender, reader) =>
            {
                if (reader.StreamId != stream.StreamId)
                {
                    Console.WriteLine("Ignoring " + reader.StreamId);
                    return;
                }

                var buffer = reader.Parameters.CreateBuffer();
                buffer.PacketSize = 1;

                buffer.OnRead += (data) =>
                {
                    var binaryTime = (long) data.Timestamps[0].Parameters[parameterName].NumericValue;
                    var sentAt = DateTime.FromBinary(binaryTime);
                    var elapsed = (DateTime.UtcNow - sentAt).TotalMilliseconds;
                    lock (timesLock)
                    {
                        times.Add(elapsed);
                        timesTotal++;
                        times = times.TakeLast(50).ToList();

                        Console.WriteLine("Avg: " + Math.Round(times.Average(), 2) + ", Max: " +
                                          Math.Round(times.Max(), 2) + ", Min: " + Math.Round(times.Min(), 2) +
                                          ", over last " + times.Count + " out of " + timesTotal);
                    }
                };
            };
            inputTopic.StartReading();

            stream.Parameters.Buffer.PacketSize = 1; // To not keep messages around and send immediately 

            while (!ct.IsCancellationRequested)
            {
                var data = new Streaming.Models.ParameterData();
                data.AddTimestampNanoseconds(10).AddValue(parameterName, DateTime.UtcNow.ToBinary());
                stream.Parameters.Buffer.Write(data);
            }
            
            stream.Close();
            inputTopic.Dispose();
        }
    }
}