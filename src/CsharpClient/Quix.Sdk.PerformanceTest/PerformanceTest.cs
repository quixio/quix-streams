using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Streaming.Models.StreamWriter;
using Quix.Sdk.Streaming.UnitTests;

namespace Quix.Sdk.PerformanceTest
{
    public class PerformanceTest
    {
        long receivedCount = 0;
        long sentCount = 0;

        const int paramCount = 100;
        const int readBufferSize = 100;
        const int writeBufferSize = 100;

        public void Run(CancellationToken ct)
        {
            var client = new TestStreamingClient(CodecType.ImprovedJson);

            //var inputTopic = client.OpenInputTopic();
            var outputTopic = client.OpenOutputTopic();

            //inputTopic.OnStreamReceived += (s, stream) =>
            //{
            //    var buffer = stream.Parameters.CreateBuffer();
            //    buffer.PacketSize = readBufferSize;
            //    buffer.OnRead += (sender, data) => 
            //    {
            //        foreach(var t in data.Timestamps)
            //        {
            //            receivedCount += t.Parameters.Count;
            //        }
            //    };
            //};

            //inputTopic.StartReading();

            var stream = outputTopic.CreateStream();
            stream.Parameters.Buffer.PacketSize = writeBufferSize;

            DateTime lastUpdate = DateTime.UtcNow;

            while (!ct.IsCancellationRequested)
            {
                var builder = stream.Parameters.Buffer.AddTimestamp(DateTime.UtcNow);
                for(var i = 0; i < paramCount; i++)
                {
                    builder = builder.AddValue($"param{i}", i);
                }
                builder.AddTag("tagTest", "Test");
                builder.Write();

                sentCount += paramCount;

                if ((DateTime.UtcNow - lastUpdate).TotalSeconds >= 1)
                {
                    Console.WriteLine($"Timestamps - SEND {sentCount} - RECEIVED: {receivedCount}");

                    sentCount = 0;
                    receivedCount = 0;
                    lastUpdate = DateTime.UtcNow;
                }
            }

            stream.Close();
            //inputTopic.Dispose();
        }

    }
}