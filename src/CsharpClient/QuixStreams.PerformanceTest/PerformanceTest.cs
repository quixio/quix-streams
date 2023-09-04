using System;
using System.Threading;
using QuixStreams.Streaming.UnitTests;
using QuixStreams.Streaming.UnitTests.Helpers;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.PerformanceTest
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
            var client = new TestStreamingClient(CodecType.CompactJsonForBetterPerformance);

            //var topicConsumer = client.GetTopicConsumer();
            var topicProducer = client.GetTopicProducer();

            //topicConsumer.OnStreamReceived += (s, stream) =>
            //{
            //    var buffer = stream.Timeseries.CreateBuffer();
            //    buffer.PacketSize = readBufferSize;
            //    buffer.OnDataReleased += (sender, data) => 
            //    {
            //        foreach(var t in data.Timestamps)
            //        {
            //            receivedCount += t.Parameters.Count;
            //        }
            //    };
            //};

            //topicConsumer.Subscribe();

            var stream = topicProducer.CreateStream();
            stream.Timeseries.Buffer.PacketSize = writeBufferSize;

            DateTime lastUpdate = DateTime.UtcNow;

            while (!ct.IsCancellationRequested)
            {
                var builder = stream.Timeseries.Buffer.AddTimestamp(DateTime.UtcNow);
                for(var i = 0; i < paramCount; i++)
                {
                    builder = builder.AddValue($"param{i}", i);
                }
                builder.AddTag("tagTest", "Test");
                builder.Publish();

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
            //topicConsumer.Dispose();
        }

    }
}