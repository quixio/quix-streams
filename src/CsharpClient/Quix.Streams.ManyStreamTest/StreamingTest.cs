using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using Quix.Streams.Process.Models;
using Quix.Streams.Streaming;

namespace Quix.Streams.ManyStreamTest
{
    public class StreamingTest
    {
        public void Run(CancellationToken ct)
        {
            Logging.UpdateFactory(LogLevel.Trace);
            CodecRegistry.Register(CodecType.ImprovedJson);
            
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            var topicConsumer = client.GetTopicConsumer(Configuration.Config.Topic, Configuration.Config.ConsumerId);
            var topicProducer = client.GetTopicProducer(Configuration.Config.Topic);

            int streamCounter = 0;
            
            topicConsumer.OnStreamReceived += (sender, reader) =>
            {
                reader.OnStreamClosed += (sr, end) =>
                {
                    streamCounter++;
                    Console.WriteLine($"Stream count: {streamCounter}");
                };
                /*var buffer = reader.Timeseries.CreateBuffer();
                buffer.PacketSize = 1;

                buffer.OnReceived += (sender, data) =>
                {
                    streamCounter++; 
                    Console.WriteLine($"Stream count: {streamCounter}");
                };*/
            };
            topicConsumer.Subscribe();

            while (!ct.IsCancellationRequested)
            {
                var stream = topicProducer.CreateStream();
                var data = new Quix.Streams.Streaming.Models.TimeseriesData();
                data.AddTimestampNanoseconds(10).AddValue("test", DateTime.UtcNow.ToBinary());
                stream.Timeseries.Buffer.Publish(data);
                stream.Events.AddTimestampNanoseconds(10).AddValue("test1", "val1");
                stream.Properties.Location = "/test";
                stream.Timeseries.AddDefinition("test");
                stream.Events.AddDefinition("test1");
                stream.Close();
            }
            topicConsumer.Dispose();
        }
    }
}