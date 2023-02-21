using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Quix.Sdk;
using Quix.Sdk.Process.Models;
using Quix.Sdk.Streaming;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Transport.Fw;

namespace Quix.Sdk.ManyStreamTest
{
    public class StreamingTest
    {
        public void Run(CancellationToken ct)
        {
            Logging.UpdateFactory(LogLevel.Trace);
            CodecRegistry.Register(CodecType.ImprovedJson);
            
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);

            var topicConsumer = client.CreateTopicConsumer(Configuration.Config.Topic, Configuration.Config.ConsumerId);
            var topicProducer = client.CreateTopicProducer(Configuration.Config.Topic);

            int streamCounter = 0;
            
            topicConsumer.OnStreamReceived += (sender, reader) =>
            {
                reader.OnStreamClosed += (sr, end) =>
                {
                    streamCounter++;
                    Console.WriteLine($"Stream count: {streamCounter}");
                };
                /*var buffer = reader.Parameters.CreateBuffer();
                buffer.PacketSize = 1;

                buffer.OnRead += (sender, data) =>
                {
                    streamCounter++; 
                    Console.WriteLine($"Stream count: {streamCounter}");
                };*/
            };
            topicConsumer.Subscribe();

            while (!ct.IsCancellationRequested)
            {
                var stream = topicProducer.CreateStream();
                var data = new Quix.Sdk.Streaming.Models.TimeseriesData();
                data.AddTimestampNanoseconds(10).AddValue("test", DateTime.UtcNow.ToBinary());
                stream.Parameters.Buffer.Write(data);
                stream.Events.AddTimestampNanoseconds(10).AddValue("test1", "val1");
                stream.Properties.Location = "/test";
                stream.Parameters.AddDefinition("test");
                stream.Events.AddDefinition("test1");
                stream.Close();
            }
            topicConsumer.Dispose();
        }
    }
}