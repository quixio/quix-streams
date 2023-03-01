using System;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Streaming.Models;

namespace QuixStreams.Streaming.Samples.Samples
{
    public class WriteSample
    {
        public void Start(CancellationToken cancellationToken, string streamId)
        {
            Task.Run(() =>
            {
                var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
                var topicProducer = client.GetTopicProducer(Configuration.Config.Topic);

                using var stream = topicProducer.CreateStream(streamId);
                stream.Properties.Name = "Volvo car telemetry";
                stream.Properties.Location = "Car telemetry/Vehicles/Volvo";
                stream.Properties.AddParent("1234");
                stream.Properties.Metadata["test_key"] = "test_value";

                stream.Timeseries.AddDefinition("param1").SetRange(0, 10).SetUnit("kmh");
                stream.Timeseries.AddDefinition("param2").SetRange(0, 10).SetUnit("kmh");

                stream.Epoch = DateTime.UtcNow;

                stream.Events.AddDefinition("e1", "e1 name", "e1 description")
                    .SetLevel(Telemetry.Models.EventLevel.Critical);

                stream.Events.AddTimestampMilliseconds(10).AddValue("e1", "value 1").AddTag("tag1", "tagValue")
                    .Publish();

                stream.Timeseries.Buffer.PacketSize = 10;

                var i = 0;
                while (!cancellationToken.IsCancellationRequested)
                {
                    stream.Timeseries.Buffer.Publish(GenerateTimeseriesData(10 * i));
                    Thread.Sleep(10);
                    i++;
                }

                stream.Close();
            });
        }
        
        private static TimeseriesData GenerateTimeseriesData(int offset)
        {
            var data = new TimeseriesData();

            data.AddTimestampMilliseconds(offset)
                .AddValue("param1", offset)
                .AddValue("param2", offset);

            return data;
        }
    }
}