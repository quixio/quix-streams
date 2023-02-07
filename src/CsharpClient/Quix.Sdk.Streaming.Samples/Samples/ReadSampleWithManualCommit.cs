using System;
using System.Diagnostics;
using System.Threading;
using Quix.Sdk.Streaming.Models;

namespace Quix.Sdk.Streaming.Samples.Samples
{
    public class ReadSampleWithManualCommit
    {
        public void Start(CancellationToken cancellationToken)
        {
            long counter = 0;
            var sw = Stopwatch.StartNew();
            var timer = new System.Timers.Timer();
            timer.Interval = 1000;
            timer.AutoReset = true;
            timer.Elapsed += (s, e) =>
            {
                Console.WriteLine($"{sw.Elapsed:g}: Parameter timestamps received {Interlocked.Read(ref counter)}");
            };
            timer.Start();
            
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var inputTopic = client.OpenInputTopic(Configuration.Config.Topic, Configuration.Config.ConsumerId, CommitMode.Manual);

            inputTopic.OnStreamReceived += (sender, streamReader) =>
            {
                var bufferConfiguration = new ParametersBufferConfiguration
                {
                    PacketSize = 100,
                    TimeSpanInMilliseconds = null,
                    TimeSpanInNanoseconds = null,
                    BufferTimeout = null,
                };

                var buffer = streamReader.Parameters.CreateBuffer(bufferConfiguration);

                buffer.OnRead += (data) =>
                {
                    // inputTopic.Commit(data); this doesn't work just yet
                    Interlocked.Add(ref counter, data.Timestamps.Count);
                };

                streamReader.Parameters.OnRead += (data) =>
                {
                    inputTopic.Commit();
                    Interlocked.Add(ref counter, data.Timestamps.Count);
                };

                streamReader.Events.OnRead += (data) =>
                {
                    inputTopic.Commit();
                    Console.WriteLine($"Event data -> StreamId: '{streamReader.StreamId}' - Event '{data.Id}' with value '{data.Value}'");
                };

                streamReader.Parameters.OnDefinitionsChanged += () =>
                {
                    foreach (var definition in streamReader.Parameters.Definitions)
                    {
                        Console.WriteLine($"Parameter definition -> StreamId: {streamReader.StreamId} - Parameter definition '{definition.Id}' with name '{definition.Name}'");
                    }
                };

                streamReader.Events.OnDefinitionsChanged += () =>
                {
                    foreach (var definition in streamReader.Events.Definitions)
                    {
                        Console.WriteLine($"Event definition -> StreamId: {streamReader.StreamId} - Event definition '{definition.Id}' with name '{definition.Name}'");
                    }
                };

                streamReader.Properties.OnChanged += () =>
                {
                    Console.WriteLine($"Stream properties -> StreamId '{streamReader.StreamId}' with name '{streamReader.Properties.Name}' located in '{streamReader.Properties.Location}'");
                };
            };

            inputTopic.StartReading();

            cancellationToken.Register(() =>
            {
                inputTopic.Dispose();
            });
        }
    }
}