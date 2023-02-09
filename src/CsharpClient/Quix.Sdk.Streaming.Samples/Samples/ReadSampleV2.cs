using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quix.Sdk.Streaming.Models;

namespace Quix.Sdk.Streaming.Samples.Samples
{
    public class ReadSampleV2
    {
        public void Start(string streamIdToRead)
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
            var inputTopic = client.OpenInputTopic(Configuration.Config.Topic, Configuration.Config.ConsumerId);

            inputTopic.OnStreamReceived += (sender, streamReader) =>
            {
                if (streamReader.StreamId != streamIdToRead) return;
                var bufferConfiguration = new ParametersBufferConfiguration
                {
                    PacketSize = 100,
                    TimeSpanInMilliseconds = null,
                    TimeSpanInNanoseconds = null,
                    BufferTimeout = null,
                    //CustomTrigger = data => data.Timestamps.Count == 1,
                    //CustomTriggerBeforeEnqueue = (timestamp) => timestamp.TimestampMilliseconds % 1000 == 0,
                    //CustomFilter = (timestamp) => timestamp.TimestampMilliseconds % 1000 == 0
                };

                var buffer = streamReader.Parameters.CreateBuffer(bufferConfiguration);

                buffer.OnRead += (data) =>
                {
                    Interlocked.Add(ref counter, data.Timestamps.Count);
                };

                streamReader.Events.OnRead += (data) =>
                {
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

                streamReader.Properties.OnChanged += (sender, properties) =>
                {
                    Console.WriteLine($"Stream properties -> StreamId '{streamReader.StreamId}' with name '{streamReader.Properties.Name}' located in '{streamReader.Properties.Location}'");
                };
                
                streamReader.OnStreamClosed += (sr, set) =>
                {
                    Console.WriteLine($"Stream Close -> StreamId '{streamReader.StreamId}' with type {set}");
                };
            };
            
            inputTopic.OnDisposed += (s, e) =>
            {
                Console.WriteLine($"input topic disposed");
            };
        }
    }
}