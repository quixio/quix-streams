using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quix.Sdk.Streaming.Models;

namespace Quix.Sdk.Streaming.Samples.Samples
{
    public class ReadSampleWithTimeout
    {
        private Action onStop;

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
            
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security, new Dictionary<string, string>()
            {
                {"max.poll.interval.ms", "10000"}
            });
            var inputTopic = client.OpenInputTopic(Configuration.Config.Topic, Configuration.Config.ConsumerId);

            var closeReadTask = new TaskCompletionSource<object>();
            var nextFail = DateTime.MinValue;
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
                    // Inline manipulation
                    //data.Timestamps.ForEach(timestamp =>
                    //{
                    //    timestamp.AddValue("param1", timestamp.Values["param2"].NumericValue * 2);
                    //    timestamp.AddValue("param3", timestamp.Values["param1"].NumericValue + timestamp.Values["param2"].NumericValue);
                    //});

                    // Cloning data
                    // var outData = data.Clone();

                    // Send using writer buffer
                    //streamWriter.Parameters.Buffer.Write(data);
                    // Send without using writer buffer
                    //streamWriter.Parameters.Write(data);

                    Interlocked.Add(ref counter, data.Timestamps.Count);
                    if (nextFail <= DateTime.UtcNow)
                    {
                        nextFail = DateTime.UtcNow.AddMinutes(1);
                        Console.WriteLine("Wait 20");
                        Thread.Sleep(20000);
                    }
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

                streamReader.Properties.OnChanged += () =>
                {
                    Console.WriteLine($"Stream properties -> StreamId '{streamReader.StreamId}' with name '{streamReader.Properties.Name}' located in '{streamReader.Properties.Location}'");
                };
                
                streamReader.OnStreamClosed += (sr, set) =>
                {
                    Console.WriteLine($"Stream Close -> StreamId '{streamReader.StreamId}' with type {set}");
                    closeReadTask.SetResult(new object());
                };
            };

            inputTopic.StartReading();
            
            this.onStop = () =>
            {
                Console.WriteLine("Waiting for incoming stream end");
                closeReadTask.Task.GetAwaiter().GetResult(); // wait for close to be read
                Console.WriteLine("Waited for incoming stream end");
                inputTopic.Dispose();
            };
        }

        public void Stop()
        {
            this.onStop();
        }
    }
}