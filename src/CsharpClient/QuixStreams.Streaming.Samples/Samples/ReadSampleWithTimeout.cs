using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.Models.StreamConsumer;

namespace QuixStreams.Streaming.Samples.Samples
{
    public class ReadSampleWithTimeout
    {
        private Action onStop;
        private long counter;

        public void Start(string streamIdToRead)
        {
            counter = 0;
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
            var topicConsumer = client.GetTopicConsumer(Configuration.Config.Topic, Configuration.Config.ConsumerId);

            var closeReadTask = new TaskCompletionSource<object>();
            var nextFail = DateTime.MinValue;
            topicConsumer.OnStreamReceived += (sender, streamConsumer) =>
            {
                if (streamConsumer.StreamId != streamIdToRead) return;
                var bufferConfiguration = new TimeseriesBufferConfiguration
                {
                    PacketSize = 100,
                    TimeSpanInMilliseconds = null,
                    TimeSpanInNanoseconds = null,
                    BufferTimeout = null,
                    //CustomTrigger = data => data.Timestamps.Count == 1,
                    //CustomTriggerBeforeEnqueue = (timestamp) => timestamp.TimestampMilliseconds % 1000 == 0,
                    //CustomFilter = (timestamp) => timestamp.TimestampMilliseconds % 1000 == 0
                };

                var buffer = streamConsumer.Timeseries.CreateBuffer(bufferConfiguration);

                buffer.OnDataReleased += (s, args) =>
                {
                    // Inline manipulation
                    //data.Timestamps.ForEach(timestamp =>
                    //{
                    //    timestamp.AddValue("param1", timestamp.Values["param2"].NumericValue * 2);
                    //    timestamp.AddValue("param3", timestamp.Values["param1"].NumericValue + timestamp.Values["param2"].NumericValue);
                    //});

                    // Cloning data
                    // var outData = data.Clone();

                    // Send using buffer
                    //streamProducer.Timeseries.Buffer.Publish(data);
                    // Send without using buffer
                    //streamProducer.Timeseries.Publish(data);

                    Interlocked.Add(ref counter, args.Data.Timestamps.Count);
                    if (nextFail <= DateTime.UtcNow)
                    {
                        nextFail = DateTime.UtcNow.AddMinutes(1);
                        Console.WriteLine("Wait 20");
                        Thread.Sleep(20000);
                    }
                };
                
                streamConsumer.Events.OnDataReceived += EventsDataReceived;
                streamConsumer.Timeseries.OnDefinitionsChanged += OnParameterDefinitionsChanged;
                streamConsumer.Events.OnDefinitionsChanged += OnEventDefinitionsChanged;
                streamConsumer.Properties.OnChanged += OnPropertiesChanged;
                streamConsumer.OnStreamClosed += (s, args) =>
                {
                    Console.WriteLine($"Stream Close -> StreamId '{streamConsumer.StreamId}' with type {args.EndType}");
                    closeReadTask.SetResult(new object());
                };
            };

            topicConsumer.Subscribe();
            
            this.onStop = () =>
            {
                Console.WriteLine("Waiting for incoming stream end");
                closeReadTask.Task.GetAwaiter().GetResult(); // wait for close to be read
                Console.WriteLine("Waited for incoming stream end");
                topicConsumer.Dispose();
            };
        }

        void EventsDataReceived(object sender, EventDataReadEventArgs args)
        {
            Console.WriteLine($"Event data -> StreamId: '{args.Stream.StreamId}' - Event '{args.Data.Id}' with value '{args.Data.Value}'");
        }

        void OnParameterDefinitionsChanged(object sender, ParameterDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Timeseries.Definitions)
            {
                Console.WriteLine($"Parameter definition -> StreamId: {args.Stream.StreamId} - Parameter definition '{definition.Id}' with name '{definition.Name}'");
            }
        }

        void OnEventDefinitionsChanged(object sender, EventDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Events.Definitions)
            {
                Console.WriteLine($"Event definition -> StreamId: {args.Stream.StreamId} - Event definition '{definition.Id}' with name '{definition.Name}'");
            }
        }
        
        void OnPropertiesChanged(object stream, StreamPropertiesChangedEventArgs args)
        {
            Console.WriteLine($"Stream properties -> StreamId '{args.Stream.StreamId}' with name '{args.Stream.Properties.Name}' located in '{args.Stream.Properties.Location}'");
        }

        public void Stop()
        {
            this.onStop();
        }
    }
}