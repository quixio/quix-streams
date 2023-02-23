using System;
using System.Diagnostics;
using System.Threading;
using Quix.Streams.Streaming.Models;
using Quix.Streams.Streaming.Models.StreamConsumer;

namespace Quix.Streams.Streaming.Samples.Samples
{
    public class ReadSampleV2
    {
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
            
            var client = new KafkaStreamingClient(Configuration.Config.BrokerList, Configuration.Config.Security);
            var topicConsumer = client.CreateTopicConsumer(Configuration.Config.Topic, Configuration.Config.ConsumerId);

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

                var buffer = streamConsumer.Parameters.CreateBuffer(bufferConfiguration);


                buffer.OnRead += OnBufferRead;
                streamConsumer.Events.OnRead += OnEventsRead;
                streamConsumer.Parameters.OnDefinitionsChanged += OnParameterDefinitionsChanged;
                streamConsumer.Events.OnDefinitionsChanged += OnEventDefinitionsChanged;
                streamConsumer.Properties.OnChanged += OnPropertiesChanged;
                streamConsumer.OnStreamClosed += OnStreamClosed;
            };
            
            topicConsumer.OnDisposed += (s, e) =>
            {
                Console.WriteLine($"Topic consumer disposed");
            };
        }
        
        void OnBufferRead(object s, TimeseriesDataReadEventArgs args)
        {
            Interlocked.Add(ref counter, args.Data.Timestamps.Count);
        }
        
        void OnEventsRead(object s, EventDataReadEventArgs args)
        {
            Console.WriteLine($"Event data -> StreamId: '{args.Stream.StreamId}' - Event '{args.Data.Id}' with value '{args.Data.Value}'");
        }
        
        void OnParameterDefinitionsChanged(object s, ParameterDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Parameters.Definitions)
            {
                Console.WriteLine($"Parameter definition -> StreamId: {args.Stream.StreamId} - Parameter definition '{definition.Id}' with name '{definition.Name}'");
            }
        }
        
        void OnEventDefinitionsChanged(object s, EventDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Events.Definitions)
            {
                Console.WriteLine($"Event definition -> StreamId: {args.Stream.StreamId} - Event definition '{definition.Id}' with name '{definition.Name}'");
            }
        }
        void OnPropertiesChanged(object s, StreamPropertiesChangedEventArgs args)
        {
            Console.WriteLine($"Stream properties -> StreamId '{args.Stream.StreamId}' with name '{args.Stream.Properties.Name}' located in '{args.Stream.Properties.Location}'");
        }
        void OnStreamClosed(object s, StreamClosedEventArgs args)
        {
            Console.WriteLine($"Stream Close -> StreamId '{args.Stream.StreamId}' with type {args.EndType}");
        }
    }
}