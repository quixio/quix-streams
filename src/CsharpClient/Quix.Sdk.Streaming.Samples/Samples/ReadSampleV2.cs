using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Streaming.Models.StreamConsumer;

namespace Quix.Sdk.Streaming.Samples.Samples
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


                buffer.OnRead += OnBufferOnOnRead;
                streamConsumer.Events.OnRead += OnEventsOnOnRead;
                streamConsumer.Parameters.OnDefinitionsChanged += OnParametersOnOnDefinitionsChanged;
                streamConsumer.Events.OnDefinitionsChanged += OnEventsOnOnDefinitionsChanged;
                streamConsumer.Properties.OnChanged += OnPropertiesOnOnChanged;
                streamConsumer.OnStreamClosed += OnStreamReaderOnOnStreamClosed;
            };
            
            topicConsumer.OnDisposed += (s, e) =>
            {
                Console.WriteLine($"Topic consumer disposed");
            };
        }
        
        void OnBufferOnOnRead(object s, TimeseriesDataReadEventArgs args)
        {
            Interlocked.Add(ref counter, args.Data.Timestamps.Count);
        }
        
        void OnEventsOnOnRead(object s, EventDataReadEventArgs args)
        {
            Console.WriteLine($"Event data -> StreamId: '{args.Stream.StreamId}' - Event '{args.Data.Id}' with value '{args.Data.Value}'");
        }
        
        void OnParametersOnOnDefinitionsChanged(object s, ParameterDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Parameters.Definitions)
            {
                Console.WriteLine($"Parameter definition -> StreamId: {args.Stream.StreamId} - Parameter definition '{definition.Id}' with name '{definition.Name}'");
            }
        }
        
        void OnEventsOnOnDefinitionsChanged(object s, EventDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Events.Definitions)
            {
                Console.WriteLine($"Event definition -> StreamId: {args.Stream.StreamId} - Event definition '{definition.Id}' with name '{definition.Name}'");
            }
        }
        void OnPropertiesOnOnChanged(object s, StreamPropertiesChangedEventArgs args)
        {
            Console.WriteLine($"Stream properties -> StreamId '{args.Stream.StreamId}' with name '{args.Stream.Properties.Name}' located in '{args.Stream.Properties.Location}'");
        }
        void OnStreamReaderOnOnStreamClosed(object s, StreamClosedEventArgs args)
        {
            Console.WriteLine($"Stream Close -> StreamId '{args.Stream.StreamId}' with type {args.EndType}");
        }
    }
}