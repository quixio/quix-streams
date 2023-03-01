using System;
using System.Diagnostics;
using System.Threading;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.Models.StreamConsumer;

namespace QuixStreams.Streaming.Samples.Samples
{
    public class ReadSampleWithManualCommit
    {
        private long counter = 0;
        
        public void Start(CancellationToken cancellationToken)
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
            var topicConsumer = client.GetTopicConsumer(Configuration.Config.Topic, Configuration.Config.ConsumerId, CommitMode.Manual);

            topicConsumer.OnStreamReceived += (s, streamConsumer) =>
            {
                var bufferConfiguration = new TimeseriesBufferConfiguration
                {
                    PacketSize = 100,
                    TimeSpanInMilliseconds = null,
                    TimeSpanInNanoseconds = null,
                    BufferTimeout = null,
                };

                var buffer = streamConsumer.Timeseries.CreateBuffer(bufferConfiguration);
                buffer.OnDataReleased += BufferDataReleased;

                streamConsumer.Timeseries.OnDataReceived += ParametersOnOnDataReceived;
                streamConsumer.Events.OnDataReceived += EventsDataReceived;
                streamConsumer.Timeseries.OnDefinitionsChanged += OnParameterDefinitionsChanged;
                streamConsumer.Events.OnDefinitionsChanged += OnEventDefinitionsChanged;
                streamConsumer.Properties.OnChanged += OnPropertiesChanged;
            };

            topicConsumer.Subscribe();

            cancellationToken.Register(() =>
            {
                topicConsumer.Dispose();
            });
        }
        
        void OnPropertiesChanged(object s, StreamPropertiesChangedEventArgs args)
        {
            Console.WriteLine($"Stream properties -> StreamId '{args.Stream.StreamId}' with name '{args.Stream.Properties.Name}' located in '{args.Stream.Properties.Location}'");
        }
        
        void OnEventDefinitionsChanged(object s, EventDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Events.Definitions)
            {
                Console.WriteLine($"Event definition -> StreamId: {args.Stream.StreamId} - Event definition '{definition.Id}' with name '{definition.Name}'");
            }
        }

        void OnParameterDefinitionsChanged(object s, ParameterDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Timeseries.Definitions)
            {
                Console.WriteLine($"Parameter definition -> StreamId: {args.Stream.StreamId} - Parameter definition '{definition.Id}' with name '{definition.Name}'");
            }
        }
        
        void EventsDataReceived(object s, EventDataReadEventArgs args)
        {
            args.TopicConsumer.Commit();
            Console.WriteLine($"Event data -> StreamId: '{args.Stream.StreamId}' - Event '{args.Data.Id}' with value '{args.Data.Value}'");
        }
        
        
        void ParametersOnOnDataReceived(object s, TimeseriesDataReadEventArgs args)
        {
            ((ITopicConsumer)args.Topic).Commit();
            Interlocked.Add(ref counter, args.Data.Timestamps.Count);
        }
        
        
        void BufferDataReleased(object s, TimeseriesDataReadEventArgs args)
        {
            // args.Topic.Commit(data); this doesn't work just yet
            Interlocked.Add(ref counter, args.Data.Timestamps.Count);
        }
    }
}