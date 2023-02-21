using System;
using System.Diagnostics;
using System.Threading;
using Quix.Sdk.Streaming.Models;
using Quix.Sdk.Streaming.Models.StreamReader;

namespace Quix.Sdk.Streaming.Samples.Samples
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
            var inputTopic = client.OpenInputTopic(Configuration.Config.Topic, Configuration.Config.ConsumerId, CommitMode.Manual);

            inputTopic.OnStreamReceived += (s, streamReader) =>
            {
                var bufferConfiguration = new TimeseriesBufferConfiguration
                {
                    PacketSize = 100,
                    TimeSpanInMilliseconds = null,
                    TimeSpanInNanoseconds = null,
                    BufferTimeout = null,
                };

                var buffer = streamReader.Parameters.CreateBuffer(bufferConfiguration);
                buffer.OnRead += OnBufferOnOnRead;

                streamReader.Parameters.OnRead += OnParametersOnOnRead;
                streamReader.Events.OnRead += OnEventsOnOnRead;
                streamReader.Parameters.OnDefinitionsChanged += OnParametersOnOnDefinitionsChanged;
                streamReader.Events.OnDefinitionsChanged += OnEventsOnOnDefinitionsChanged;
                streamReader.Properties.OnChanged += OnPropertiesOnOnChanged;
            };

            inputTopic.StartReading();

            cancellationToken.Register(() =>
            {
                inputTopic.Dispose();
            });
        }
        
        void OnPropertiesOnOnChanged(object s, StreamPropertiesChangedEventArgs args)
        {
            Console.WriteLine($"Stream properties -> StreamId '{args.Stream.StreamId}' with name '{args.Stream.Properties.Name}' located in '{args.Stream.Properties.Location}'");
        }
        
        void OnEventsOnOnDefinitionsChanged(object s, EventDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Events.Definitions)
            {
                Console.WriteLine($"Event definition -> StreamId: {args.Stream.StreamId} - Event definition '{definition.Id}' with name '{definition.Name}'");
            }
        }

        void OnParametersOnOnDefinitionsChanged(object s, ParameterDefinitionsChangedEventArgs args)
        {
            foreach (var definition in args.Stream.Parameters.Definitions)
            {
                Console.WriteLine($"Parameter definition -> StreamId: {args.Stream.StreamId} - Parameter definition '{definition.Id}' with name '{definition.Name}'");
            }
        }
        
        void OnEventsOnOnRead(object s, EventDataReadEventArgs args)
        {
            args.Topic.Commit();
            Console.WriteLine($"Event data -> StreamId: '{args.Stream.StreamId}' - Event '{args.Data.Id}' with value '{args.Data.Value}'");
        }
        
        
        void OnParametersOnOnRead(object s, TimeseriesDataReadEventArgs args)
        {
            ((IInputTopic)args.Topic).Commit();
            Interlocked.Add(ref counter, args.Data.Timestamps.Count);
        }
        
        
        void OnBufferOnOnRead(object s, TimeseriesDataReadEventArgs args)
        {
            // args.Topic.Commit(data); this doesn't work just yet
            Interlocked.Add(ref counter, args.Data.Timestamps.Count);
        }
    }
}