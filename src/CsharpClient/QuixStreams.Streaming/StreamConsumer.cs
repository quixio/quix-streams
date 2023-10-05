using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using QuixStreams.Streaming.Models;
using QuixStreams.Streaming.Models.StreamConsumer;
using QuixStreams.Streaming.States;
using QuixStreams.Telemetry;
using QuixStreams.Telemetry.Models;
using QuixStreams.Telemetry.Models.Utility;

namespace QuixStreams.Streaming
{
    /// <summary>
    /// Handles reading data for the assigned stream from the protocol.
    /// </summary>
    internal class StreamConsumer : StreamPipeline, IStreamConsumerInternal
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly ILogger logger = Logging.CreateLogger<StreamConsumer>();
        private readonly StreamPropertiesConsumer streamPropertiesConsumer;
        private readonly StreamTimeseriesConsumer streamTimeseriesConsumer;
        private readonly StreamEventsConsumer streamEventsConsumer;
        private bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamConsumer"/>
        /// This constructor is called internally by the <see cref="StreamPipelineFactory"/>
        /// </summary>
        /// <param name="topicConsumer">The topic the reader belongs to</param>
        /// <param name="id">Stream consumer identifier</param>
        internal StreamConsumer(ITopicConsumer topicConsumer, StreamConsumerId id): base(id.StreamId)
        {
            this.topicConsumer = topicConsumer;
            this.Id = id;
            
            // Managed readers
            this.streamPropertiesConsumer = new StreamPropertiesConsumer(this.topicConsumer, this);
            this.streamTimeseriesConsumer = new StreamTimeseriesConsumer(this.topicConsumer, this);
            this.streamEventsConsumer = new StreamEventsConsumer(this.topicConsumer, this);

            InitializeStreaming();
        }

        /// <summary>
        /// Exists for mocking purposes
        /// </summary>
        protected StreamConsumer()
        {
            
        }
        
        /// <inheritdoc />
        public StreamConsumerId Id { get; }
        
        /// <inheritdoc />
        public StreamPropertiesConsumer Properties => streamPropertiesConsumer;

        /// <inheritdoc />
        public StreamTimeseriesConsumer Timeseries => streamTimeseriesConsumer;

        /// <inheritdoc />
        public StreamEventsConsumer Events => streamEventsConsumer;

        /// <inheritdoc />
        public event EventHandler<PackageReceivedEventArgs> OnPackageReceived;

        /// <inheritdoc />
        public event EventHandler<StreamClosedEventArgs> OnStreamClosed;
        
        public StreamDictionaryState<T> GetDictionaryState<T>(string stateName, StreamStateDefaultValueDelegate<T> defaultValueFactory)
        {
            return this.GetStateManager().GetDictionaryState(stateName, defaultValueFactory);
        }
        
        public StreamScalarState<T> GetScalarState<T>(string stateName, StreamStateDefaultValueDelegate<T> defaultValueFactory)
        {
            return this.GetStateManager().GetScalarState(stateName, defaultValueFactory);
        }

        /// <inheritdoc />
        public StreamStateManager GetStateManager()
        {
            this.logger.LogTrace("Creating Stream state manager for {0}", StreamId);
            return StreamStateManager.GetOrCreate(
                this.topicConsumer,
                new StreamConsumerId(Id.ConsumerGroup, Id.TopicName, Id.Partition, StreamId),
                Logging.Factory);
            
        }
        
        /// <inheritdoc />
        public virtual event Action<IStreamConsumer, QuixStreams.Telemetry.Models.StreamProperties> OnStreamPropertiesChanged;

        /// <inheritdoc />
        public virtual event Action<IStreamConsumer, QuixStreams.Telemetry.Models.ParameterDefinitions> OnParameterDefinitionsChanged;

        /// <inheritdoc />
        public virtual event Action<IStreamConsumer, QuixStreams.Telemetry.Models.TimeseriesDataRaw> OnTimeseriesData;

        /// <inheritdoc />
        public virtual  event Action<IStreamConsumer, QuixStreams.Telemetry.Models.EventDataRaw> OnEventData;

        /// <inheritdoc />
        public virtual  event Action<IStreamConsumer, QuixStreams.Telemetry.Models.EventDefinitions> OnEventDefinitionsChanged;

        private void InitializeStreaming()
        {
            // Modifiers
            // this.AddComponent(SimpleModifier)

            this.Subscribe<QuixStreams.Telemetry.Models.StreamProperties>(OnStreamPropertiesReceived);
            this.Subscribe<QuixStreams.Telemetry.Models.TimeseriesDataRaw>(OnTimeseriesDataReceived);
            this.Subscribe<QuixStreams.Telemetry.Models.ParameterDefinitions>(OnParameterDefinitionsReceived);
            this.Subscribe<QuixStreams.Telemetry.Models.EventDataRaw[]>(OnEventDataReceived);
            this.Subscribe<QuixStreams.Telemetry.Models.EventDataRaw>(OnEventDataReceived);
            this.Subscribe<QuixStreams.Telemetry.Models.EventDefinitions>(OnEventDefinitionsReceived);
            this.Subscribe<QuixStreams.Telemetry.Models.StreamEnd>(OnStreamEndReceived);
            this.Subscribe(OnStreamPackageReceived);

            this.OnClosed += () =>
            {
                RaiseStreamClosed(StreamEndType.Terminated);
            };
        }

        private void OnStreamPackageReceived(IStreamPipeline streamPipeline, QuixStreams.Telemetry.Models.StreamPackage package)
        {
            if (package.Type == typeof(byte[]))
            {
                this.logger.LogTrace("StreamConsumer: OnStreamPackageReceived - raw message.");
                var ev = new EventDataRaw
                {
                    Timestamp = package.KafkaMessage.Timestamp.UtcDateTime.ToUnixNanoseconds(),
                    Id = streamPipeline.StreamId,
                    Tags = new Dictionary<string, string>(),
                    Value = Encoding.UTF8.GetString((byte[])package.Value)
                };

                this.OnEventData?.Invoke(this, ev);
                return;
            }


            this.logger.LogTrace("StreamConsumer: OnStreamPackageReceived");
            this.OnPackageReceived?.Invoke(this, new PackageReceivedEventArgs(this.topicConsumer, this, package));
        }

        private void OnStreamPropertiesReceived(IStreamPipeline streamPipeline, QuixStreams.Telemetry.Models.StreamProperties obj)
        {
            this.logger.LogTrace("StreamConsumer: OnStreamPropertiesReceived");
            this.OnStreamPropertiesChanged?.Invoke(this, obj);
        }

        private void OnTimeseriesDataReceived(IStreamPipeline streamPipeline, QuixStreams.Telemetry.Models.TimeseriesDataRaw obj)
        {
            this.logger.LogTrace("StreamConsumer: OnTimeseriesDataReceived. Data packet of size = {0}", obj.Timestamps.Length);
            this.OnTimeseriesData?.Invoke(this, obj);
        }

        private void OnParameterDefinitionsReceived(IStreamPipeline streamPipeline, QuixStreams.Telemetry.Models.ParameterDefinitions obj)
        {
            this.logger.LogTrace("StreamConsumer: OnParameterDefinitionsReceived");
            this.OnParameterDefinitionsChanged?.Invoke(this, obj);
        }
        
        private void OnEventDataReceived(IStreamPipeline streamPipeline, QuixStreams.Telemetry.Models.EventDataRaw @event)
        {
            this.logger.LogTrace("StreamConsumer: OnEventDataReceived");
            this.OnEventData?.Invoke(this, @event);
        }

        private void OnEventDataReceived(IStreamPipeline streamPipeline, QuixStreams.Telemetry.Models.EventDataRaw[] events)
        {
            this.logger.LogTrace("StreamConsumer: OnEventDataReceived");
            for (var index = 0; index < events.Length; index++)
            {
                var ev = events[index];
                this.OnEventData?.Invoke(this, ev);
            }
        }

        private void OnEventDefinitionsReceived(IStreamPipeline streamPipeline, QuixStreams.Telemetry.Models.EventDefinitions obj)
        {
            this.logger.LogTrace("StreamConsumer: OnEventDefinitionsReceived");
            this.OnEventDefinitionsChanged?.Invoke(this, obj);
        }
        
        private void OnStreamEndReceived(IStreamPipeline streamPipeline, QuixStreams.Telemetry.Models.StreamEnd obj)
        {
            RaiseStreamClosed(obj.StreamEndType);
        }

        private void RaiseStreamClosed(StreamEndType endType)
        {
            if (isClosed) return;
            isClosed = true;
            this.logger.LogTrace("StreamConsumer: OnStreamEndReceived");

            this.Timeseries.Buffers.ForEach(buffer => buffer.Dispose());
            
            this.OnStreamClosed?.Invoke(this, new StreamClosedEventArgs(this.topicConsumer, this, endType));
        }

        public override void Dispose()
        {
            this.streamEventsConsumer.Dispose();
            this.streamTimeseriesConsumer.Dispose();
            this.streamPropertiesConsumer.Dispose();
            base.Dispose();
        }
    }
}
