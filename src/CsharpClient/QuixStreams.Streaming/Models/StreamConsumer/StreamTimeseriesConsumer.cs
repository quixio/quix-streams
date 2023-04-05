using System;
using System.Collections.Generic;
using System.Linq;
using QuixStreams.Telemetry.Models;

namespace QuixStreams.Streaming.Models.StreamConsumer
{
    /// <summary>
    /// Consumer for streams, which raises <see cref="TimeseriesData"/> and <see cref="ParameterDefinitions"/> related messages
    /// </summary>
    public class StreamTimeseriesConsumer : IDisposable
    {
        private readonly ITopicConsumer topicConsumer;
        private readonly IStreamConsumerInternal streamConsumer;

        /// <summary>
        /// Initializes a new instance of <see cref="StreamTimeseriesConsumer"/>
        /// </summary>
        /// <param name="topicConsumer">The topic consumer which owns the stream consumer</param>
        /// <param name="streamConsumer">The Stream consumer which owns this stream event consumer</param>
        internal StreamTimeseriesConsumer(ITopicConsumer topicConsumer, IStreamConsumerInternal streamConsumer)
        {
            this.topicConsumer = topicConsumer;
            this.streamConsumer = streamConsumer;

            this.streamConsumer.OnParameterDefinitionsChanged += OnParameterDefinitionsChangedEventHandler;

            this.streamConsumer.OnTimeseriesData += OnTimeseriesDataEventHandler;
            this.streamConsumer.OnTimeseriesData += OnTimeseriesDataRawEventHandler;
        }

        private void OnParameterDefinitionsChangedEventHandler(IStreamConsumer sender, ParameterDefinitions parameterDefinitions)
        {
            this.LoadFromTelemetryDefinitions(parameterDefinitions);

            this.OnDefinitionsChanged?.Invoke(this.streamConsumer, new ParameterDefinitionsChangedEventArgs(this.topicConsumer, this.streamConsumer));
        }

        /// <summary>
        /// Creates a new buffer for reading data
        /// </summary>
        /// <param name="bufferConfiguration">An optional TimeseriesBufferConfiguration</param>
        /// <param name="parametersFilter">Zero or more parameter identifiers to filter as a whitelist. If provided, only those parameters will be available through this buffer</param>
        /// <returns><see cref="TimeseriesBufferConsumer"/> which will raise OnDataReceived event when new data is consumed</returns>
        public TimeseriesBufferConsumer CreateBuffer(TimeseriesBufferConfiguration bufferConfiguration = null, params string[] parametersFilter)
        {
            var buffer = new TimeseriesBufferConsumer(this.topicConsumer, this.streamConsumer, bufferConfiguration, parametersFilter);
            this.Buffers.Add(buffer);

            return buffer;
        }

        /// <summary>
        /// Creates a new buffer for reading data
        /// </summary>
        /// <param name="parametersFilter">Zero or more parameter identifiers to filter as a whitelist. If provided, only those parameters will be available through this buffer</param>
        /// <returns><see cref="TimeseriesBufferConsumer"/> which will raise OnDataReceived event when new data is consumed</returns>
        public TimeseriesBufferConsumer CreateBuffer(params string[] parametersFilter)
        {
            var buffer = new TimeseriesBufferConsumer(this.topicConsumer, this.streamConsumer, null, parametersFilter);
            this.Buffers.Add(buffer);

            return buffer;
        }

        /// <summary>
        /// Raised when the parameter definitions have changed for the stream.
        /// See <see cref="Definitions"/> for the latest set of parameter definitions
        /// </summary>
        public event EventHandler<ParameterDefinitionsChangedEventArgs> OnDefinitionsChanged;

        /// <summary>
        /// Event raised when data is received (without buffering)
        /// This event does not use Buffers, and data will be raised as they arrive without any processing.
        /// </summary>
        public event EventHandler<TimeseriesDataReadEventArgs> OnDataReceived;

        /// <summary>
        /// Event raised when data is received (without buffering) in raw transport format
        /// This event does not use Buffers, and data will be raised as they arrive without any processing.
        /// </summary>
        public event EventHandler<TimeseriesDataRawReadEventArgs> OnRawReceived;

        /// <summary>
        /// Gets the latest set of parameter definitions
        /// </summary>
        public List<ParameterDefinition> Definitions { get; private set; } = new List<ParameterDefinition>();

        /// <summary>
        /// List of buffers created for this stream
        /// </summary>
        internal List<TimeseriesBufferConsumer> Buffers { get; private set; } = new List<TimeseriesBufferConsumer>();

        private void LoadFromTelemetryDefinitions(QuixStreams.Telemetry.Models.ParameterDefinitions definitions)
        {
            var defs = new List<ParameterDefinition>();
            
            if (definitions.Parameters != null) 
                this.ConvertParameterDefinitions(definitions.Parameters, "").ForEach(d => defs.Add(d));
            if (definitions.ParameterGroups != null)
                this.ConvertGroupParameterDefinitions(definitions.ParameterGroups, "").ForEach(d => defs.Add(d));

            this.Definitions = defs;
        }

        private List<ParameterDefinition> ConvertParameterDefinitions(List<QuixStreams.Telemetry.Models.ParameterDefinition> parameterDefinitions, string location)
        {
            var result = parameterDefinitions.Select(d => new ParameterDefinition
            {
                Id = d.Id,
                Name = d.Name,
                Description = d.Description,
                MinimumValue = d.MinimumValue,
                MaximumValue = d.MaximumValue,
                Unit = d.Unit,
                Format = d.Format,
                CustomProperties = d.CustomProperties,
                Location = location
            }).ToList();

            return result;
        }

        private List<ParameterDefinition> ConvertGroupParameterDefinitions(List<QuixStreams.Telemetry.Models.ParameterGroupDefinition> parameterGroupDefinitions, string location)
        {
            var result = new List<ParameterDefinition>();

            foreach (var group in parameterGroupDefinitions)
            {
                if (group.Parameters != null)
                    this.ConvertParameterDefinitions(group.Parameters, location + "/" + group.Name).ForEach(d => result.Add(d));
                if (group.ChildGroups != null)
                    this.ConvertGroupParameterDefinitions(group.ChildGroups, location + "/" + group.Name).ForEach(d => result.Add(d));
            }

            return result;
        }

        private void OnTimeseriesDataEventHandler(IStreamConsumer streamConsumer, QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            if (this.OnDataReceived == null) return;
            var tsdata = new TimeseriesData(timeseriesDataRaw, null, false, false);
            this.OnDataReceived?.Invoke(streamConsumer, new TimeseriesDataReadEventArgs(this.topicConsumer, streamConsumer, tsdata));
        }

        private void OnTimeseriesDataRawEventHandler(IStreamConsumer streamConsumer, QuixStreams.Telemetry.Models.TimeseriesDataRaw timeseriesDataRaw)
        {
            this.OnRawReceived?.Invoke(streamConsumer, new TimeseriesDataRawReadEventArgs(this.topicConsumer, streamConsumer, timeseriesDataRaw));
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.streamConsumer.OnParameterDefinitionsChanged -= OnParameterDefinitionsChangedEventHandler;
            this.streamConsumer.OnTimeseriesData -= OnTimeseriesDataEventHandler;
            this.streamConsumer.OnTimeseriesData -= OnTimeseriesDataRawEventHandler;
        }
    }

    public class ParameterDefinitionsChangedEventArgs
    {
        public ParameterDefinitionsChangedEventArgs(ITopicConsumer topicConsumer, IStreamConsumer consumer)
        {
            this.TopicConsumer = topicConsumer;
            this.Stream = consumer;
        }

        public ITopicConsumer TopicConsumer { get; }
        public IStreamConsumer Stream { get; }
    }

    public class TimeseriesDataReadEventArgs
    {
        public TimeseriesDataReadEventArgs(object topic, object stream, TimeseriesData data)
        {
            this.Topic = topic;
            this.Stream = stream;
            this.Data = data;
        }
        
        /// <summary>
        /// Topic of type <see cref="ITopicConsumer"/> or <see cref="ITopicProducer"/>
        /// </summary>
        public object Topic { get; }
        
        /// <summary>
        /// Stream of type <see cref="IStreamConsumer"/> or <see cref="IStreamProducer"/>
        /// </summary>
        public object Stream { get; }
        
        public TimeseriesData Data { get; }
    }
    
    public class TimeseriesDataRawReadEventArgs
    {
        public TimeseriesDataRawReadEventArgs(object topic, object stream, TimeseriesDataRaw data)
        {
            this.Topic = topic;
            this.Stream = stream;
            this.Data = data;
        }

        /// <summary>
        /// Topic of type <see cref="ITopicConsumer"/> or <see cref="ITopicProducer"/>
        /// </summary>
        public object Topic { get; }
        
        /// <summary>
        /// Stream of type <see cref="IStreamConsumer"/> or <see cref="IStreamProducer"/>
        /// </summary>
        public object Stream { get; }
        
        public TimeseriesDataRaw Data { get; }
    }
}
